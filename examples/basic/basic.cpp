#include <stdio.h>
#include <thread>
#include <fstream>
#include <random>
#include <hicr/backends/hwloc/memoryManager.hpp>
#include <hicr/backends/mpi/memoryManager.hpp>
#include <hicr/backends/hwloc/topologyManager.hpp>
#include <hicr/backends/mpi/instanceManager.hpp>
#include <hicr/backends/mpi/communicationManager.hpp>
#include <hicr/backends/pthreads/computeManager.hpp>
#include <hicr/backends/pthreads/communicationManager.hpp>
#include <hicr/backends/boost/computeManager.hpp>
#include <hicr/frontends/RPCEngine/RPCEngine.hpp>
#include <hllm/engine.hpp>
#include <taskr/taskr.hpp>

#define _REPLICAS_PER_PARTITION 4
#define _PROMPT_THREAD_COUNT 16
#define _REQUESTS_PER_THREAD_COUNT 128

int main(int argc, char *argv[])
{
  // Creating HWloc topology object
  hwloc_topology_t hwlocTopologyObject;

  // Reserving memory for hwloc 
  hwloc_topology_init(&hwlocTopologyObject);

  // Initializing host (CPU) topology manager
  HiCR::backend::hwloc::TopologyManager hwlocTopologyManager(&hwlocTopologyObject);

  // Gathering topology from the topology manager
  const auto topology = hwlocTopologyManager.queryTopology();

  // Selecting first device
  auto d = *topology.getDevices().begin();

  // Getting memory space list from device
  auto memSpaces = d->getMemorySpaceList();

  // Grabbing first memory space for buffering
  auto bufferMemorySpace = *memSpaces.begin();

  // Now getting compute resource list from device
  auto computeResources = d->getComputeResourceList();

  // Grabbing first compute resource for computing incoming RPCs
  auto computeResource = *computeResources.begin();

  // Creating pthreads manager core
  HiCR::backend::pthreads::Core core(1);

  // Getting managers
  auto instanceManager              = HiCR::backend::mpi::InstanceManager::createDefault(&argc, &argv);
  auto communicationManager         = std::make_shared<HiCR::backend::mpi::CommunicationManager>();
  auto memoryManager                = std::make_shared<HiCR::backend::mpi::MemoryManager>();
  auto workerComputeManager         = std::make_shared<HiCR::backend::pthreads::ComputeManager>();
  auto taskComputeManager           = std::make_shared<HiCR::backend::boost::ComputeManager>();

  // Creating taskr object
  nlohmann::json taskrConfig;
  taskrConfig["Task Worker Inactivity Time (Ms)"] = 100;   // Suspend workers if a certain time of inactivity elapses
  taskrConfig["Task Suspend Interval Time (Ms)"]  = 100;   // Workers suspend for this time before checking back
  taskrConfig["Minimum Active Task Workers"]      = 1;     // Have at least one worker active at all times
  taskrConfig["Service Worker Count"]             = 1;     // Have one dedicated service workers at all times to listen for incoming messages
  taskrConfig["Make Task Workers Run Services"]   = true;  // Workers will check for meta messages in between executions
  auto taskr  = std::make_unique<taskr::Runtime>(taskComputeManager.get(), workerComputeManager.get(), computeResources, taskrConfig);

  // Instantiate RPC Engine
  auto rpcEngine = std::make_shared<HiCR::frontend::RPCEngine>(*communicationManager, *instanceManager, *memoryManager, *workerComputeManager, bufferMemorySpace, computeResource);

  // Initialize RPC Engine
  rpcEngine->initialize();

  // Check whether the instance is root
  const auto isRoot = instanceManager->getCurrentInstance()->isRootInstance();

  // Creating hLLM Engine object
  hLLM::Engine hllm(instanceManager.get(), rpcEngine.get(), taskr.get());

  // Deployment object, it dictates how the work will be distributed
  hLLM::configuration::Deployment deployment;

  // If I am root, checking arguments and config file and deploying
  if (isRoot == true)
  {
    if (argc != 2)
    {
      fprintf(stderr, "Error: Must provide the request file as argument.\n");
      instanceManager->abort(-1);
    }

    // Getting config file name from arguments
    std::string hllmConfigFilePath = std::string(argv[1]);

    // Parsing request file contents to a JSON object
    std::ifstream hllmConfigFs(hllmConfigFilePath);
    auto hllmConfigJs = nlohmann::json::parse(hllmConfigFs);

    // Parsing config file using hLLM
    deployment.deserialize(hllmConfigJs);

    // Calculating the number of instances required (1 per partition that runs the coorinator and a replica)
    const auto instancesRequired = 1;

    // Checking I have the correct number of instances (only one)
    if (instanceManager->getInstances().size() != instancesRequired)
    {
      fprintf(stderr, "Error: %lu instances provided, but %d are required\n", instanceManager->getInstances().size(), instancesRequired);
      instanceManager->abort(-1);
    }

    // Assigning this example to run in a single instance
    auto instance = *instanceManager->getInstances().begin();

    // Assigning instance ids to the partitions
    for (auto p : deployment.getPartitions()) 
    {
      // Getting instance Id that will run this partition (only one)
      const auto partitionInstanceId = instance->getId();

      // Setting this partition to be executed by the same instance than replica zero
      p->setCoordinatorInstanceId(partitionInstanceId);

      // Adding a replicas to this partition, with the same instance as the coordinator
      for (size_t i = 0; i < _REPLICAS_PER_PARTITION; i++)
      {
        const auto replica = std::make_shared<hLLM::configuration::Replica>(partitionInstanceId);
        p->addReplica(replica);
      }
    }
    
    // printf("%s\n", deployment.serialize().dump(2).c_str());
  }

  // Broadcasting deployment from the root instance to all the other intervening instances
  hllm.initialize(deployment, instanceManager->getRootInstanceId());

  // Before deploying, we need to indicate what communication and memory managers to assign to each of the edges
  // This allows for flexibility to choose in which devices to place the payload and coordination buffers
  for (const auto& edge : hllm.getDeployment().getEdges())
  {
    edge->setPayloadCommunicationManager(communicationManager.get());
    edge->setPayloadMemoryManager(memoryManager.get());
    edge->setPayloadMemorySpace(bufferMemorySpace);

    edge->setCoordinationCommunicationManager(communicationManager.get());
    edge->setCoordinationMemoryManager(memoryManager.get());
    edge->setCoordinationMemorySpace(bufferMemorySpace);

    // Setting capacity to the number of replicas in this example to allow for multiple replicas working simultaneously over TaskR
    edge->setBufferCapacity(_REPLICAS_PER_PARTITION);
  }

  // Setting managers for partition-wise control messaging
  hllm.getDeployment().getControlBuffer().communicationManager = communicationManager.get();
  hllm.getDeployment().getControlBuffer().memoryManager = memoryManager.get();
  hllm.getDeployment().getControlBuffer().memorySpace = bufferMemorySpace;

  // Declaring local value outside the functions for them to persist, but particular to each replica
  std::vector<float> cathetusSquaredSummedOutput;
  cathetusSquaredSummedOutput.resize(_REPLICAS_PER_PARTITION);

  // Declaring the hLLM tasks for the application
  hllm.registerFunction("Listen Request", [&](hLLM::Task *task) 
  {
    // Getting raw request
    const auto &requestMemSlot = task->getInput("Catheti");
    const auto request = std::string((const char *)requestMemSlot->getPointer());

    // Getting catheti values
    float cathetusAoutput;
    float cathetusBoutput;
    sscanf(request.c_str(), "%f %f", &cathetusAoutput, &cathetusBoutput);

    // Sending outputs
    auto cathetusAMemorySlot = memoryManager->registerLocalMemorySlot(bufferMemorySpace, &cathetusAoutput, sizeof(float));
    auto cathetusBMemorySlot = memoryManager->registerLocalMemorySlot(bufferMemorySpace, &cathetusBoutput, sizeof(float));
    task->setOutput("Cathetus A", cathetusAMemorySlot);
    task->setOutput("Cathetus B", cathetusBMemorySlot);
    memoryManager->deregisterLocalMemorySlot(cathetusAMemorySlot);
    memoryManager->deregisterLocalMemorySlot(cathetusBMemorySlot);
  });

  hllm.registerFunction("Square Cathetus A", [&](hLLM::Task *task) 
  {
    // Getting input
    const auto &cathetusMemSlot = task->getInput("Cathetus A");
    const float* cathetusA = (float *)cathetusMemSlot->getPointer();

    // Squaring cathetus
    float cathetusASquaredOutput = (*cathetusA) * (*cathetusA);

    // Sending output
    auto cathetusASquaredMemorySlot = memoryManager->registerLocalMemorySlot(bufferMemorySpace, &cathetusASquaredOutput, sizeof(float));
    task->setOutput("Cathetus A Squared", cathetusASquaredMemorySlot);
    memoryManager->deregisterLocalMemorySlot(cathetusASquaredMemorySlot);
  });

  hllm.registerFunction("Square Cathetus B", [&](hLLM::Task *task) 
  {
    // Getting input
    const auto &cathetusMemSlot = task->getInput("Cathetus B");
    const float* cathetusB = (float *)cathetusMemSlot->getPointer();

    // Squaring cathetus
    float cathetusBSquaredOutput = (*cathetusB) * (*cathetusB);

    // Sending output
    auto cathetusBSquaredMemorySlot = memoryManager->registerLocalMemorySlot(bufferMemorySpace, &cathetusBSquaredOutput, sizeof(float));
    task->setOutput("Cathetus B Squared", cathetusBSquaredMemorySlot);
    memoryManager->deregisterLocalMemorySlot(cathetusBSquaredMemorySlot);
  });

  hllm.registerFunction("Sum Catheti Squares", [&](hLLM::Task *task) 
  {
    // Getting inputs
    const auto &cathetusASquaredMemSlot = task->getInput("Cathetus A Squared");
    const float* cathetusAsquared = (float *)cathetusASquaredMemSlot->getPointer();
    const auto &cathetusBSquaredMemSlot = task->getInput("Cathetus B Squared");
    const float* cathetusBsquared = (float *)cathetusBSquaredMemSlot->getPointer();

    // Getting my replica id
    const auto replicaId = task->getReplicaIdx();

    // Squaring cathetus
    cathetusSquaredSummedOutput[replicaId] = (*cathetusAsquared) + (*cathetusBsquared);
  });

  hllm.registerFunction("Square Root Sum", [&](hLLM::Task *task) 
  {
    // Getting my replica id
    const auto replicaId = task->getReplicaIdx();

    // Squaring cathetus
    float hypotenuseOutput = sqrt(cathetusSquaredSummedOutput[replicaId]);

    // printf("[Basic Example] Returning response: '%s'\n", responseOutput.c_str());
    auto hypotenuseMemorySlot = memoryManager->registerLocalMemorySlot(bufferMemorySpace, &hypotenuseOutput, sizeof(float));
    task->setOutput("Hypotenuse", hypotenuseMemorySlot);
    memoryManager->deregisterLocalMemorySlot(hypotenuseMemorySlot);
  });

  // If I am the root, create a session to send prompt inputs
  std::vector<std::unique_ptr<std::thread>> promptThreads;
  if (isRoot)
  {
    // RNG for wait time between prompts
    std::default_random_engine promptTimeRandomEngine;
    std::uniform_real_distribution<float> promptTimeRandomDistribution(0.0, 1.0); 

    // RNG for catheti values
    std::default_random_engine cathetiRandomEngine;
    std::uniform_real_distribution<float> cathetiRandomDistribution(0.1, 10.0); 

    // Counter for the finished threads
    std::atomic<size_t> finishedPromptThreads = 0;

    // Error tolerance
    const auto tolerance = 0.0001;

    for (size_t i = 0; i < _PROMPT_THREAD_COUNT; i++)
      promptThreads.push_back(std::make_unique<std::thread>([&, i]()
      {
        // Wait until the hLLM has deployed
        while (hllm.isDeployed() == false);
        
        // Now create session
        auto session = hllm.createSession();
        printf("[User %04lu] Created Session: %lu\n", i, session->getSessionId());

        // Producing prompt, and receiving a response
        for (size_t promptCount = 0; promptCount < _REQUESTS_PER_THREAD_COUNT; promptCount++)
        {
          // Getting catheti values
          float cathetusA = cathetiRandomDistribution(cathetiRandomEngine);
          float cathetusB = cathetiRandomDistribution(cathetiRandomEngine);

          // Creating prompt with the catheti values
          const auto prompt = session->createPrompt(std::to_string(cathetusA) + std::string(" ") + std::to_string(cathetusB));

          // Pushing propmpt to the service
          session->pushPrompt(prompt);

          // Getting prompt id
          const auto promptId = prompt->getPromptId();
          // printf("[User] Sent prompt (%lu/%lu): '%s'\n", promptId.first, promptId.second,  prompt->getPrompt().c_str());

          // Wait until the prompt receives a response
          while(prompt->hasResponse() == false);

          // Getting response
          const float response = *(float*)prompt->getResponse().data();

          // Calculating
          const float error = std::abs(sqrtf(cathetusA * cathetusA + cathetusB * cathetusB) - response);

          // Printing response
          printf("[User %04lu] Got response: %f for prompt %lu/%lu: '%s'. |Error|: %f (< tolerance: %f)\n", i, response, promptId.first, promptId.second, prompt->getPrompt().c_str(), error, tolerance);

          // Verifying result
          if (error > tolerance) { fprintf(stderr, "Response error is higher than tolerance, aborting...\n"); exit(-1); }

          // Waiting a random amount of time before sending the next prompt
          usleep(100000.0 * promptTimeRandomDistribution(promptTimeRandomEngine));
        }

        // Increase counter for finished prompt threads
        const auto finishedThreads = finishedPromptThreads.fetch_add(1) + 1;
        // printf("Finished Threads: %lu\n", finishedThreads);

        // If ths was the last thread, then ask hllm to shutdown
        if (finishedThreads == _PROMPT_THREAD_COUNT) hllm.requestTermination();
      }));
  }

  // Deploying hLLM
  hllm.deploy(deployment);

  // // Waiting for prompt thread to finish
  if (isRoot) for (auto& thread : promptThreads) thread->join();

  // Finalize Instance Manager
  instanceManager->finalize();
}

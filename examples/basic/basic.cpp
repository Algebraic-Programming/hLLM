#include <stdio.h>
#include <thread>
#include <fstream>
#include <random>
#include <hicr/backends/hwloc/memoryManager.hpp>
#include <hicr/backends/hwloc/topologyManager.hpp>
#include <hicr/backends/mpi/instanceManager.hpp>
#include <hicr/backends/mpi/communicationManager.hpp>
#include <hicr/backends/mpi/memoryManager.hpp>
#include <hicr/backends/pthreads/computeManager.hpp>
#include <hicr/backends/pthreads/communicationManager.hpp>
#include <hicr/backends/boost/computeManager.hpp>
#include <hicr/frontends/RPCEngine/RPCEngine.hpp>
#include <hllm/engine.hpp>
#include <taskr/taskr.hpp>

#define _PROMPT_THREAD_COUNT 16
#define _REQUESTS_PER_THREAD_COUNT 32

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

  // Getting MPI managers
  auto instanceManager              = HiCR::backend::mpi::InstanceManager::createDefault(&argc, &argv);
  auto mpiCommunicationManager      = std::make_shared<HiCR::backend::mpi::CommunicationManager>();
  auto mpiMemoryManager             = std::make_shared<HiCR::backend::mpi::MemoryManager>();
  auto pthreadsComputeManager       = std::make_shared<HiCR::backend::pthreads::ComputeManager>();
  auto boostComputeManager          = std::make_shared<HiCR::backend::boost::ComputeManager>();
  auto hwlocMemoryManager           = std::make_shared<HiCR::backend::hwloc::MemoryManager>(&hwlocTopologyObject);

  // Creating taskr object
  nlohmann::json taskrConfig;
  taskrConfig["Task Worker Inactivity Time (Ms)"] = 100;   // Suspend workers if a certain time of inactivity elapses
  taskrConfig["Task Suspend Interval Time (Ms)"]  = 100;   // Workers suspend for this time before checking back
  taskrConfig["Minimum Active Task Workers"]      = 1;     // Have at least one worker active at all times
  taskrConfig["Service Worker Count"]             = 1;     // Have one dedicated service workers at all times to listen for incoming messages
  taskrConfig["Make Task Workers Run Services"]   = false; // Workers will check for meta messages in between executions
  auto taskr  = std::make_unique<taskr::Runtime>(boostComputeManager.get(), pthreadsComputeManager.get(), computeResources, taskrConfig);

  // Instantiate RPC Engine
  auto rpcEngine = std::make_shared<HiCR::frontend::RPCEngine>(*mpiCommunicationManager, *instanceManager, *mpiMemoryManager, *pthreadsComputeManager, bufferMemorySpace, computeResource);

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

    // Checking I have the correct number of replicas (at least one per partition)
    size_t replicasRequired = 0;
    for (auto p : deployment.getPartitions())
    {
      // Getting number of replicas required
      const auto replicaCount = p->getReplicas().size();

      // Checking replicas count for this partition is not zero
      if (replicaCount == 0)
      {
        fprintf(stderr, "Error: This example requires at least one replica per partition to be provided. Partition '%s' has no replicas\n", p->getName().c_str());
        instanceManager->abort(-1);
      }

      // Adding the number of requested replicas
      replicasRequired += p->getReplicas().size();
    } 
    
    // Checking I have the correct number of instances (one per replica)
    if (instanceManager->getInstances().size() != replicasRequired)
    {
      fprintf(stderr, "Error: %lu MPI instances provided, but %lu partition replicas were requested\n", instanceManager->getInstances().size(), replicasRequired);
      instanceManager->abort(-1);
    }

    // Assigning instance ids to the partitions
    auto instanceItr = instanceManager->getInstances().begin();
    for (auto p : deployment.getPartitions()) 
    {
      // Setting this partition to be executed by the same instance than replica zero
      p->setInstanceId(instanceItr.operator*()->getId());

      // Setting this partition replicas incrementally
      for (auto r : p->getReplicas()) r->setInstanceId(instanceItr++.operator*()->getId());
    }
    
    // printf("%s\n", deployment.serialize().dump(2).c_str());
  }

  // Broadcasting deployment from the root instance to all the other intervening instances
  hllm.initialize(deployment, instanceManager->getRootInstanceId());

  // Before deploying, we need to indicate what communication and memory managers to assign to each of the edges
  // This allows for flexibility to choose in which devices to place the payload and coordination buffers
  for (const auto& edge : hllm.getDeployment().getEdges())
  {
    edge->setPayloadCommunicationManager(mpiCommunicationManager.get());
    edge->setPayloadMemoryManager(mpiMemoryManager.get());
    edge->setPayloadMemorySpace(bufferMemorySpace);

    edge->setCoordinationCommunicationManager(mpiCommunicationManager.get());
    edge->setCoordinationMemoryManager(mpiMemoryManager.get());
    edge->setCoordinationMemorySpace(bufferMemorySpace);
  }

  // Setting managers for partition-wise control messaging
  hllm.getDeployment().getControlBuffer().communicationManager = mpiCommunicationManager.get();
  hllm.getDeployment().getControlBuffer().memoryManager = mpiMemoryManager.get();
  hllm.getDeployment().getControlBuffer().memorySpace = bufferMemorySpace;

  // Declaring the hLLM tasks for the application
  thread_local std::string responseOutput;
  hllm.registerFunction("Listen Request", [&](hLLM::Task *task) 
  {
    // Getting input
    const auto &requestMemSlot = task->getInput("Prompt");
    const auto request = std::string((const char *)requestMemSlot->getPointer());

    // Create output
    responseOutput             = request + std::string(" [Processed]");
    const auto responseMemSlot = mpiMemoryManager->registerLocalMemorySlot(bufferMemorySpace, responseOutput.data(), responseOutput.size() + 1);

    // printf("[Basic Example] Returning response: '%s'\n", responseOutput.c_str());
    task->setOutput("Response", responseMemSlot);
  });

  // If I am the root, create a session to send prompt inputs
  std::vector<std::unique_ptr<std::thread>> promptThreads;
  std::default_random_engine promptTimeRandomEngine;
  std::uniform_real_distribution<double> promptTimeRandomDistribution(0.0, 1.0); 
  std::atomic<size_t> finishedPromptThreads = 0;
  if (isRoot)
  {
    for (size_t i = 0; i < _PROMPT_THREAD_COUNT; i++)
      promptThreads.push_back(std::make_unique<std::thread>([&, i]()
      {
        // Wait until the hLLM has deployed
        while (hllm.isDeployed() == false);
        
        // Now create session
        auto session = hllm.createSession();

        // Send a test message
        for (size_t promptCount = 0; promptCount < _REQUESTS_PER_THREAD_COUNT; promptCount++)
        {
          const auto prompt = session->createPrompt(std::string("Hello, World! ") + std::to_string(promptCount));
          session->pushPrompt(prompt);
          // printf("[User] Sent prompt: %s\n", prompt->getPrompt().c_str());
          while(prompt->hasResponse() == false);
          const auto promptId = prompt->getPromptId();
          printf("[User %04lu] Got response: '%s' for prompt %lu/%lu: '%s'\n", i, prompt->getResponse().c_str(), promptId.first, promptId.second, prompt->getPrompt().c_str());
          usleep(100000.0 * promptTimeRandomDistribution(promptTimeRandomEngine));
        }

        // Increase counter for finished prompt threads
        const auto finishedThreads = finishedPromptThreads.fetch_add(1) + 1;
        printf("Finished Threads: %lu\n", finishedThreads);

        // If ths was the last thread, then ask hllm to shutdown
        if (finishedThreads == _PROMPT_THREAD_COUNT) hllm.requestTermination();
      }));
  }

  // Deploying hLLM
  hllm.deploy(deployment);

  // // Waiting for prompt thread to finish
  if (isRoot) for (auto& thread : promptThreads) thread->join();

  // Finalize Instance Manager
  // instanceManager->finalize();
}

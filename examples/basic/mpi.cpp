#include <stdio.h>
#include <thread>
#include <fstream>
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
#include "basic.hpp"
#include "requester.hpp"

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
  createTasks(hllm, mpiMemoryManager.get(), bufferMemorySpace);

  // If I am the root, create a session to send prompt inputs
  std::unique_ptr<std::thread> promptThread;
  if (isRoot)
  {
    promptThread = std::make_unique<std::thread>([&]()
    {
      // Wait until the hLLM has deployed
      while (hllm.isDeployed() == false);

      // Now create session
      auto session = hllm.createSession();

      // Send a test message
      size_t currentPrompt = 0;
      while(true)
      {
        const auto prompt = session->pushPrompt(std::string("Hello, World! ") + std::to_string(currentPrompt));
        currentPrompt++;
        printf("[User] Sent prompt: %s\n", prompt->getPrompt().c_str());
        while(prompt->hasResponse() == false);
        printf("[User] Got response: %s\n", prompt->getResponse().c_str());
        sleep(1);
      }
    });
  }

  // Deploying hLLM
  hllm.deploy(deployment);

  // // Instantiating request server (emulates live users)
  // size_t requestCount   = 32;
  // size_t requestDelayMs = 100;
  // initializeRequestServer(&hllm, requestCount);
  // auto requestThread = std::thread([&]() { startRequestServer(requestDelayMs); });

  // // Deploy all hLLM instances
  // hllm.deploy(hllmConfigJs);

  // // Waiting for request server to finish producing requests
  // printf("[basic.cpp] Waiting for request hllm thread to come back...\n");
  // requestThread.join();

  // // Finalizing hLLM
  // printf("[basic.cpp] Finalizing...\n");
  // hllm.finalize();

  // printf("[basic.cpp] Finalizing Instance Manager...\n");
  // Finalize Instance Manager
  
  // Waiting for prompt thread to finish
  if (isRoot) promptThread->join();

  instanceManager->finalize();
}

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
#include <hicr/frontends/RPCEngine/RPCEngine.hpp>
#include <hllm/engine.hpp>
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
  auto computeManager               = std::make_shared<HiCR::backend::pthreads::ComputeManager>();
  auto pthreadsCommunicationManager = std::make_shared<HiCR::backend::pthreads::CommunicationManager>();
  auto hwlocMemoryManager           = std::make_shared<HiCR::backend::hwloc::MemoryManager>(&hwlocTopologyObject);

  // Instantiate RPC Engine
  auto rpcEngine = std::make_shared<HiCR::frontend::RPCEngine>(*mpiCommunicationManager, *instanceManager, *mpiMemoryManager, *computeManager, bufferMemorySpace, computeResource);

  // Initialize RPC Engine
  rpcEngine->initialize();

  // Check whether the instance is root
  const auto isRoot = instanceManager->getCurrentInstance()->isRootInstance();

  // Creating hLLM Engine object
  hLLM::Engine engine(instanceManager.get(),
                      mpiCommunicationManager.get(),
                      pthreadsCommunicationManager.get(),
                      mpiMemoryManager.get(),
                      hwlocMemoryManager.get(),
                      rpcEngine.get(),
                      bufferMemorySpace,
                      bufferMemorySpace,
                      topology);

  // Declaring the hLLM tasks for the application
  createTasks(engine, mpiMemoryManager.get(), bufferMemorySpace);

  // If I am not root, await deployment
  if (isRoot == false) engine.awaitDeployment();

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
    hLLM::configuration::Deployment deployment(hllmConfigJs);

    // Checking I have the correct number of instances (one per partition)
    if (instanceManager->getInstances().size() != deployment.getPartitions().size())
    {
      fprintf(stderr, "Error: %lu MPI instances provided, but %lu partitions were requested\n", instanceManager->getInstances().size(),  deployment.getPartitions().size());
      instanceManager->abort(-1);
    }

    // Assigning instance ids to the partitions
    auto instanceItr = instanceManager->getInstances().begin();
    for (auto p : deployment.getPartitions()) p->setInstanceId(instanceItr++.operator*()->getId());
    // printf("%s\n", deployment.serialize().dump(2).c_str());

    // Deploying
    engine.deploy(deployment);
  }

  // // Instantiating request server (emulates live users)
  // size_t requestCount   = 32;
  // size_t requestDelayMs = 100;
  // initializeRequestServer(&engine, requestCount);
  // auto requestThread = std::thread([&]() { startRequestServer(requestDelayMs); });

  // // Initializing LLM engine with deployer id 0
  // engine.initialize(0);

  // // Deploy all LLM Engine instances
  // engine.deploy(hllmConfigJs);

  // // Waiting for request server to finish producing requests
  // printf("[basic.cpp] Waiting for request engine thread to come back...\n");
  // requestThread.join();

  // // Finalizing LLM engine
  // printf("[basic.cpp] Finalizing...\n");
  // engine.finalize();

  printf("[basic.cpp] Finalizing Instance Manager...\n");
  // Finalize Instance Manager
  instanceManager->finalize();
}

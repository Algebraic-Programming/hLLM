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

#include "serving.hpp"
#include "requester.hpp"

#define __SERVING_BROADCAST_CONFIGURATION_RPC "[serving.cpp] Exchange Configuration"

void broadcastConfigJson(std::shared_ptr<HiCR::frontend::RPCEngine> &rpcEngine,
                         nlohmann::json_abi_v3_11_2::json           &configJs,
                         const bool                                  isRoot,
                         std::unique_ptr<HiCR::InstanceManager>     &instanceManager)
{
  // Register the RPC to exchange the JSON configuration
  rpcEngine->addRPCTarget(__SERVING_BROADCAST_CONFIGURATION_RPC, HiCR::backend::pthreads::ComputeManager::createExecutionUnit([&](void *) {
                            // Serializing configuration
                            const auto serializedConfiguration = configJs.dump();

                            // Returning serialized configuration
                            rpcEngine->submitReturnValue((void *)serializedConfiguration.c_str(), serializedConfiguration.size() + 1);
                          }));

  // Listen for the incoming RPCs and return the hLLM configuration
  if (isRoot)
  {
    for (size_t i = 0; i < instanceManager->getInstances().size() - 1; ++i) rpcEngine->listen();
  }
  // Request the configuration from the root instance
  else
  {
    // Find the root instance
    HiCR::Instance *rootInstance = nullptr;
    for (auto &instance : instanceManager->getInstances())
    {
      if (instance->isRootInstance()) { rootInstance = instance.get(); }
    }

    // Request the root instance to send the configuration
    rpcEngine->requestRPC(rootInstance->getId(), __SERVING_BROADCAST_CONFIGURATION_RPC);

    // Get the return value as a memory slot
    auto returnValue = rpcEngine->getReturnValue();

    // Receive raw information from root
    std::string serializedConfiguration = static_cast<char *>(returnValue->getPointer());

    // Parse the configuration
    configJs = nlohmann::json::parse(serializedConfiguration);

    // Free return value
    rpcEngine->getMemoryManager()->freeLocalMemorySlot(returnValue);
  }
}

int main(int argc, char *argv[])
{
  // Creating HWloc topology object
  hwloc_topology_t topology;

  // Reserving memory for hwloc
  hwloc_topology_init(&topology);

  // Initializing host (CPU) topology manager
  HiCR::backend::hwloc::TopologyManager tm(&topology);

  // Gathering topology from the topology manager
  const auto t = tm.queryTopology();

  // Selecting first device
  auto d = *t.getDevices().begin();

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
  auto hwlocMemoryManager           = std::make_shared<HiCR::backend::hwloc::MemoryManager>(&topology);

  // Instantiate RPC Engine
  auto rpcEngine = std::make_shared<HiCR::frontend::RPCEngine>(*mpiCommunicationManager, *instanceManager, *mpiMemoryManager, *computeManager, bufferMemorySpace, computeResource);

  // Initialize RPC Engine
  rpcEngine->initialize();

  // Check whether the instance is root
  const auto isRoot = instanceManager->getCurrentInstance()->isRootInstance();

  // If I am root, read config file
  nlohmann::json configJs;
  if (isRoot)
  {
    // Checking arguments
    if (argc != 2)
    {
      fprintf(stderr, "Error: Must provide the request file as argument.\n");
      return -1;
    }

    // Getting config file name from arguments
    std::string configFilePath = std::string(argv[1]);

    // Parsing request file contents to a JSON object
    std::ifstream ifs(configFilePath);
    configJs = nlohmann::json::parse(ifs);
  }

  // Send the configuration to all the other instances
  broadcastConfigJson(rpcEngine, configJs, isRoot, instanceManager);

  // Creating hLLM Engine object
  hLLM::Engine engine(instanceManager.get(),
                      mpiCommunicationManager.get(),
                      pthreadsCommunicationManager.get(),
                      mpiMemoryManager.get(),
                      hwlocMemoryManager.get(),
                      rpcEngine.get(),
                      bufferMemorySpace,
                      bufferMemorySpace,
                      t);
                      
  // Instantiating request server (emulates live users)
  size_t requestCount   = 1;
  size_t requestDelayMs = 100;
  initializeRequestServer(&engine, requestCount);
  
  // Create hLLM tasks for the application
  createTasks(engine, mpiMemoryManager.get(), bufferMemorySpace);

  // Start running the  request Server (which creates requests)
  auto requestThread = std::thread([&]() { startRequestServer(requestDelayMs); });

  // Initializing LLM engine with deployer id 0
  engine.initialize(0);

  // Deploy all LLM Engine instances
  engine.deploy(configJs);

  // Waiting for request server to finish producing requests
  printf("[serving.cpp] Waiting for request engine thread to come back...\n");
  requestThread.join();

  // Finalizing LLM engine
  printf("[serving.cpp] Finalizing...\n");
  engine.finalize();

  printf("[serving.cpp] Finalizing Instance Manager...\n");
  // Finalize Instance Manager
  instanceManager->finalize();
}

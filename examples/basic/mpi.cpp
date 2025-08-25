#include <stdio.h>
#include <thread>
#include <fstream>

#include <hicr/backends/mpi/instanceManager.hpp>
#include <hicr/backends/mpi/communicationManager.hpp>
#include <hicr/backends/mpi/memoryManager.hpp>
#include <hicr/backends/pthreads/computeManager.hpp>
#include <hicr/frontends/RPCEngine/RPCEngine.hpp>
#include <hicr/backends/hwloc/topologyManager.hpp>

#include <hllm/engine.hpp>

#include "basic.hpp"
#include "requester.hpp"

#define __BASIC_BROADCAST_CONFIGURATION_RPC "[Basic] Exchange Configuration"

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
  auto instanceManager      = HiCR::backend::mpi::InstanceManager::createDefault(&argc, &argv);
  auto communicationManager = std::make_shared<HiCR::backend::mpi::CommunicationManager>();
  auto memoryManager        = std::make_shared<HiCR::backend::mpi::MemoryManager>();
  auto computeManager       = std::make_shared<HiCR::backend::pthreads::ComputeManager>();

  // Instantiate RPC Engine
  auto rpcEngine = std::make_shared<HiCR::frontend::RPCEngine>(*communicationManager, *instanceManager, *memoryManager, *computeManager, bufferMemorySpace, computeResource);

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

  // Register the RPC to exchange the JSON configuration
  rpcEngine->addRPCTarget(__BASIC_BROADCAST_CONFIGURATION_RPC, HiCR::backend::pthreads::ComputeManager::createExecutionUnit([&](void *) {
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
    rpcEngine->requestRPC(*rootInstance, __BASIC_BROADCAST_CONFIGURATION_RPC);

    // Get the return value as a memory slot
    auto returnValue = rpcEngine->getReturnValue(*rootInstance);

    // Receive raw information from root
    std::string serializedConfiguration = static_cast<char *>(returnValue->getPointer());

    // Parse the configuration
    configJs = nlohmann::json::parse(serializedConfiguration);

    // Free return value
    rpcEngine->getMemoryManager()->freeLocalMemorySlot(returnValue);
  }

  // Creating hLLM Engine object
  hLLM::Engine engine(instanceManager.get(), communicationManager.get(), memoryManager.get(), rpcEngine.get(), bufferMemorySpace);

  // Create hLLM tasks for the application
  createTasks(engine, memoryManager.get(), bufferMemorySpace);

  // Instantiating request server (emulates live users)
  size_t requestCount   = 32;
  size_t requestDelayMs = 100;
  initializeRequestServer(&engine, requestCount);
  auto requestThread = std::thread([&]() { startRequestServer(requestDelayMs); });

  // Initializing LLM engine
  engine.initialize();

  // Deploy all LLM Engine instances
  engine.deploy(configJs);

  // Run the engine
  engine.run();

  // Waiting for request server to finish producing requests
  printf("[basic.cpp] Waiting for request engine thread to come back...\n");
  requestThread.join();

  // Finalizing LLM engine
  printf("[basic.cpp] Finalizing...\n");
  engine.finalize();
}
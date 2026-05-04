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
#include <taskr/runtime.hpp>
#include <system/engine.hpp>

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

  // Getting managers
  auto instanceManager      = std::shared_ptr<HiCR::InstanceManager>(HiCR::backend::mpi::InstanceManager::createDefault(&argc, &argv));
  auto communicationManager = std::make_shared<HiCR::backend::mpi::CommunicationManager>();
  auto memoryManager        = std::make_shared<HiCR::backend::mpi::MemoryManager>();
  auto workerComputeManager = std::make_shared<HiCR::backend::pthreads::ComputeManager>();
  auto taskComputeManager   = std::make_shared<HiCR::backend::boost::ComputeManager>();

  // Instantiate RPC Engine
  auto rpcEngine = std::make_shared<HiCR::frontend::RPCEngine>(*communicationManager, *instanceManager, *memoryManager, *workerComputeManager, bufferMemorySpace, computeResource);

  // Initialize RPC Engine
  rpcEngine->initialize();

  // Check whether the instance is root
  const auto isRoot = instanceManager->getCurrentInstance()->isRootInstance();

  // Creating module entry point function
  auto moduleEntrypoint = [&]() { printf("[Instance %lu] Hello from the module entry point!\n", instanceManager->getCurrentInstance()->getId()); };

  // Creating hLLM Engine object
  hLLM::system::Engine hllm(instanceManager, taskComputeManager, rpcEngine, moduleEntrypoint);

  auto numInstances = 0;
  // If I am root, checking arguments.
  // Do not assume other instances will have the correct arguments set (e.g., file that exists only on root instance)
  if (isRoot == true)
  {
    if (argc != 2)
    {
      fprintf(stderr, "Error: Must provide the desired number of instances.\n");
      instanceManager->abort(-1);
    }

    // Getting config file name from arguments
    numInstances = std::stoi(argv[1]);
  }

  // Broadcasting deployment from the root instance to all the other intervening instances
  hllm.initialize(numInstances);

  // Running hLLM
  hllm.run();

  // Finalizing hLLM
  hllm.finalize();

  // Finalize Instance Manager
  instanceManager->finalize();
}

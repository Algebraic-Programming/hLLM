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

  // Creating hLLM Engine object
  hLLM::system::Engine hllm(instanceManager, taskComputeManager, rpcEngine, instanceManager->getRootInstanceId());

  // Initializing hLLM
  hllm.initialize();

  // Running hLLM
  hllm.run();

  // Finalizing hLLM
  hllm.terminate();

  // Awaiting hLLM termination
  hllm.await();

  // Finalize Instance Manager
  instanceManager->finalize();
}

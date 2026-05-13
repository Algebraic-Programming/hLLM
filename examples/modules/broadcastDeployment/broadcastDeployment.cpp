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

#include <modules/configuration/deployment.hpp>
#include <modules/broadcastDeployment/module.hpp>
#include <system/engine.hpp>

#include "broadcastDeployment.hpp"

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

  // Check whether the instance is root
  const auto isRoot             = instanceManager->getCurrentInstance()->isRootInstance();
  const auto instanceId         = instanceManager->getCurrentInstance()->getId();
  const auto deployerInstanceId = instanceManager->getRootInstanceId();

  ///// Configuration parsing
  hLLM::configuration::Deployment deployment;

  // If I am root, checking arguments.
  // Do not assume other instances will have the correct arguments set (e.g., file that exists only on root instance)
  if (isRoot == true)
  {
    if (argc != 2)
    {
      fprintf(stderr, "Error: Must provide the config file path.\n");
      instanceManager->abort(-1);
    }
    // Read and parse config file
    readAndParseConfiguration(argv, deployment, instanceManager);
  }

  std::shared_ptr<hLLM::modules::broadcastDeployment::Module> broadcastDeploymentModule;
  if (instanceId == deployerInstanceId)
  {
    broadcastDeploymentModule =
      std::make_shared<hLLM::modules::broadcastDeployment::Module>(instanceManager, taskComputeManager, rpcEngine, deployerInstanceId, instanceId, deployment);
  }
  else { broadcastDeploymentModule = std::make_shared<hLLM::modules::broadcastDeployment::Module>(instanceManager, taskComputeManager, rpcEngine, deployerInstanceId, instanceId); }

  const auto &receivedDeployment = broadcastDeploymentModule->getDeployment();
  // Adding broadcast deployment module to hLLM
  hllm.addModule("BroadcastDeployment", broadcastDeploymentModule);

  // Initializing hLLM
  hllm.initialize();

  // Running hLLM
  hllm.run();

  // Finalizing hLLM
  hllm.terminate();

  // Awaiting hLLM termination
  hllm.await();

  // Printing deployment information to verify it was correctly received
  std::this_thread::sleep_for(std::chrono::seconds(instanceId)); // Sleep a bit to ensure all output is printed before this
  printf("[Instance %lu] Received deployment configuration:\n%s\n", instanceId, receivedDeployment.serialize().dump(2).c_str());

  // Finalize Instance Manager
  instanceManager->finalize();
}

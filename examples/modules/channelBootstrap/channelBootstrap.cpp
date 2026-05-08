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
#include <modules/channelBootstrap/module.hpp>
#include <system/engine.hpp>

#include "channelBootstrap.hpp"
#include "telephoneGame.hpp"

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
  const auto isRoot     = instanceManager->getCurrentInstance()->isRootInstance();
  const auto instanceId = instanceManager->getCurrentInstance()->getId();

  ///// Configuration parsing
  hLLM::configuration::Deployment deployment;

  // Checking arguments
  if (argc != 2)
  {
    fprintf(stderr, "Error: Must provide the config file path.\n");
    instanceManager->abort(-1);
  }
  // Read and parse config file
  readAndParseConfiguration(argv, deployment, instanceManager);

  // Assign managers into all edges
  assignEdgeManagers(deployment, communicationManager.get(), memoryManager.get(), bufferMemorySpace);

  // Build local channels for this rank
  std::vector<std::shared_ptr<hLLM::system::channels::Input>>  inputs;
  std::vector<std::shared_ptr<hLLM::system::channels::Output>> outputs;
  hLLM::system::channels::keyBuilderFc_t                       keyBuilder = defaultChannelKeyBuilder;
  buildLocalChannelsFromDeployment(deployment, instanceId, keyBuilder, inputs, outputs);
  printf("[Instance %lu] Local channels prepared: inputs=%lu outputs=%lu\n", instanceId, inputs.size(), outputs.size());

  // Bootstrap module
  std::vector<HiCR::CommunicationManager *> managerOrder           = {communicationManager.get()};
  auto                                      channelBootstrapModule = std::make_unique<hLLM::modules::channelBootstrap::Module>(inputs, outputs, managerOrder);

  // Adding channel bootstrap module to hLLM
  hllm.addModule("ChannelBootstrap", std::move(channelBootstrapModule));

  // Initializing hLLM
  hllm.initialize();

  // Running hLLM
  hllm.run();

  //// Here we need hllm to run the modules. Now we can do the telephone game
  telephoneGame(inputs, outputs, instanceId, isRoot);

  // Finalizing hLLM
  hllm.terminate();

  // Awaiting hLLM termination
  hllm.await();

  // Finalize Instance Manager
  instanceManager->finalize();
}

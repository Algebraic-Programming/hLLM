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
#include <modules/channelDispatcher/module.hpp>
#include <modules/heartbeat/module.hpp>
#include <modules/service/module.hpp>
#include <system/engine.hpp>
#include <system/channels/messageTypeRegistry.hpp>

#include "heartbeat.hpp"

int main(int argc, char *argv[])
{
  hwloc_topology_t hwlocTopologyObject;
  hwloc_topology_init(&hwlocTopologyObject);

  HiCR::backend::hwloc::TopologyManager hwlocTopologyManager(&hwlocTopologyObject);
  const auto                            topology = hwlocTopologyManager.queryTopology();

  auto d                  = *topology.getDevices().begin();
  auto memSpaces          = d->getMemorySpaceList();
  auto bufferMemorySpace  = *memSpaces.begin();
  auto computeResourcesIt = d->getComputeResourceList().begin();

  // Use only 2 cores
  std::vector<std::shared_ptr<HiCR::ComputeResource>> computeResources;
  for (int i = 0; i < 2; i++)
  {
    computeResources.push_back(*computeResourcesIt);
    computeResourcesIt++;
  }
  auto computeResource = *computeResources.begin();

  auto instanceManager      = std::shared_ptr<HiCR::InstanceManager>(HiCR::backend::mpi::InstanceManager::createDefault(&argc, &argv));
  auto communicationManager = std::make_shared<HiCR::backend::mpi::CommunicationManager>();
  auto memoryManager        = std::make_shared<HiCR::backend::mpi::MemoryManager>();
  auto workerComputeManager = std::make_shared<HiCR::backend::pthreads::ComputeManager>();
  auto taskComputeManager   = std::make_shared<HiCR::backend::boost::ComputeManager>();

  auto rpcEngine = std::make_shared<HiCR::frontend::RPCEngine>(*communicationManager, *instanceManager, *memoryManager, *workerComputeManager, bufferMemorySpace, computeResource);

  rpcEngine->initialize();

  nlohmann::json taskrConfig;
  taskrConfig["Task Worker Inactivity Time (Ms)"] = 100;
  taskrConfig["Task Suspend Interval Time (Ms)"]  = 100;
  taskrConfig["Minimum Active Task Workers"]      = 1;
  taskrConfig["Service Worker Count"]             = 1;
  taskrConfig["Make Task Workers Run Services"]   = true;
  auto taskr                                      = std::make_shared<taskr::Runtime>(taskComputeManager.get(), workerComputeManager.get(), computeResources, taskrConfig);

  hLLM::system::Engine hllm(instanceManager, taskComputeManager, rpcEngine, instanceManager->getRootInstanceId());

  const auto isRoot     = instanceManager->getCurrentInstance()->isRootInstance();
  const auto instanceId = instanceManager->getCurrentInstance()->getId();

  hLLM::configuration::Deployment deployment;
  if (argc != 2)
  {
    fprintf(stderr, "Error: Must provide the config file path.\n");
    instanceManager->abort(-1);
  }

  readAndParseConfiguration(argv, deployment, instanceManager);
  assignEdgeManagers(deployment, communicationManager.get(), memoryManager.get(), bufferMemorySpace);

  std::vector<localInput_t>  localInputs;
  std::vector<localOutput_t> localOutputs;

  hLLM::system::channels::keyBuilderFc_t keyBuilder = defaultChannelKeyBuilder;
  buildLocalChannelsFromDeploymentWithIds(deployment, instanceId, keyBuilder, localInputs, localOutputs);
  printf("[Instance %lu] Local channels prepared: inputs=%lu outputs=%lu\n", instanceId, localInputs.size(), localOutputs.size());

  std::vector<std::shared_ptr<hLLM::system::channels::Input>>  bootstrapInputs;
  std::vector<std::shared_ptr<hLLM::system::channels::Output>> bootstrapOutputs;
  bootstrapInputs.reserve(localInputs.size());
  bootstrapOutputs.reserve(localOutputs.size());
  for (const auto &in : localInputs) bootstrapInputs.push_back(in.channel);
  for (const auto &out : localOutputs) bootstrapOutputs.push_back(out.channel);

  std::vector<HiCR::CommunicationManager *> managerOrder           = {communicationManager.get()};
  auto                                      channelBootstrapModule = std::make_shared<hLLM::modules::channelBootstrap::Module>(bootstrapInputs, bootstrapOutputs, managerOrder);

  auto channelDispatcherModule = std::make_shared<hLLM::modules::channelDispatcher::Module>(100);

  auto &messageTypeRegistry = hllm.getMessageTypeRegistry();
  auto  heartbeatModule     = std::make_shared<hLLM::modules::heartbeat::Module>(
    instanceId,
    1000,
    [&](const hLLM::modules::heartbeat::Module::healthEvent_t &event) {
      if (event.previousHealth != event.newHealth)
      {
        printf("[Instance %lu][Heartbeat] Peer %lu health %s -> %s\n",
               instanceId,
               event.instanceId,
               hLLM::modules::heartbeat::Module::health_tToString(event.previousHealth).c_str(),
               hLLM::modules::heartbeat::Module::health_tToString(event.newHealth).c_str());
      }
      else
      {
        printf(
          "[Instance %lu][Heartbeat] Peer %lu health %s (no change)\n", instanceId, event.instanceId, hLLM::modules::heartbeat::Module::health_tToString(event.newHealth).c_str());
      }
    },
    messageTypeRegistry,
    500);

  // Keep real remote instance IDs
  for (const auto &in : localInputs) heartbeatModule->addInput(in.sourceInstanceId, in.channel);
  for (const auto &out : localOutputs) heartbeatModule->addOutput(out.targetInstanceId, out.channel);
  for (auto &subscription : heartbeatModule->buildSubscriptions()) channelDispatcherModule->subscribe(subscription);

  auto serviceModule = std::make_shared<hLLM::modules::service::Module>(taskr);

  serviceModule->addService("ChannelDispatcher", channelDispatcherModule->getService());
  serviceModule->addService("Heartbeat", heartbeatModule->getService());

  hllm.addModule("ChannelBootstrap", channelBootstrapModule);
  hllm.addModule("ChannelDispatcher", channelDispatcherModule);
  hllm.addModule("Heartbeat", heartbeatModule);
  hllm.addModule("Service", serviceModule);

  hllm.initialize();

  hllm.run();

  std::this_thread::sleep_for(std::chrono::seconds(5));

  for (const auto &[messageType, input] : heartbeatModule->buildUnsubscriptions()) { channelDispatcherModule->unsubscribe(messageType, input); }

  if (isRoot) { hllm.terminate(); }

  hllm.await();

  instanceManager->finalize();
}
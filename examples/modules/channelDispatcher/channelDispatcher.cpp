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
#include <modules/service/module.hpp>
#include <system/engine.hpp>
#include <system/channels/messageTypeRegistry.hpp>

#include "channelDispatcher.hpp"
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
  const auto topology           = hwlocTopologyManager.queryTopology();
  auto       d                  = *topology.getDevices().begin();
  auto       memSpaces          = d->getMemorySpaceList();
  auto       bufferMemorySpace  = *memSpaces.begin();
  auto       computeResourcesIt = d->getComputeResourceList().begin();

  // Use only 2 cores
  std::vector<std::shared_ptr<HiCR::ComputeResource>> computeResources;
  for (int i = 0; i < 2; i++)
  {
    computeResources.push_back(*computeResourcesIt);
    computeResourcesIt++;
  }
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

  // Creating taskr object
  nlohmann::json taskrConfig;
  taskrConfig["Task Worker Inactivity Time (Ms)"] = 100;  // Suspend workers if a certain time of inactivity elapses
  taskrConfig["Task Suspend Interval Time (Ms)"]  = 100;  // Workers suspend for this time before checking back
  taskrConfig["Minimum Active Task Workers"]      = 1;    // Have at least one worker active at all times
  taskrConfig["Service Worker Count"]             = 1;    // Have one dedicated service workers at all times to listen for incoming messages
  taskrConfig["Make Task Workers Run Services"]   = true; // Workers will check for meta messages in between executions
  auto taskr                                      = std::make_shared<taskr::Runtime>(taskComputeManager.get(), workerComputeManager.get(), computeResources, taskrConfig);

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

  auto channelDispatcherModule = std::make_unique<hLLM::modules::channelDispatcher::Module>(100);

  // Register message type for the telephone game
  auto &messageTypeRegistry = hllm.getMessageTypeRegistry();
  auto  messageType         = messageTypeRegistry.registerType("examples.telephoneGame");

  std::atomic<bool> rootDone = false;
  const auto       &input    = inputs[0];

  // Add call back for incoming messages of the telephone game type. When a message is received, print it and forward to the next instance.
  // If this is the root instance and it receives a message, it means the message completed the ring and we can terminate.
  channelDispatcherModule->subscribe(
    hLLM::modules::channelDispatcher::Subscription(messageType, input, [&](const std::shared_ptr<hLLM::system::channels::Input>, const hLLM::system::channels::Message &message) {
      const std::string text(reinterpret_cast<const char *>(message.getData()), message.getSize());
      printf("[Instance %lu][Dispatcher] Received: %s\n", instanceId, text.c_str());

      if (isRoot)
      {
        rootDone.store(true);
        return;
      }

      if (outputs.empty()) HICR_THROW_LOGIC("Non-root instance has no output channel.");
      hLLM::system::channels::Message::metadata_t md;
      md.type       = messageType;
      md.groupId    = message.getMetadata().groupId;
      md.sequenceId = message.getMetadata().sequenceId + 1;
      const hLLM::system::channels::Message forwarded(reinterpret_cast<const uint8_t *>(text.data()), text.size(), md);
      outputs[0]->pushMessageLocking(forwarded);
    }));

  auto serviceModule = std::make_unique<hLLM::modules::service::Module>(taskr);

  serviceModule->addService("ChannelDispatcher", channelDispatcherModule->getService());

  // Adding modules to hLLM
  hllm.addModule("ChannelBootstrap", std::move(channelBootstrapModule));
  hllm.addModule("ChannelDispatcher", std::move(channelDispatcherModule));
  hllm.addModule("Service", std::move(serviceModule));

  // Initializing hLLM
  hllm.initialize();

  // Running hLLM
  hllm.run();

  // Root sends one message into the ring and waits until it comes back
  if (isRoot)
  {
    const std::string text = "Hello from root instance!";

    hLLM::system::channels::Message::metadata_t md;
    md.type       = messageType;
    md.groupId    = static_cast<hLLM::system::channels::Message::groupId_t>(instanceId);
    md.sequenceId = 0;

    const hLLM::system::channels::Message message(reinterpret_cast<const uint8_t *>(text.data()), text.size(), md);
    printf("[Instance %lu][Dispatcher] Sending message: %s\n", instanceId, text.c_str());
    outputs[0]->pushMessageLocking(message);
    while (rootDone.load() == false) { std::this_thread::sleep_for(std::chrono::milliseconds(50)); }
    hllm.terminate();
  }

  // Awaiting hLLM termination
  hllm.await();

  // Finalize Instance Manager
  instanceManager->finalize();
}

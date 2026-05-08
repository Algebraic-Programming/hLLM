#pragma once

#include <map>
#include <vector>
#include <memory>
#include <string>
#include <fstream>

#include <nlohmann_json/json.hpp>

#include <hicr/core/instanceManager.hpp>

#include <modules/configuration/deployment.hpp>
#include <system/channels/base.hpp>
#include <system/channels/input.hpp>
#include <system/channels/output.hpp>

#define _REPLICAS_PER_PARTITION 1

__INLINE__ hLLM::system::channels::slotKeys_t defaultChannelKeyBuilder(const HiCR::Instance::instanceId_t        sourceInstanceId,
                                                                       const HiCR::Instance::instanceId_t        targetInstanceId,
                                                                       const hLLM::system::channels::channelId_t channelId)
{
  using key_t         = HiCR::GlobalMemorySlot::globalKey_t;
  const key_t src     = (key_t(sourceInstanceId) & ((1ull << 20) - 1)) << 44;
  const key_t dst     = (key_t(targetInstanceId) & ((1ull << 20) - 1)) << 24;
  const key_t ch      = (key_t(channelId) & ((1ull << 20) - 1)) << 4;
  auto        makeKey = [&](const key_t slot) -> key_t { return src | dst | ch | (slot & 0xFull); };

  hLLM::system::channels::slotKeys_t keys;
  keys.dataConsumerSizesBufferKey                  = makeKey(0);
  keys.dataConsumerPayloadBufferKey                = makeKey(1);
  keys.dataConsumerCoordinationBufferForSizesKey   = makeKey(2);
  keys.dataConsumerCoordinationBufferForPayloadKey = makeKey(3);
  keys.dataProducerCoordinationBufferForSizesKey   = makeKey(4);
  keys.dataProducerCoordinationBufferForPayloadKey = makeKey(5);
  keys.metadataConsumerPayloadBufferKey            = makeKey(6);
  keys.metadataConsumerCoordinationBufferKey       = makeKey(7);
  keys.metadataProducerCoordinationBufferKey       = makeKey(8);
  return keys;
}

__INLINE__ void assignEdgeManagers(hLLM::configuration::Deployment          &deployment,
                                   HiCR::CommunicationManager               *communicationManager,
                                   HiCR::MemoryManager                      *memoryManager,
                                   const std::shared_ptr<HiCR::MemorySpace> &memorySpace)
{
  for (const auto &edge : deployment.getEdges())
  {
    edge->setPayloadCommunicationManager(communicationManager);
    edge->setPayloadMemoryManager(memoryManager);
    edge->setPayloadMemorySpace(memorySpace);
    edge->setCoordinationCommunicationManager(communicationManager);
    edge->setCoordinationMemoryManager(memoryManager);
    edge->setCoordinationMemorySpace(memorySpace);
  }
}

__INLINE__ void inferEdgeEndpointsFromTasks(hLLM::configuration::Deployment &deployment)
{
  std::set<std::string> edgeNameSet;
  for (const auto &edge : deployment.getEdges())
  {
    const auto &edgeName = edge->getName();
    if (edgeNameSet.contains(edgeName)) HICR_THROW_LOGIC("Repeated edge name '%s' in deployment.", edgeName.c_str());
    edgeNameSet.insert(edgeName);
  }
  std::map<std::string, std::string> producerPartitionMap;
  std::map<std::string, std::string> consumerPartitionMap;
  for (const auto &partition : deployment.getPartitions())
  {
    const auto &partitionName = partition->getName();
    for (const auto &task : partition->getTasks())
    {
      for (const auto &output : task->getOutputs())
      {
        if (edgeNameSet.contains(output) == false) HICR_THROW_LOGIC("Task '%s' references undefined output edge '%s'.", task->getFunctionName().c_str(), output.c_str());
        if (producerPartitionMap.contains(output))
          HICR_THROW_LOGIC("Edge '%s' has multiple producer partitions ('%s' and '%s').", output.c_str(), producerPartitionMap.at(output).c_str(), partitionName.c_str());
        producerPartitionMap[output] = partitionName;
      }
      for (const auto &input : task->getInputs())
      {
        if (edgeNameSet.contains(input) == false) HICR_THROW_LOGIC("Task '%s' references undefined input edge '%s'.", task->getFunctionName().c_str(), input.c_str());
        if (consumerPartitionMap.contains(input))
          HICR_THROW_LOGIC("Edge '%s' has multiple consumer partitions ('%s' and '%s').", input.c_str(), consumerPartitionMap.at(input).c_str(), partitionName.c_str());
        consumerPartitionMap[input] = partitionName;
      }
    }
  }
  for (const auto &edge : deployment.getEdges())
  {
    const auto &edgeName = edge->getName();
    if (producerPartitionMap.contains(edgeName) == false) HICR_THROW_LOGIC("Edge '%s' is never produced by any task.", edgeName.c_str());
    if (consumerPartitionMap.contains(edgeName) == false) HICR_THROW_LOGIC("Edge '%s' is never consumed by any task.", edgeName.c_str());
    const auto &producer = producerPartitionMap.at(edgeName);
    const auto &consumer = consumerPartitionMap.at(edgeName);
    if (producer == consumer) HICR_THROW_LOGIC("Edge '%s' is both produced and consumed by partition '%s'.", edgeName.c_str(), producer.c_str());
    edge->setProducer(producer);
    edge->setConsumer(consumer);
  }
}

__INLINE__ void buildLocalChannelsFromDeployment(const hLLM::configuration::Deployment                        &deployment,
                                                 const HiCR::Instance::instanceId_t                            myInstanceId,
                                                 const hLLM::system::channels::keyBuilderFc_t                 &keyBuilder,
                                                 std::vector<std::shared_ptr<hLLM::system::channels::Input>>  &inputs,
                                                 std::vector<std::shared_ptr<hLLM::system::channels::Output>> &outputs)
{
  std::map<std::string, HiCR::Instance::instanceId_t> partitionToInstance;
  for (const auto &partition : deployment.getPartitions()) partitionToInstance[partition->getName()] = partition->getCoordinatorInstanceId();
  for (hLLM::configuration::Edge::edgeIndex_t edgeIdx = 0; edgeIdx < deployment.getEdges().size(); edgeIdx++)
  {
    const auto &edge              = deployment.getEdges()[edgeIdx];
    const auto &producerPartition = edge->getProducer();
    const auto &consumerPartition = edge->getConsumer();
    if (partitionToInstance.contains(producerPartition) == false)
      HICR_THROW_LOGIC("Edge '%s' producer partition '%s' is not present in deployment partition map.", edge->getName().c_str(), producerPartition.c_str());
    if (partitionToInstance.contains(consumerPartition) == false)
      HICR_THROW_LOGIC("Edge '%s' consumer partition '%s' is not present in deployment partition map.", edge->getName().c_str(), consumerPartition.c_str());
    const auto sourceInstanceId = partitionToInstance.at(producerPartition);
    const auto targetInstanceId = partitionToInstance.at(consumerPartition);
    const auto channelId        = static_cast<hLLM::system::channels::channelId_t>(edgeIdx);
    if (myInstanceId == sourceInstanceId) outputs.push_back(std::make_shared<hLLM::system::channels::Output>(*edge, channelId, sourceInstanceId, targetInstanceId, keyBuilder));
    if (myInstanceId == targetInstanceId) inputs.push_back(std::make_shared<hLLM::system::channels::Input>(*edge, channelId, sourceInstanceId, targetInstanceId, keyBuilder));
  }
}

__INLINE__ void readAndParseConfiguration(char *argv[], hLLM::configuration::Deployment &deployment, std::shared_ptr<HiCR::InstanceManager> &instanceManager)
{
  // Getting config file name from arguments
  std::string hllmConfigFilePath = std::string(argv[1]);

  // Parsing request file contents to a JSON object
  std::ifstream hllmConfigFs(hllmConfigFilePath);
  auto          hllmConfigJs = nlohmann::json::parse(hllmConfigFs);

  // Parsing config file using hLLM
  deployment.deserialize(hllmConfigJs);
  inferEdgeEndpointsFromTasks(deployment);

  // Calculating the number of instances required (1 per partition that runs the coordinator and a replica)
  const auto instancesRequired = deployment.getPartitions().size() * _REPLICAS_PER_PARTITION;

  // Checking I have the correct number of instances (only one)
  if (instanceManager->getInstances().size() != instancesRequired)
  {
    fprintf(stderr, "Error: %lu instances provided, but %lu are required\n", instanceManager->getInstances().size(), instancesRequired);
    instanceManager->abort(-1);
  }

  // Assigning this example to run in a single instance
  auto instance = instanceManager->getInstances().begin();

  // Assigning instance ids to the partitions
  for (auto p : deployment.getPartitions())
  {
    // Getting instance Id that will run this partition (only one)
    const auto partitionInstanceId = (*instance)->getId();

    // Update instance
    instance++;

    // Setting this partition to be executed by the same instance than replica zero
    p->setCoordinatorInstanceId(partitionInstanceId);

    // Adding a replicas to this partition, with the same instance as the coordinator
    for (size_t i = 0; i < _REPLICAS_PER_PARTITION; i++)
    {
      const auto replica = std::make_shared<hLLM::configuration::Replica>(partitionInstanceId);
      p->addReplica(replica);
    }
  }
  printf("[Instance %lu] Parsed deployment configuration\n", instanceManager->getCurrentInstance()->getId());
}
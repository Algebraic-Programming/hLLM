#pragma once

#include <fstream>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include <nlohmann_json/json.hpp>

#include <hicr/core/instanceManager.hpp>

#include <modules/configuration/deployment.hpp>
#include <modules/configuration/replica.hpp>
#include <system/channels/base.hpp>
#include <system/channels/input.hpp>
#include <system/channels/output.hpp>

#define _REPLICAS_PER_PARTITION 1

struct localInput_t
{
  HiCR::Instance::instanceId_t                   sourceInstanceId;
  std::shared_ptr<hLLM::system::channels::Input> channel;
};

struct localOutput_t
{
  HiCR::Instance::instanceId_t                    targetInstanceId;
  std::shared_ptr<hLLM::system::channels::Output> channel;
};

__INLINE__ hLLM::system::channels::slotKeys_t defaultChannelKeyBuilder(const HiCR::Instance::instanceId_t        sourceInstanceId,
                                                                       const HiCR::Instance::instanceId_t        targetInstanceId,
                                                                       const hLLM::system::channels::channelId_t channelId)
{
  using key_t         = HiCR::GlobalMemorySlot::globalKey_t;
  const key_t src     = (key_t(sourceInstanceId) & ((1ull << 20) - 1)) << 44;
  const key_t dst     = (key_t(targetInstanceId) & ((1ull << 20) - 1)) << 24;
  const key_t ch      = (key_t(channelId) & ((1ull << 20) - 1)) << 4;
  auto        makeKey = [&](const key_t slot) -> key_t { return src | dst | ch | (slot & 0xFull); };
  return {.dataConsumerSizesBufferKey                  = makeKey(0),
          .dataConsumerPayloadBufferKey                = makeKey(1),
          .dataConsumerCoordinationBufferForSizesKey   = makeKey(2),
          .dataConsumerCoordinationBufferForPayloadKey = makeKey(3),
          .dataProducerCoordinationBufferForSizesKey   = makeKey(4),
          .dataProducerCoordinationBufferForPayloadKey = makeKey(5),
          .metadataConsumerPayloadBufferKey            = makeKey(6),
          .metadataConsumerCoordinationBufferKey       = makeKey(7),
          .metadataProducerCoordinationBufferKey       = makeKey(8)};
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

__INLINE__ void buildLocalChannelsFromDeploymentWithIds(const hLLM::configuration::Deployment        &deployment,
                                                        const HiCR::Instance::instanceId_t            myInstanceId,
                                                        const hLLM::system::channels::keyBuilderFc_t &keyBuilder,
                                                        std::vector<localInput_t>                    &inputs,
                                                        std::vector<localOutput_t>                   &outputs)
{
  std::map<std::string, HiCR::Instance::instanceId_t> partitionToInstance;
  for (const auto &partition : deployment.getPartitions()) partitionToInstance[partition->getName()] = partition->getCoordinatorInstanceId();
  for (hLLM::configuration::Edge::edgeIndex_t edgeIdx = 0; edgeIdx < deployment.getEdges().size(); edgeIdx++)
  {
    const auto &edge             = deployment.getEdges()[edgeIdx];
    const auto  sourceInstanceId = partitionToInstance.at(edge->getProducer());
    const auto  targetInstanceId = partitionToInstance.at(edge->getConsumer());
    const auto  channelId        = static_cast<hLLM::system::channels::channelId_t>(edgeIdx);
    if (myInstanceId == sourceInstanceId)
    {
      outputs.push_back(localOutput_t{.targetInstanceId = targetInstanceId,
                                      .channel          = std::make_shared<hLLM::system::channels::Output>(*edge, channelId, sourceInstanceId, targetInstanceId, keyBuilder)});
    }
    if (myInstanceId == targetInstanceId)
    {
      inputs.push_back(localInput_t{.sourceInstanceId = sourceInstanceId,
                                    .channel          = std::make_shared<hLLM::system::channels::Input>(*edge, channelId, sourceInstanceId, targetInstanceId, keyBuilder)});
    }
  }
}

// Compatibility helper (if some code still expects raw vectors).
__INLINE__ void buildLocalChannelsFromDeployment(const hLLM::configuration::Deployment                        &deployment,
                                                 const HiCR::Instance::instanceId_t                            myInstanceId,
                                                 const hLLM::system::channels::keyBuilderFc_t                 &keyBuilder,
                                                 std::vector<std::shared_ptr<hLLM::system::channels::Input>>  &inputs,
                                                 std::vector<std::shared_ptr<hLLM::system::channels::Output>> &outputs)
{
  std::vector<localInput_t>  localInputs;
  std::vector<localOutput_t> localOutputs;
  buildLocalChannelsFromDeploymentWithIds(deployment, myInstanceId, keyBuilder, localInputs, localOutputs);
  for (const auto &in : localInputs) inputs.push_back(in.channel);
  for (const auto &out : localOutputs) outputs.push_back(out.channel);
}

__INLINE__ void readAndParseConfiguration(char *argv[], hLLM::configuration::Deployment &deployment, std::shared_ptr<HiCR::InstanceManager> &instanceManager)
{
  std::string   hllmConfigFilePath = std::string(argv[1]);
  std::ifstream hllmConfigFs(hllmConfigFilePath);
  auto          hllmConfigJs = nlohmann::json::parse(hllmConfigFs);
  deployment.deserialize(hllmConfigJs);
  inferEdgeEndpointsFromTasks(deployment);
  const auto instancesRequired = deployment.getPartitions().size() * _REPLICAS_PER_PARTITION;
  if (instanceManager->getInstances().size() != instancesRequired)
  {
    fprintf(stderr, "Error: %lu instances provided, but %lu are required\n", instanceManager->getInstances().size(), instancesRequired);
    instanceManager->abort(-1);
  }
  auto instance = instanceManager->getInstances().begin();
  for (auto p : deployment.getPartitions())
  {
    const auto partitionInstanceId = (*instance)->getId();
    instance++;
    p->setCoordinatorInstanceId(partitionInstanceId);
    for (size_t i = 0; i < _REPLICAS_PER_PARTITION; i++)
    {
      const auto replica = std::make_shared<hLLM::configuration::Replica>(partitionInstanceId);
      p->addReplica(replica);
    }
  }
  printf("[Instance %lu] Parsed deployment configuration\n", instanceManager->getCurrentInstance()->getId());
}
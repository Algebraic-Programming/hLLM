#pragma once

#include <memory>
#include <vector>
#include <hicr/core/exceptions.hpp>
#include <hicr/core/definitions.hpp>
#include <taskr/taskr.hpp>
#include "configuration/deployment.hpp"
#include "edge/input.hpp"
#include "edge/output.hpp"

namespace hLLM
{

class Coordinator final
{
  public:

  Coordinator() = delete;

  Coordinator(
    const configuration::Deployment deployment,
    const configuration::Partition::partitionIndex_t partitionIdx
  ) :
    _deployment(deployment),
    _partitionIdx(partitionIdx)
  {
    // Get my partition configuration
    const auto& partitionConfiguration = _deployment.getPartitions()[_partitionIdx];

    // Get my partition name
    const auto& partitionName = partitionConfiguration->getName();

    // Getting list of edges in the deployment
    const auto& edgeConfigs = _deployment.getEdges();

    // Iterating through edges by their index
    for (configuration::Edge::edgeIndex_t edgeIdx = 0; edgeIdx < edgeConfigs.size(); edgeIdx++)
    {
      // Getting edge object by index
      const auto edgeConfig = edgeConfigs[edgeIdx];

      // If I am a consumer in this edge
      if (edgeConfig->getConsumer() == partitionName)
      {
        // Create the input edges to pass this information to the receiving partition
        _partitionInputs.push_back(std::make_shared<edge::Input>(*edgeConfig, edgeIdx, edge::Base::coordinatorReplicaIndex));

        // Create the output edges to pass this distribute the input to any of the replicas
        for (configuration::Replica::replicaIndex_t replicaIdx = 0; replicaIdx < partitionConfiguration->getReplicas().size(); replicaIdx++)
          _replicaOutputs.push_back(std::make_shared<edge::Output>(*edgeConfig, edgeIdx, replicaIdx));
      } 

      // If I am a producer in this edge
      if (edgeConfig->getProducer() == partitionName)
      {
        // Create the output edge to pass this information to the receiving partition
        _partitionOutputs.push_back(std::make_shared<edge::Output>(*edgeConfig, edgeIdx, edge::Base::coordinatorReplicaIndex));

        // Create the input edges to receive the output from any of the replicas
        for (configuration::Replica::replicaIndex_t replicaIdx = 0; replicaIdx < partitionConfiguration->getReplicas().size(); replicaIdx++)
          _replicaInputs.push_back(std::make_shared<edge::Input>(*edgeConfig, edgeIdx, replicaIdx));
      } 
    }

    printf("Deploying Partition Index %lu - Name: %s - %lu Consumer / %lu Producer edges...\n", _partitionIdx, partitionName.c_str(), _partitionInputs.size(), _partitionOutputs.size());
  }

  /// This function completes the initialization of the edges, after the memory slot exchanges are completed
  __INLINE__ void initializeEdges(const HiCR::GlobalMemorySlot::tag_t tag)
  {
    for (const auto& edge : _partitionInputs)  edge->initialize(tag);
    for (const auto& edge : _partitionOutputs) edge->initialize(tag);
    for (const auto& edge : _replicaInputs)    edge->initialize(tag);
    for (const auto& edge : _replicaOutputs)   edge->initialize(tag);
  }

  ~Coordinator() = default;

  __INLINE__ void getMemorySlotsToExchange(std::vector<hLLM::edge::memorySlotExchangeInfo_t>& memorySlots)
  {
    for (const auto& edge : _partitionInputs)  edge->getMemorySlotsToExchange(memorySlots);
    for (const auto& edge : _partitionOutputs) edge->getMemorySlotsToExchange(memorySlots);
    for (const auto& edge : _replicaInputs)    edge->getMemorySlotsToExchange(memorySlots);
    for (const auto& edge : _replicaOutputs)   edge->getMemorySlotsToExchange(memorySlots);
  }

  private:

  const configuration::Deployment _deployment;
  const configuration::Partition::partitionIndex_t _partitionIdx;

  // Input / Output  edges from other partition coordinators
  std::vector<std::shared_ptr<edge::Input>> _partitionInputs;
  std::vector<std::shared_ptr<edge::Output>> _partitionOutputs;

  // Input / Output edges from my partition's replicas
  std::vector<std::shared_ptr<edge::Input>> _replicaInputs;
  std::vector<std::shared_ptr<edge::Output>> _replicaOutputs;

}; // class Coordinator

} // namespace hLLM
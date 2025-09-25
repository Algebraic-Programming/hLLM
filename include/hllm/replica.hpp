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

class Replica final
{
  public:

  Replica() = delete;

  Replica(
    const configuration::Deployment deployment,
    const configuration::Partition::partitionIndex_t  partitionIdx,
    const configuration::Replica::replicaIndex_t replicaIdx
  ) :
    _deployment(deployment),
    _partitionIdx(partitionIdx),
    _replicaIdx(replicaIdx)
  {
    // Get my partition configuration
    const auto& partitionConfiguration = _deployment.getPartitions()[_partitionIdx];

    // Get my replica configuration
    const auto& replicaConfiguration = partitionConfiguration->getReplicas()[_replicaIdx];

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
        _coordinatorInputs.push_back(std::make_shared<edge::Input>(*edgeConfig, edgeIdx, _replicaIdx));
      } 

      // If I am a producer in this edge
      if (edgeConfig->getProducer() == partitionName)
      {
        // Create the output edge to pass this information to the receiving partition
        _coordinatorOutputs.push_back(std::make_shared<edge::Output>(*edgeConfig, edgeIdx, _replicaIdx));
      } 
    }

    printf("Deploying Replica Index P%lu/R%lu - Name: %s - %lu Consumer / %lu Producer edges...\n", _partitionIdx, _replicaIdx, replicaConfiguration->getName().c_str(), _coordinatorInputs.size(), _coordinatorOutputs.size());
  }

  ~Replica() = default;

  /// This function completes the initialization of the edges, after the memory slot exchanges are completed
  __INLINE__ void initializeEdges(const HiCR::GlobalMemorySlot::tag_t tag)
  {
    for (const auto& edge : _coordinatorInputs)  edge->initialize(tag);
    for (const auto& edge : _coordinatorOutputs) edge->initialize(tag);
  }

  __INLINE__ void getMemorySlotsToExchange(std::vector<hLLM::edge::memorySlotExchangeInfo_t>& memorySlots)
  {
    for (const auto& edge : _coordinatorInputs)  edge->getMemorySlotsToExchange(memorySlots);
    for (const auto& edge : _coordinatorOutputs) edge->getMemorySlotsToExchange(memorySlots);
  }

  private:

  const configuration::Deployment _deployment;
  const configuration::Partition::partitionIndex_t _partitionIdx;
  const configuration::Replica::replicaIndex_t _replicaIdx;

  // Input/Output edges from/to the coordinator
  std::vector<std::shared_ptr<edge::Input>> _coordinatorInputs;
  std::vector<std::shared_ptr<edge::Output>> _coordinatorOutputs;

}; // class Replica

} // namespace hLLM
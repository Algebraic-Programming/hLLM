#pragma once

#include <memory>
#include <vector>
#include <hicr/core/exceptions.hpp>
#include <hicr/core/definitions.hpp>
#include <taskr/taskr.hpp>
#include "../configuration/deployment.hpp"
#include "../edge/input.hpp"
#include "../edge/output.hpp"
#include "../messages/heartbeat.hpp"
#include "../partition.hpp"

namespace hLLM::replica
{

class Replica final : public hLLM::Partition
{
  public:

  Replica() = delete;

  Replica(
    const configuration::Deployment deployment,
    const configuration::Partition::partitionIndex_t  partitionIdx,
    const configuration::Replica::replicaIndex_t replicaIdx,
    taskr::Runtime* const taskr
  ) : Partition(deployment, partitionIdx, taskr),
    _replicaIdx(replicaIdx)
  {
    // Get my partition configuration
    const auto& partitionConfiguration = _deployment.getPartitions()[_partitionIdx];

    // Get my partition name
    const auto& partitionName = partitionConfiguration->getName();

    // Iterating through input edges to create a connection with the coordinator on that edge
    for (const auto& edge : _inputEdges)
    {
      const auto edgeIdx = edge.index;
      const auto& edgeConfig = edge.config;
      _coordinatorDataInputs.push_back(std::make_shared<edge::Input>(*edgeConfig, edge::edgeType_t::coordinatorToReplica, edgeIdx, _partitionIdx, _partitionIdx, _replicaIdx));
    }
    
    // Iterating through output edges to create a connection with the coordinator on that edge
    for (const auto& edge : _outputEdges)
    {
      const auto edgeIdx = edge.index;
      const auto& edgeConfig = edge.config;
      _coordinatorDataOutputs.push_back(std::make_shared<edge::Output>(*edgeConfig, edge::edgeType_t::replicaToCoordinator, edgeIdx, _partitionIdx, _partitionIdx, _replicaIdx));
    }

    // Create Control edges with my partition coordinator
    _coordinatorControlInput = std::make_shared<edge::Input>(*_controlEdgeConfig, edge::edgeType_t::coordinatorToReplica, edge::Base::controlEdgeIndex, _partitionIdx, _partitionIdx, _replicaIdx);
    _coordinatorControlOutput = std::make_shared<edge::Output>(*_controlEdgeConfig, edge::edgeType_t::replicaToCoordinator, edge::Base::controlEdgeIndex, _partitionIdx, _partitionIdx, _replicaIdx);
  }

  ~Replica() = default;

  /// This function completes the initialization of the edges, after the memory slot exchanges are completed
  __INLINE__ void initializeEdges(const HiCR::GlobalMemorySlot::tag_t tag)
  {
    for (const auto& edge : _coordinatorDataInputs)  edge->initialize(tag);
    for (const auto& edge : _coordinatorDataOutputs) edge->initialize(tag);
    _coordinatorControlInput->initialize(tag);
    _coordinatorControlOutput->initialize(tag);
  }

  __INLINE__ void getMemorySlotsToExchange(std::vector<hLLM::edge::memorySlotExchangeInfo_t>& memorySlots)
  {
    for (const auto& edge : _coordinatorDataInputs)  edge->getMemorySlotsToExchange(memorySlots);
    for (const auto& edge : _coordinatorDataOutputs) edge->getMemorySlotsToExchange(memorySlots);
    _coordinatorControlInput->getMemorySlotsToExchange(memorySlots);
    _coordinatorControlOutput->getMemorySlotsToExchange(memorySlots);
  }

  private: 
  
  /// This function initializes the workload of the partition replica role
  __INLINE__ void initializeImpl() override
  {
    printf("[Replica %lu / %lu] Initializing...\n", _partitionIdx, _replicaIdx);

    //////// Adding heartbeat service for my coordinator's control edge
    subscribeHeartbeatEdge(_coordinatorControlOutput);

    // Registering a handler for the handshake message 
    subscribeMessageEdge(_coordinatorControlInput);
    subscribeMessageHandler(hLLM::messages::messageTypes::heartbeat, [this](const std::shared_ptr<edge::Input> edge, const hLLM::messages::Base* message){ heartbeatMessageHandler(edge, static_cast<const hLLM::messages::Heartbeat*>(message)); });
  }

  void heartbeatMessageHandler(const std::shared_ptr<edge::Input> edge, const hLLM::messages::Heartbeat* message)
  {
    printf("[Replica %lu / %lu] Received heartbeat from coordinator.\n", _partitionIdx, _replicaIdx);
  }

  // Identifier for the replica index
  const configuration::Replica::replicaIndex_t _replicaIdx;

  // Data Input/Output edges from/to the coordinator
  std::vector<std::shared_ptr<edge::Input>> _coordinatorDataInputs;
  std::vector<std::shared_ptr<edge::Output>> _coordinatorDataOutputs;

  // Control Input/Output edges from/to the coordinator
  std::shared_ptr<edge::Input> _coordinatorControlInput;
  std::shared_ptr<edge::Output> _coordinatorControlOutput;
}; // class Replica

} // namespace hLLM::replica
#pragma once

#include <memory>
#include <vector>
#include <hicr/core/exceptions.hpp>
#include <hicr/core/definitions.hpp>
#include <taskr/taskr.hpp>
#include "../configuration/deployment.hpp"
#include "../edge/input.hpp"
#include "../edge/output.hpp"
#include "../messages/base.hpp"
#include "../messages/heartbeat.hpp"
#include "../partition.hpp"
#include "replica.hpp"

namespace hLLM::coordinator
{

class Coordinator final : public hLLM::Partition
{
  public:

  Coordinator() = delete;
  ~Coordinator() = default;

  Coordinator(
    const configuration::Deployment deployment,
    const configuration::Partition::partitionIndex_t partitionIdx,
    taskr::Runtime* const taskr
  ) :
    Partition(deployment, partitionIdx, taskr)
  {
    // Get my partition configuration
    const auto& partitionConfiguration = _deployment.getPartitions()[_partitionIdx];

    // Get my partition name
    const auto& partitionName = partitionConfiguration->getName();

    // Getting list of replicas in the partition
    const auto& replicas = partitionConfiguration->getReplicas();

    // Filling replica set
    for (configuration::Replica::replicaIndex_t replicaIndex = 0; replicaIndex < replicas.size(); replicaIndex++)
    {
      auto newReplica = std::make_shared<Replica>(_partitionIdx, replicaIndex);
      _replicas.push_back(std::move(newReplica));
    }
    // Iterating through input edges to create a connection with replicas and peer coordinators on that input
    for (const auto& edge : _inputEdges)
    {
      const auto edgeIdx = edge.index;
      const auto& edgeConfig = edge.config;
      const auto producerPartitionIdx = edge.producerPartitionIndex;
      const auto consumerPartitionIdx = edge.consumerPartitionIndex;
        
      // Create the input edges to pass this information to the receiving partition
      _partitionDataInputs.push_back(std::make_shared<edge::Input>(*edgeConfig, edge::edgeType_t::coordinatorToCoordinator, edgeIdx, producerPartitionIdx, consumerPartitionIdx, edge::Base::coordinatorReplicaIndex));

      // Create the output edges to pass this distribute the input to any of the replicas
      for (configuration::Replica::replicaIndex_t replicaIdx = 0; replicaIdx < partitionConfiguration->getReplicas().size(); replicaIdx++)
        _replicaDataOutputs.push_back(std::make_shared<edge::Output>(*edgeConfig, edge::edgeType_t::coordinatorToReplica, edgeIdx, _partitionIdx, _partitionIdx, replicaIdx));
    } 

    // Iterating through output edges to create a connection with replicas and peer coordinators on that output
    for (const auto& edge : _outputEdges)
    {
      const auto edgeIdx = edge.index;
      const auto& edgeConfig = edge.config;
      const auto producerPartitionIdx = edge.producerPartitionIndex;
      const auto consumerPartitionIdx = edge.consumerPartitionIndex;

      // Create the output edge to pass this information to the receiving partition
      _partitionDataOutputs.push_back(std::make_shared<edge::Output>(*edgeConfig, edge::edgeType_t::coordinatorToCoordinator, edgeIdx, producerPartitionIdx, consumerPartitionIdx, edge::Base::coordinatorReplicaIndex));

      // Create the input edges to receive the output from any of the replicas
      for (configuration::Replica::replicaIndex_t replicaIdx = 0; replicaIdx < partitionConfiguration->getReplicas().size(); replicaIdx++)
        _replicaDataInputs.push_back(std::make_shared<edge::Input>(*edgeConfig, edge::edgeType_t::replicaToCoordinator, edgeIdx, _partitionIdx, _partitionIdx, replicaIdx));
    } 

    // For each replica, also create a Control edge
    const auto& controlBufferConfig = _deployment.getControlBufferConst();
    auto replicaControlEdgeConfig = configuration::Edge("Control Edge", partitionName, partitionName, "Copy", controlBufferConfig.capacity, controlBufferConfig.size);
    replicaControlEdgeConfig.setCoordinationCommunicationManager(controlBufferConfig.communicationManager);
    replicaControlEdgeConfig.setCoordinationMemoryManager(controlBufferConfig.memoryManager);
    replicaControlEdgeConfig.setCoordinationMemorySpace(controlBufferConfig.memorySpace);
    replicaControlEdgeConfig.setPayloadCommunicationManager(controlBufferConfig.communicationManager);
    replicaControlEdgeConfig.setPayloadMemoryManager(controlBufferConfig.memoryManager);
    replicaControlEdgeConfig.setPayloadMemorySpace(controlBufferConfig.memorySpace);
    for (configuration::Replica::replicaIndex_t replicaIdx = 0; replicaIdx < partitionConfiguration->getReplicas().size(); replicaIdx++)
    {
      _replicaControlInputs.push_back(std::make_shared<edge::Input>(replicaControlEdgeConfig, edge::edgeType_t::replicaToCoordinator, edge::Base::controlEdgeIndex, _partitionIdx, _partitionIdx, replicaIdx));
      _replicaControlOutputs.push_back(std::make_shared<edge::Output>(replicaControlEdgeConfig, edge::edgeType_t::coordinatorToReplica, edge::Base::controlEdgeIndex, _partitionIdx, _partitionIdx, replicaIdx));
    }
  }

  /// This function completes the initialization of the edges, after the memory slot exchanges are completed
  __INLINE__ void initializeEdges(const HiCR::GlobalMemorySlot::tag_t tag)
  {
    // Data Edges
    for (const auto& edge : _partitionDataInputs)  edge->initialize(tag);
    for (const auto& edge : _partitionDataOutputs) edge->initialize(tag);
    for (const auto& edge : _replicaDataInputs)    edge->initialize(tag);
    for (const auto& edge : _replicaDataOutputs)   edge->initialize(tag);

    // Control edges
    for (const auto& edge : _replicaControlInputs)    edge->initialize(tag);
    for (const auto& edge : _replicaControlOutputs)   edge->initialize(tag);
  }

  __INLINE__ void getMemorySlotsToExchange(std::vector<hLLM::edge::memorySlotExchangeInfo_t>& memorySlots)
  {
    for (const auto& edge : _partitionDataInputs)  edge->getMemorySlotsToExchange(memorySlots);
    for (const auto& edge : _partitionDataOutputs) edge->getMemorySlotsToExchange(memorySlots);
    for (const auto& edge : _replicaDataInputs)    edge->getMemorySlotsToExchange(memorySlots);
    for (const auto& edge : _replicaDataOutputs)   edge->getMemorySlotsToExchange(memorySlots);

    for (const auto& edge : _replicaControlInputs)    edge->getMemorySlotsToExchange(memorySlots);
    for (const auto& edge : _replicaControlOutputs)   edge->getMemorySlotsToExchange(memorySlots);
  }

  private:

  /// This function adds the services and initial task for the coordinator
  __INLINE__ void initializeImpl() override
  {
    // Get my partition configuration
    const auto& partitionConfiguration = _deployment.getPartitions()[_partitionIdx];

    // Get my partition name
    const auto& partitionName = partitionConfiguration->getName();

    // Welcome message
    printf("Initializing Partition Coordinator Index %lu - Name: %s - %lu Consumer / %lu Producer edges...\n", _partitionIdx, partitionName.c_str(), _partitionDataInputs.size(), _partitionDataOutputs.size());

    // Subscribing to the heartbeat sending service for my replicas
    for (const auto& edge : _replicaControlOutputs) subscribeHeartbeatEdge(edge);

    // Subscribing input edges to the control message service for my replicas
    for (const auto& edge : _replicaControlInputs) subscribeControlMessageEdge(edge);

    // Registering a handler for the handshake message 
    subscribeControlMessageHandler(hLLM::messages::messageTypes::heartbeat, [this](const std::shared_ptr<edge::Input> edge, const hLLM::messages::Base* message){ heartbeatMessageHandler(edge, static_cast<const hLLM::messages::Heartbeat*>(message)); });
  }

  void heartbeatMessageHandler(const std::shared_ptr<edge::Input> edge, const hLLM::messages::Heartbeat* message)
  {
    const auto replicaIdx = edge->getReplicaIndex();
    printf("[Coordinator %lu] Received heartbeat from replica %lu.\n", _partitionIdx, replicaIdx);
  }

  // Container for partition replica objects
  std::vector<std::shared_ptr<Replica>> _replicas;

  // Data Input / Output edges from other partition coordinators
  std::vector<std::shared_ptr<edge::Input>> _partitionDataInputs;
  std::vector<std::shared_ptr<edge::Output>> _partitionDataOutputs;

  // Data Input / Output edges from my partition's replicas
  std::vector<std::shared_ptr<edge::Input>> _replicaDataInputs;
  std::vector<std::shared_ptr<edge::Output>> _replicaDataOutputs;

  // Control Input / Output edges from my partition's replicas
  std::vector<std::shared_ptr<edge::Input>> _replicaControlInputs;
  std::vector<std::shared_ptr<edge::Output>> _replicaControlOutputs;
}; // class Coordinator

} // namespace hLLM::coordinator
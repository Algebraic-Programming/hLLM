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

    //////// Adding service to listen for incoming control messages. 
    // Interval is zero because we don't want any delays in listening for incoming messages
    _taskr->addService(&_taskrControlMessagesListeningService);

    //////// Adding heartbeat service for my replicas
    for (const auto& edge : _replicaControlOutputs) subscribeHeartbeatEdge(edge);
  }

  /////////// Services
  // These will keep being checked constantly as long as there is a worker free to take them.
  // But even if there is no such worker, there is a reserve of taskR threads reserved the execute them.
  // This is to prevent services from starving
  
  // Control Message-listening service
  __INLINE__ void controlMessagesListeningService()
  {
    // printf("[Coordinator %lu] Checking for messages...\n", _partitionIdx);

    // Checking, for all replicas' edges, whether any of them has a pending message
    for (const auto& input : _replicaControlInputs) if (input->hasMessage())
    {
      // Getting message from input edge
      const auto message = input->getMessage();
      const auto messageType = message.getMetadata().type;

      // Getting edge metadata
      const auto replicaIdx = input->getReplicaIndex();

      // Deciding what to do based on message type
      bool isTypeRecognized = false;
      if (messageType == hLLM::messages::messageTypes::heartbeat)
      {
        printf("[Coordinator %lu] Received heartbeat from replica %lu with message: %s\n", _partitionIdx, replicaIdx, message.getData());
        isTypeRecognized = true;
      }

      if (isTypeRecognized == false) HICR_THROW_RUNTIME("[Coordinator %lu] Received unrecognized message type %lu from replica %lu\n", _partitionIdx, messageType, replicaIdx);

      // Immediately disposing (popping) of message out of the edge
      input->popMessage();
    } 
  }
  taskr::Service::serviceFc_t _taskrControlMessagesListeningServiceFunction = [this](){ this->controlMessagesListeningService(); };
  taskr::Service _taskrControlMessagesListeningService = taskr::Service(_taskrControlMessagesListeningServiceFunction, 0);

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
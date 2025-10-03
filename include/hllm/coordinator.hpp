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
  ~Coordinator() = default;

  Coordinator(
    const configuration::Deployment deployment,
    const configuration::Partition::partitionIndex_t partitionIdx,
    taskr::Runtime* const taskr
  ) :
    _deployment(deployment),
    _partitionIdx(partitionIdx),
    _taskr(taskr)
  {
    // Get my partition configuration
    const auto& partitionConfiguration = _deployment.getPartitions()[_partitionIdx];

    // Get my partition name
    const auto& partitionName = partitionConfiguration->getName();

    // Getting list of edges in the deployment
    const auto& edgeConfigs = _deployment.getEdges();

    // Iterating through edges by their index and creating them
    for (configuration::Edge::edgeIndex_t edgeIdx = 0; edgeIdx < edgeConfigs.size(); edgeIdx++)
    {
      // Getting edge object by index
      const auto edgeConfig = edgeConfigs[edgeIdx];

      // If I am a consumer in this edge
      if (edgeConfig->getConsumer() == partitionName)
      {
        // Looking for the index of the producer of the input
        const auto& producerPartitionName = edgeConfig->getProducer(); 
        configuration::Partition::partitionIndex_t producerPartitionIdx = 0;
        bool producerFound = false;
        for (const auto& partition : _deployment.getPartitions())
        {
          if (partition->getName() == producerPartitionName)
          {
            producerFound = true;
            break;
          } 
          producerPartitionIdx++;
        }

        // Sanity check
        if (producerFound == false) HICR_THROW_RUNTIME("Could not find index of producer '%s' for edge '%s'. This is a bug in hLLM.", producerPartitionName.c_str(), edgeConfig->getName().c_str());
        
        // Create the input edges to pass this information to the receiving partition
        _partitionDataInputs.push_back(std::make_shared<edge::Input>(*edgeConfig, edge::edgeType_t::coordinatorToCoordinator, edgeIdx, producerPartitionIdx, _partitionIdx, edge::Base::coordinatorReplicaIndex));

        // Create the output edges to pass this distribute the input to any of the replicas
        for (configuration::Replica::replicaIndex_t replicaIdx = 0; replicaIdx < partitionConfiguration->getReplicas().size(); replicaIdx++)
          _replicaDataOutputs.push_back(std::make_shared<edge::Output>(*edgeConfig, edge::edgeType_t::coordinatorToReplica, edgeIdx, _partitionIdx, _partitionIdx, replicaIdx));
      } 

      // If I am a producer in this edge
      if (edgeConfig->getProducer() == partitionName)
      {
        // Looking for the index of the consumer of the input
        const auto& consumerPartitionName = edgeConfig->getConsumer(); 
        configuration::Partition::partitionIndex_t consumerPartitionIdx = 0;
        bool consumerFound = false;
        for (const auto& partition : _deployment.getPartitions())
        {
          if (partition->getName() == consumerPartitionName)
          {
            consumerFound = true;
            break;
          } 
          consumerPartitionIdx++;
        }

        // Sanity check
        if (consumerFound == false) HICR_THROW_RUNTIME("Could not find index of consumer '%s' for edge '%s'. This is a bug in hLLM.", consumerPartitionName.c_str(), edgeConfig->getName().c_str());

        // Create the output edge to pass this information to the receiving partition
        _partitionDataOutputs.push_back(std::make_shared<edge::Output>(*edgeConfig, edge::edgeType_t::coordinatorToCoordinator, edgeIdx, _partitionIdx, consumerPartitionIdx, edge::Base::coordinatorReplicaIndex));

        // Create the input edges to receive the output from any of the replicas
        for (configuration::Replica::replicaIndex_t replicaIdx = 0; replicaIdx < partitionConfiguration->getReplicas().size(); replicaIdx++)
          _replicaDataInputs.push_back(std::make_shared<edge::Input>(*edgeConfig, edge::edgeType_t::replicaToCoordinator, edgeIdx, _partitionIdx, _partitionIdx, replicaIdx));
      } 
    }

    // For each replica, also create a Control edge
    auto replicaControlEdgeConfig = configuration::Edge("Control Edge", partitionName, partitionName, "Copy", partitionConfiguration->getControlBufferCapacity(), partitionConfiguration->getControlBufferSize());
    replicaControlEdgeConfig.setCoordinationCommunicationManager(partitionConfiguration->getControlCommunicationManager());
    replicaControlEdgeConfig.setCoordinationMemoryManager(partitionConfiguration->getControlMemoryManager());
    replicaControlEdgeConfig.setCoordinationMemorySpace(partitionConfiguration->getControlMemorySpace());
    replicaControlEdgeConfig.setPayloadCommunicationManager(partitionConfiguration->getControlCommunicationManager());
    replicaControlEdgeConfig.setPayloadMemoryManager(partitionConfiguration->getControlMemoryManager());
    replicaControlEdgeConfig.setPayloadMemorySpace(partitionConfiguration->getControlMemorySpace());
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
    for (const auto& edge : _partitionControlInputs)  edge->initialize(tag);
    for (const auto& edge : _partitionControlOutputs) edge->initialize(tag);
    for (const auto& edge : _replicaControlInputs)    edge->initialize(tag);
    for (const auto& edge : _replicaControlOutputs)   edge->initialize(tag);
  }

  __INLINE__ void getMemorySlotsToExchange(std::vector<hLLM::edge::memorySlotExchangeInfo_t>& memorySlots)
  {
    for (const auto& edge : _partitionDataInputs)  edge->getMemorySlotsToExchange(memorySlots);
    for (const auto& edge : _partitionDataOutputs) edge->getMemorySlotsToExchange(memorySlots);
    for (const auto& edge : _replicaDataInputs)    edge->getMemorySlotsToExchange(memorySlots);
    for (const auto& edge : _replicaDataOutputs)   edge->getMemorySlotsToExchange(memorySlots);

    for (const auto& edge : _partitionControlInputs)  edge->getMemorySlotsToExchange(memorySlots);
    for (const auto& edge : _partitionControlOutputs) edge->getMemorySlotsToExchange(memorySlots);
    for (const auto& edge : _replicaControlInputs)    edge->getMemorySlotsToExchange(memorySlots);
    for (const auto& edge : _replicaControlOutputs)   edge->getMemorySlotsToExchange(memorySlots);
  }

  /// This function initializes the workload of the partition coordinator role
  __INLINE__ void initialize()
  {
    _taskr->addTask(&_taskrInitialTask);
  }

  void initialFunction()
  {
       // Get my partition configuration
    const auto& partitionConfiguration = _deployment.getPartitions()[_partitionIdx];

    // Get my partition name
    const auto& partitionName = partitionConfiguration->getName();

    printf("Initializing Partition Coordinator Index %lu - Name: %s - %lu Consumer / %lu Producer edges...\n", _partitionIdx, partitionName.c_str(), _partitionDataInputs.size(), _partitionDataOutputs.size());

    // Sending heartbeat ping messages to all my replicas
    std::string heartbeat = "Heartbeat Ping";
    for (const auto& output : _replicaControlOutputs) output->pushMessage(edge::Message(
       (const uint8_t*) heartbeat.data(),
       heartbeat.length()+1,
       edge::Message::metadata_t { .type = edge::messageType_t::heartbeatPing_t, .messageId = 0, .sessionId = 0 }
      ));

    // Receiving heartbeat pong from the coordinator
    configuration::Replica::replicaIndex_t replicaIdx = 0;
    for (const auto& input : _replicaControlInputs)
    {
      while (input->hasMessage() == false);
      const auto message = input->getMessage();
      printf("[Coordinator %lu] Received heartbeat pong from replica %lu with message: %s\n", _partitionIdx, replicaIdx, message.getData());
      input->popMessage();
      replicaIdx++;
    } 
  }

  typedef std::function<void(taskr::Task *)> function_t;

  private:

  const configuration::Deployment _deployment;
  const configuration::Partition::partitionIndex_t _partitionIdx;
  taskr::Runtime* const _taskr;

  // Data Input / Output edges from other partition coordinators
  std::vector<std::shared_ptr<edge::Input>> _partitionDataInputs;
  std::vector<std::shared_ptr<edge::Output>> _partitionDataOutputs;

  // Data Input / Output edges from my partition's replicas
  std::vector<std::shared_ptr<edge::Input>> _replicaDataInputs;
  std::vector<std::shared_ptr<edge::Output>> _replicaDataOutputs;

  // Control Input / Output edges from other partition coordinators
  std::vector<std::shared_ptr<edge::Input>> _partitionControlInputs;
  std::vector<std::shared_ptr<edge::Output>> _partitionControlOutputs;

  // Control Input / Output edges from my partition's replicas
  std::vector<std::shared_ptr<edge::Input>> _replicaControlInputs;
  std::vector<std::shared_ptr<edge::Output>> _replicaControlOutputs;

  // TaskR functions and tasks belonging to the partition coordinator
  taskr::Function _taskrInitialFc = taskr::Function([this](taskr::Task*){ this->initialFunction(); });
  taskr::Task _taskrInitialTask = taskr::Task(&_taskrInitialFc);

}; // class Coordinator

} // namespace hLLM
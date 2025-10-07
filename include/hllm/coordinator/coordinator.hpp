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

    // Getting list of edges in the deployment
    const auto& edgeConfigs = _deployment.getEdges();

    // Getting list of replicas in the partition
    const auto& replicas = partitionConfiguration->getReplicas();

    // Filling replica set
    for (configuration::Replica::replicaIndex_t replicaIndex = 0; replicaIndex < replicas.size(); replicaIndex++)
    {
      auto newReplica = std::make_unique<Replica>(_partitionIdx, replicaIndex);
      _replicas.push_back(std::move(newReplica));
    }

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
        _partitionDataInputs.push_back(std::make_unique<edge::Input>(*edgeConfig, edge::edgeType_t::coordinatorToCoordinator, edgeIdx, producerPartitionIdx, _partitionIdx, edge::Base::coordinatorReplicaIndex));

        // Create the output edges to pass this distribute the input to any of the replicas
        for (configuration::Replica::replicaIndex_t replicaIdx = 0; replicaIdx < partitionConfiguration->getReplicas().size(); replicaIdx++)
          _replicaDataOutputs.push_back(std::make_unique<edge::Output>(*edgeConfig, edge::edgeType_t::coordinatorToReplica, edgeIdx, _partitionIdx, _partitionIdx, replicaIdx));
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
        _partitionDataOutputs.push_back(std::make_unique<edge::Output>(*edgeConfig, edge::edgeType_t::coordinatorToCoordinator, edgeIdx, _partitionIdx, consumerPartitionIdx, edge::Base::coordinatorReplicaIndex));

        // Create the input edges to receive the output from any of the replicas
        for (configuration::Replica::replicaIndex_t replicaIdx = 0; replicaIdx < partitionConfiguration->getReplicas().size(); replicaIdx++)
          _replicaDataInputs.push_back(std::make_unique<edge::Input>(*edgeConfig, edge::edgeType_t::replicaToCoordinator, edgeIdx, _partitionIdx, _partitionIdx, replicaIdx));
      } 
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
      _replicaControlInputs.push_back(std::make_unique<edge::Input>(replicaControlEdgeConfig, edge::edgeType_t::replicaToCoordinator, edge::Base::controlEdgeIndex, _partitionIdx, _partitionIdx, replicaIdx));
      _replicaControlOutputs.push_back(std::make_unique<edge::Output>(replicaControlEdgeConfig, edge::edgeType_t::coordinatorToReplica, edge::Base::controlEdgeIndex, _partitionIdx, _partitionIdx, replicaIdx));
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
    printf("[Coordinator %lu] Initializing...\n", _partitionIdx);

    // Indicate the coordinator must keep running
    _continueRunning = true;

    // Adding runtime task -- only to keep the coordinator running until shutdown
    _taskr->addTask(&_taskrRuntimeTask);

    //////// Adding service to listen for incoming control messages. 
    // Interval is zero because we don't want any delays in listening for incoming messages
    _taskrControlMessagesListeningService.setInterval(500);
    _taskr->addService(&_taskrControlMessagesListeningService);

    //////// Adding heartbeat service
    _taskrHeartbeatService.setInterval(_deployment.getHeartbeat().interval);

    // Disabling service, if not needed for now
    if (_deployment.getHeartbeat().enabled == false) _taskrHeartbeatService.disable();

    // Adding service to taskr
    _taskr->addService(&_taskrHeartbeatService);
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

  // Heartbeat sending message
  __INLINE__ void heartbeatService()
  {
    // Checking, for all replicas' edges, whether any of them has a pending message
    const auto message = messages::Heartbeat().encode();
    for (const auto& output : _replicaControlOutputs)
    {
      // Getting edge metadata
      const auto replicaIdx = output->getReplicaIndex();

      // If the edge is not full, send a heartbeat
      if (output->isFull(message.getSize()) == false)
      {
        output->pushMessage(message);
      } 
      else // Otherwise, report it's full
      {
        printf("[Coordinator %lu] Replica heartbeat buffer in replica %lu is full!\n", _partitionIdx, replicaIdx);
      }
    } 
  }
  taskr::Service::serviceFc_t _taskrHeartbeatServiceFunction = [this](){ this->heartbeatService(); };
  taskr::Service _taskrHeartbeatService = taskr::Service(_taskrHeartbeatServiceFunction);

  // Initial task
  // It's only function is to keep the coordinator running and finalize when/if deployment is terminated
  __INLINE__ void runtimeTask(taskr::Task* task)
  {
    // Get my partition configuration
    const auto& partitionConfiguration = _deployment.getPartitions()[_partitionIdx];

    // Get my partition name
    const auto& partitionName = partitionConfiguration->getName();

    printf("Initializing Partition Coordinator Index %lu - Name: %s - %lu Consumer / %lu Producer edges...\n", _partitionIdx, partitionName.c_str(), _partitionDataInputs.size(), _partitionDataOutputs.size());

    // Now suspend until the deployment is terminated
    task->addPendingOperation([this](){ return _continueRunning == false; });
    task->suspend();
  }
  taskr::Function _taskrRuntimeTaskFunction = taskr::Function([this](taskr::Task* task){ this->runtimeTask(task); });
  taskr::Task _taskrRuntimeTask = taskr::Task(&_taskrRuntimeTaskFunction);

  // Container for partition replica objects
  std::vector<std::unique_ptr<Replica>> _replicas;

  // Data Input / Output edges from other partition coordinators
  std::vector<std::unique_ptr<edge::Input>> _partitionDataInputs;
  std::vector<std::unique_ptr<edge::Output>> _partitionDataOutputs;

  // Data Input / Output edges from my partition's replicas
  std::vector<std::unique_ptr<edge::Input>> _replicaDataInputs;
  std::vector<std::unique_ptr<edge::Output>> _replicaDataOutputs;

  // Control Input / Output edges from my partition's replicas
  std::vector<std::unique_ptr<edge::Input>> _replicaControlInputs;
  std::vector<std::unique_ptr<edge::Output>> _replicaControlOutputs;

  // Flag indicating whether the execution must keep running
  __volatile__ bool _continueRunning;

}; // class Coordinator

} // namespace hLLM::coordinator
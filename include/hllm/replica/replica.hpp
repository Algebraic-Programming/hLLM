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
    const auto& controlBufferConfig = _deployment.getControlBufferConst();
    auto replicaControlEdgeConfig = configuration::Edge("Control Edge", partitionName, partitionName, "Copy", controlBufferConfig.capacity, controlBufferConfig.size);
    replicaControlEdgeConfig.setCoordinationCommunicationManager(controlBufferConfig.communicationManager);
    replicaControlEdgeConfig.setCoordinationMemoryManager(controlBufferConfig.memoryManager);
    replicaControlEdgeConfig.setCoordinationMemorySpace(controlBufferConfig.memorySpace);
    replicaControlEdgeConfig.setPayloadCommunicationManager(controlBufferConfig.communicationManager);
    replicaControlEdgeConfig.setPayloadMemoryManager(controlBufferConfig.memoryManager);
    replicaControlEdgeConfig.setPayloadMemorySpace(controlBufferConfig.memorySpace);
    _coordinatorControlInput = std::make_shared<edge::Input>(replicaControlEdgeConfig, edge::edgeType_t::coordinatorToReplica, edge::Base::controlEdgeIndex, _partitionIdx, _partitionIdx, _replicaIdx);
    _coordinatorControlOutput = std::make_shared<edge::Output>(replicaControlEdgeConfig, edge::edgeType_t::replicaToCoordinator, edge::Base::controlEdgeIndex, _partitionIdx, _partitionIdx, _replicaIdx);
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

      // Indicate the coordinator must keep running
    _continueRunning = true;

    // Adding runtime task -- only to keep the replica running until shutdown
    _taskr->addTask(&_taskrRuntimeTask);

    // Adding service to listen for incoming control messages
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
    // printf("[Replica %lu/%lu] Checking for messages...\n", _partitionIdx, _replicaIdx);

    // Checking, for all replicas' edges, whether any of them has a pending message
    if (_coordinatorControlInput->hasMessage())
    {
      // Getting message from input edge
      const auto message = _coordinatorControlInput->getMessage();
      const auto messageType = message.getMetadata().type;

      // Getting edge metadata
      const auto replicaIdx = _coordinatorControlInput->getReplicaIndex();

      // Deciding what to do based on message type
      bool isTypeRecognized = false;
      if (messageType == hLLM::messages::messageTypes::heartbeat)
      {
        printf("[Replica %lu / %lu] Received heartbeat from coordinator with message: %s\n", _partitionIdx, _replicaIdx, message.getData());
        isTypeRecognized = true;
      }

      if (isTypeRecognized == false) HICR_THROW_RUNTIME("[Replica %lu / %lu] Received unrecognized message type %lu from coordinator\n", _partitionIdx, replicaIdx, messageType);

      // Immediately disposing (popping) of message out of the edge
      _coordinatorControlInput->popMessage();
    } 
  }
  taskr::Service::serviceFc_t _taskrControlMessagesListeningServiceFunction = [this](){ this->controlMessagesListeningService(); };
  taskr::Service _taskrControlMessagesListeningService = taskr::Service(_taskrControlMessagesListeningServiceFunction, 0);

  // Heartbeat sending message
  __INLINE__ void heartbeatService()
  {
    // Checking, for all replicas' edges, whether any of them has a pending message
    const auto message = messages::Heartbeat().encode();
    if (_coordinatorControlOutput->isFull(message.getSize()) == false)
    {
      _coordinatorControlOutput->pushMessage(message);
    } 
    else // Otherwise, report it's full
    {
      printf("[Replica %lu / %lu] Coordinator heartbeat buffer is full!\n", _partitionIdx, _replicaIdx);
    }
  }
  taskr::Service::serviceFc_t _taskrHeartbeatServiceFunction = [this](){ this->heartbeatService(); };
  taskr::Service _taskrHeartbeatService = taskr::Service(_taskrHeartbeatServiceFunction);

  /////////// Tasks

  // Initial task. It's only function is to keep the replica running and finalize when/if deployment is terminated
  __INLINE__ void runtimeTask(taskr::Task* task)
  {
    // Get my partition configuration
    const auto& partitionConfiguration = _deployment.getPartitions()[_partitionIdx];

    // Get my replica configuration
    const auto& replicaConfiguration = partitionConfiguration->getReplicas()[_replicaIdx];

    // Printing debug message
    printf("Initializing Replica Index P%lu/R%lu - Name: %s - %lu Consumer / %lu Producer edges...\n", _partitionIdx, _replicaIdx, replicaConfiguration->getName().c_str(), _coordinatorDataInputs.size(), _coordinatorDataOutputs.size());

    // Now suspend until the deployment is terminated
    task->addPendingOperation([this](){ return _continueRunning == false; });
    task->suspend();
  }
  taskr::Function _taskrRuntimeTaskFunction = taskr::Function([this](taskr::Task* task){ this->runtimeTask(task); });
  taskr::Task _taskrRuntimeTask = taskr::Task(&_taskrRuntimeTaskFunction);

  // Identifier for the replica index
  const configuration::Replica::replicaIndex_t _replicaIdx;

  // Data Input/Output edges from/to the coordinator
  std::vector<std::shared_ptr<edge::Input>> _coordinatorDataInputs;
  std::vector<std::shared_ptr<edge::Output>> _coordinatorDataOutputs;

  // Control Input/Output edges from/to the coordinator
  std::shared_ptr<edge::Input> _coordinatorControlInput;
  std::shared_ptr<edge::Output> _coordinatorControlOutput;

  // Flag indicating whether the execution must keep running
  __volatile__ bool _continueRunning;

}; // class Replica

} // namespace hLLM::replica
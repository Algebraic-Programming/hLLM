#pragma once

#include <memory>
#include <vector>
#include <hicr/core/exceptions.hpp>
#include <hicr/core/definitions.hpp>
#include <taskr/taskr.hpp>
#include "configuration/deployment.hpp"
#include "edge/input.hpp"
#include "edge/output.hpp"
#include "messages/base.hpp"
#include "messages/heartbeat.hpp"
#include "messages/prompt.hpp"
#include "prompt.hpp"
#include "partition.hpp"

namespace hLLM
{

class Coordinator final : public hLLM::Partition
{
  private:

  /** 
   * This is the definition of a replica from the standpoint of the coordinator.
   * 
   * It holds all the necessary information to communicate and distribute work to the associated replica
   */
  class Replica final
  {
    public:

    Replica() = delete;
    Replica(
      const configuration::Partition::partitionIndex_t partitionIndex,
      const configuration::Replica::replicaIndex_t replicaIndex,
      const std::vector<hLLM::edge::edgeInfo_t>& coordinatorInputs,
      const std::vector<hLLM::edge::edgeInfo_t>& coordinatorOutputs,
      const std::shared_ptr<hLLM::configuration::Edge> controlEdgeConfig) :
      _partitionIndex(partitionIndex),
      _replicaIndex(replicaIndex)
      {
        // For every one of the coordinator inputs, we create an output edge that allows us to redirect such inputs to the replica
        for (const auto& edge : coordinatorInputs)
        {
          const auto edgeIdx = edge.index;
          const auto& edgeConfig = edge.config;
            
          // Create the output edges to pass this distribute the input to any of the replicas
          auto newOutput = std::make_shared<edge::Output>(*edgeConfig, edge::edgeType_t::coordinatorToReplica, edgeIdx, _partitionIndex, _partitionIndex, _replicaIndex);
          _dataOutputs.push_back(newOutput);
        }

        // For everyone of the coordinator outputs, we create an input edge that receives such data from the replica to be redirected to a peer coordinator
        for (const auto& edge : coordinatorOutputs)
        {
          const auto edgeIdx = edge.index;
          const auto& edgeConfig = edge.config;
            
          // Create the input edges to receive the replica 
          auto newInput = std::make_shared<edge::Input>(*edgeConfig, edge::edgeType_t::replicaToCoordinator, edgeIdx, _partitionIndex, _partitionIndex, _replicaIndex);
          _dataInputs.push_back(newInput);
        }

        // Creating control edges for exchanging control operations (e.g., heartbeat, etc)
        _controlInput = std::make_shared<edge::Input>(*controlEdgeConfig, edge::edgeType_t::replicaToCoordinator, edge::Base::controlEdgeIndex, _partitionIndex, _partitionIndex, _replicaIndex);
        _controlOutput = std::make_shared<edge::Output>(*controlEdgeConfig, edge::edgeType_t::coordinatorToReplica, edge::Base::controlEdgeIndex, _partitionIndex, _partitionIndex, _replicaIndex);
      }

    ~Replica() = default;

    __INLINE__ void addDataInputEdge(std::shared_ptr<edge::Input> edge) { _dataInputs.push_back(edge); }
    __INLINE__ void addDataOutputEdge(std::shared_ptr<edge::Output> edge) { _dataOutputs.push_back(edge); }
    __INLINE__ const auto& getDataInputs() const { return _dataInputs; }
    __INLINE__ const auto& getDataOutputs() const { return _dataOutputs; }
    __INLINE__ const auto& getControlInput() const { return _controlInput; }
    __INLINE__ const auto& getControlOutput() const { return _controlOutput; }

    __INLINE__ void getMemorySlotsToExchange(std::vector<hLLM::edge::memorySlotExchangeInfo_t>& memorySlots)
    {
      for (const auto& edge : _dataInputs)  edge->getMemorySlotsToExchange(memorySlots);
      for (const auto& edge : _dataOutputs) edge->getMemorySlotsToExchange(memorySlots);
      _controlInput->getMemorySlotsToExchange(memorySlots);
      _controlOutput->getMemorySlotsToExchange(memorySlots);
    }

    __INLINE__ void initializeEdges(const HiCR::GlobalMemorySlot::tag_t tag)
    {
      for (const auto& edge : _dataInputs)    edge->initialize(tag);
      for (const auto& edge : _dataOutputs)   edge->initialize(tag);
      _controlInput->initialize(tag);
      _controlOutput->initialize(tag);
    }

    private:

    const configuration::Partition::partitionIndex_t _partitionIndex; 
    const configuration::Replica::replicaIndex_t _replicaIndex;

    // Data Input / Output edges to/from this coordinator<->replica
    std::vector<std::shared_ptr<edge::Input>> _dataInputs;
    std::vector<std::shared_ptr<edge::Output>> _dataOutputs;

    // Control Input / Output edges to/from this coordinator<->replica
    std::shared_ptr<edge::Input> _controlInput;
    std::shared_ptr<edge::Output> _controlOutput;

  }; // class Replica

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
      // Creating new replica object, to represent an actual replica we can communicate to/from
      auto newReplica = std::make_shared<Replica>(_partitionIdx, replicaIndex, _inputEdges, _outputEdges, _controlEdgeConfig);

      // Adding new replica
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
    } 
  }

  // Gets the memory slots required by the edges
  __INLINE__ void getMemorySlotsToExchange(std::vector<hLLM::edge::memorySlotExchangeInfo_t>& memorySlots)
  {
    for (const auto& edge : _partitionDataInputs)  edge->getMemorySlotsToExchange(memorySlots);
    for (const auto& edge : _partitionDataOutputs) edge->getMemorySlotsToExchange(memorySlots);
    for (const auto& replica : _replicas) replica->getMemorySlotsToExchange(memorySlots);
  }

  /// This function completes the initialization of the edges, after the memory slot exchanges are completed
  __INLINE__ void initializeEdges(const HiCR::GlobalMemorySlot::tag_t tag)
  {
    for (const auto& edge : _partitionDataInputs)  edge->initialize(tag);
    for (const auto& edge : _partitionDataOutputs) edge->initialize(tag);
    for (const auto& replica : _replicas) replica->initializeEdges(tag);
  }

  __INLINE__ void pushPrompt(const std::shared_ptr<hLLM::messages::Prompt> prompt)
  {
    // Finding the corresponding prompt edge
    const auto& promptEdge = _partitionDataOutputs[_edgeIndexToVectorPositionMap[_promptEdgeIdx]];

    // Encoding raw message
    const auto rawMessage = prompt->encode();

    // printf("Pushing Prompt A\n");
    // Waiting until it is freed
    while (promptEdge->isFull(rawMessage.getSize()) == true);
    // printf("Pushing Prompt B\n");

    // Pushing prompt to the corresponding edge
    promptEdge->pushMessage(rawMessage);
  }

  private:

  /// This function subscribes the handlers and services for the coordinator role
  __INLINE__ void initializeImpl() override
  {
    // Get my partition configuration
    const auto& partitionConfiguration = _deployment.getPartitions()[_partitionIdx];

    // Get my partition name
    const auto& partitionName = partitionConfiguration->getName();

    // Welcome message
    printf("Initializing Partition Coordinator Index %lu - Name: %s - %lu Consumer / %lu Producer edges...\n", _partitionIdx, partitionName.c_str(), _partitionDataInputs.size(), _partitionDataOutputs.size());

    // Subscribing to the heartbeat sending service for my replicas
    for (const auto& replica : _replicas) subscribeHeartbeatEdge(replica->getControlOutput());

    // Subscribing input edges to the control message service for my replicas
    for (const auto& replica : _replicas) subscribeMessageEdge(replica->getControlInput());

    // Subscribing input edges to the control message service for the peer coordinators who provide data inputs to us
    for (const auto& dataEdge : _partitionDataInputs) subscribeMessageEdge(dataEdge);

    // Registering a handler for the handshake message 
    subscribeMessageHandler(hLLM::messages::messageTypes::heartbeat, [this](const std::shared_ptr<edge::Input> edge, const hLLM::edge::Message& message){ heartbeatMessageHandler(edge, std::make_shared<hLLM::messages::Heartbeat>(message)); });

    // Registering a handler for the prompt message 
    subscribeMessageHandler(hLLM::messages::messageTypes::prompt, [this](const std::shared_ptr<edge::Input> edge, const hLLM::edge::Message& message){ promptMessageHandler(edge, std::make_shared<hLLM::messages::Prompt>(message)); });

    // If this is the prompt management partition, then set up a service for its handling
    if (isPromptPartition())  _taskr->addService(&_taskrPromptHandlingService);
  }

  __INLINE__ void promptMessageHandler(const std::shared_ptr<edge::Input> edge, const std::shared_ptr<hLLM::messages::Prompt> message)
  {
    // Sanity check
    if (isPromptPartition() == false) HICR_THROW_LOGIC("Sent a prompt to the coordinator of partition %lu, but this is not a prompt partition. This must be a bug in hLLM", _partitionIdx);

    // Creating new prompt object
    const auto input = message->getInput();
    const auto sessionId = message->getSessionId();
    const auto messageId = message->getMessageId();
    const auto promptId = Prompt::promptId_t(sessionId, messageId);
    printf("[Coordinator %lu] Received prompt '%s' from session %lu, message: %lu\n", _partitionIdx, input.c_str(), sessionId, messageId);

    // Creating new prompt
    auto prompt = std::make_shared<Prompt>(promptId, input);

    // Adding prompt to new prompt queue
    _promptManagementMutex.lock();
    _pendingNewPromptsQueue.push(prompt);
    _promptManagementMutex.unlock();
  }
  
  __INLINE__ void heartbeatMessageHandler(const std::shared_ptr<edge::Input> edge, const std::shared_ptr<hLLM::messages::Heartbeat> message)
  {
    const auto replicaIdx = edge->getReplicaIndex();
    if(_deployment.getHeartbeat().visible == true) printf("[Coordinator %lu] Received heartbeat from replica %lu.\n", _partitionIdx, replicaIdx);
  }

  ///////////// Prompt handling service
  __INLINE__ void promptHandlingService()
  {
    // Checking for new prompts to be added to the list
    _promptManagementMutex.lock();

    // Accepting incoming session connection requests
    while(_pendingNewPromptsQueue.empty() == false)
    {
      // Getting next pending session to connect
      const auto prompt = _pendingNewPromptsQueue.front();
      const auto promptId = prompt->getPromptId();
      
      // Registering session
      _activePromptMap.insert({promptId, prompt});
      printf("Added Prompt id: %lu/%lu\n", promptId.first, promptId.second);

      // Freeing entry in the pending session connection queue
      _pendingNewPromptsQueue.pop();
    }

    _promptManagementMutex.unlock();
  }
  taskr::Service::serviceFc_t _promptHandlingServiceFunction = [this](){ this->promptHandlingService(); };
  taskr::Service _taskrPromptHandlingService = taskr::Service(_promptHandlingServiceFunction);

  // Container for partition replica objects
  std::vector<std::shared_ptr<Replica>> _replicas;

  // Data Input / Output edges from other partition coordinators
  std::vector<std::shared_ptr<edge::Input>> _partitionDataInputs;
  std::vector<std::shared_ptr<edge::Output>> _partitionDataOutputs;

  // Prompt management mutex
  std::mutex _promptManagementMutex;

  // Queue of pending new prompts
  std::queue<std::shared_ptr<Prompt>> _pendingNewPromptsQueue;

  // Active prompt map
  std::map<Prompt::promptId_t, std::shared_ptr<Prompt>> _activePromptMap;

}; // class Coordinator

} // namespace hLLM::coordinator
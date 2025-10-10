#pragma once

#include <memory>
#include <vector>
#include <hicr/core/exceptions.hpp>
#include <hicr/core/definitions.hpp>
#include <hicr/core/globalMemorySlot.hpp>
#include <taskr/taskr.hpp>
#include "edge/base.hpp"
#include "edge/output.hpp"
#include "configuration/deployment.hpp"

namespace hLLM
{

class Partition
{
  public:

  #define __HLLM_PARTITION_DEFAULT_DATA_BUFFER_CAPACITY 1

  Partition() = delete;
  ~Partition() = default;

  Partition(
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

    // Creating control edge configuration object
    const auto& controlBufferConfig = _deployment.getControlBufferConst();
    _controlEdgeConfig = std::make_shared<configuration::Edge>("Control Edge", controlBufferConfig.capacity, controlBufferConfig.size);
    _controlEdgeConfig->setCoordinationCommunicationManager(controlBufferConfig.communicationManager);
    _controlEdgeConfig->setCoordinationMemoryManager(controlBufferConfig.memoryManager);
    _controlEdgeConfig->setCoordinationMemorySpace(controlBufferConfig.memorySpace);
    _controlEdgeConfig->setPayloadCommunicationManager(controlBufferConfig.communicationManager);
    _controlEdgeConfig->setPayloadMemoryManager(controlBufferConfig.memoryManager);
    _controlEdgeConfig->setPayloadMemorySpace(controlBufferConfig.memorySpace);

    // Iterating through edges by their index and creating them
    size_t inputEdgeVectorPosition = 0;
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

        // Adding new entry
        _inputEdges.push_back( edge::edgeInfo_t { .index = edgeIdx, .config = edgeConfig, .producerPartitionIndex = producerPartitionIdx, .consumerPartitionIndex = _partitionIdx });

        // Adding map entry to link the edge index to its position
        _edgeIndexToVectorPositionMap[edgeIdx] = inputEdgeVectorPosition;

        // Increasing position
        inputEdgeVectorPosition++;
      } 

      // If I am a producer in this edge
      size_t outputEdgeVectorPosition = 0;
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

        // Adding new entry
        _outputEdges.push_back( edge::edgeInfo_t { .index = edgeIdx, .config = edgeConfig, .producerPartitionIndex = _partitionIdx, .consumerPartitionIndex = consumerPartitionIdx });

        // Adding map entry to link the edge index to its position
        _edgeIndexToVectorPositionMap[edgeIdx] = outputEdgeVectorPosition;

        // Increasing position
        outputEdgeVectorPosition++;
      } 
    }
  }

  __INLINE__ void initialize()
  {
    // Running role-specific implementation
    initializeImpl();

    //////// Adding heartbeat service
    _taskrHeartbeatService.setInterval(_deployment.getHeartbeat().interval);

    // Adding service to taskr for heartbeat detection
    _taskr->addService(&_taskrHeartbeatService);

    // Disabling heartbeat service, if not needed for now
    if (_deployment.getHeartbeat().enabled == false) _taskrHeartbeatService.disable();

    ////////// Adding service to listen for incoming messages
    _taskr->addService(&_taskrMessagesListeningService);

    ////////// Adding runtime task -- only to keep the engine running until shutdown
    _taskr->addTask(&_taskrRuntimeTask);

    ////////// Set to continue running until the deployment is stopped
    _continueRunning = true;
  }

  protected: 

  /// This function completes the initialization of the edges, after the memory slot exchanges are completed
  virtual void initializeEdges(const HiCR::GlobalMemorySlot::tag_t tag) = 0;
  virtual void getMemorySlotsToExchange(std::vector<hLLM::edge::memorySlotExchangeInfo_t>& memorySlots) = 0;
  virtual void initializeImpl() = 0;

  const configuration::Deployment _deployment;
  const configuration::Partition::partitionIndex_t _partitionIdx;
  taskr::Runtime* const _taskr;

  // Container for input edges for this partition
  std::vector<edge::edgeInfo_t> _inputEdges;

  // Container for output edges for this partition
  std::vector<edge::edgeInfo_t> _outputEdges;

  // This map links an edge index (as defined in the deployment configuration) to its position in the input/output vectors
  std::map<hLLM::configuration::Edge::edgeIndex_t, size_t> _edgeIndexToVectorPositionMap;

  // Flag indicating whether the execution must keep running
  __volatile__ bool _continueRunning;

  //  Configuration for control buffer edges
  std::shared_ptr<configuration::Edge> _controlEdgeConfig;

  // Function to subscribe an edge for the heartbeat service
  __INLINE__ void subscribeHeartbeatEdge(const std::shared_ptr<edge::Output> edge) { _heartbeatOutputEdges.push_back(edge); }

  // Function to subscribe a  message handler
  typedef std::function<void(const std::shared_ptr<edge::Input>, const hLLM::messages::Base*)> messageHandler_t;
  __INLINE__ void subscribeMessageHandler(const hLLM::edge::Message::messageType_t type, const messageHandler_t handler) { _messageHandlers[type] = handler; }
  __INLINE__ void subscribeMessageEdge(const std::shared_ptr<edge::Input> edge) { _inputEdgesForHandler.push_back(edge); }
  
  private:

  ////////// Runtime task
  // Its only role is to keep the engine running and finalize when/if deployment is terminated
  __INLINE__ void runtimeTask(taskr::Task* task)
  {
    // Now suspend until the deployment is terminated
    task->addPendingOperation([this](){ return _continueRunning == false; });
    task->suspend();
  }
  taskr::Function _taskrRuntimeTaskFunction = taskr::Function([this](taskr::Task* task){ this->runtimeTask(task); });
  taskr::Task _taskrRuntimeTask = taskr::Task(&_taskrRuntimeTaskFunction);

  ///////////// Heartbeat sending service
  __INLINE__ void heartbeatService()
  {
    // Checking, for all replicas' edges, whether any of them has a pending message
    const auto message = messages::Heartbeat().encode();
    for (const auto& edge : _heartbeatOutputEdges)
    {
      // Getting edge metadata
      const auto replicaIdx = edge->getReplicaIndex();

      // If the edge is not full, send a heartbeat
      if (edge->isFull(message.getSize()) == false)
      {
        edge->pushMessage(message);
      } 
      else // Otherwise, report it's full
      {
        printf("[Warning] Heartbeat buffer for %lu / %lu is full!\n", _partitionIdx, replicaIdx);
      }
    } 
  }
  taskr::Service::serviceFc_t _taskrHeartbeatServiceFunction = [this](){ this->heartbeatService(); };
  taskr::Service _taskrHeartbeatService = taskr::Service(_taskrHeartbeatServiceFunction);
  std::vector<std::shared_ptr<edge::Output>> _heartbeatOutputEdges;

  /////////// Message Hanlding Service
  __INLINE__ void messagesListeningService()
  {
    // Checking for all input edges
    for (const auto& edge : _inputEdgesForHandler)
    if (edge->hasMessage())
    {
      // Getting message from input edge
      const auto message = edge->getMessage();
      const auto messageType = message.getMetadata().type;
      const auto replicaIdx = edge->getReplicaIndex();

      // Checking whether the message type is subscribed to
      if (_messageHandlers.contains(messageType) == false) HICR_THROW_RUNTIME("[Partition %lu / %lu] Received message type %lu that has no subscribed handler\n", _partitionIdx, replicaIdx, messageType);

      // Decoding message based on type
      hLLM::messages::Base* decodedMessage;
      bool isRecognized = false;

      if (messageType == hLLM::messages::messageTypes::heartbeat)
      {
        isRecognized = true;
        decodedMessage = new hLLM::messages::Heartbeat(message);
      }

      // Checking whether the message type is even recognized
      if (isRecognized == false) HICR_THROW_RUNTIME("[Partition %lu / %lu] Received message type %lu that is not recognized. This must be a bug in hLLM\n", _partitionIdx, replicaIdx, messageType);

      // Now calling the handler
      _messageHandlers[messageType](edge, decodedMessage);

      // Immediately disposing (popping) of message out of the edge
      edge->popMessage();
    } 
  }
  taskr::Service::serviceFc_t _taskrMessagesListeningServiceFunction = [this](){ this->messagesListeningService(); };
  taskr::Service _taskrMessagesListeningService = taskr::Service(_taskrMessagesListeningServiceFunction, 0);
  std::vector<std::shared_ptr<edge::Input>> _inputEdgesForHandler;
  std::map<hLLM::edge::Message::messageType_t, messageHandler_t> _messageHandlers;

}; // class Partition

} // namespace hLLM
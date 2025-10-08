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

  struct edgeInfo_t
  {
    configuration::Edge::edgeIndex_t index;
    std::shared_ptr<hLLM::configuration::Edge> config;
    configuration::Partition::partitionIndex_t producerPartitionIndex;
    configuration::Partition::partitionIndex_t consumerPartitionIndex;
  };

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

        // Adding new entry
        _inputEdges.push_back( edgeInfo_t { .index = edgeIdx, .config = edgeConfig, .producerPartitionIndex = producerPartitionIdx, .consumerPartitionIndex = _partitionIdx });
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

        // Adding new entry
        _outputEdges.push_back( edgeInfo_t { .index = edgeIdx, .config = edgeConfig, .producerPartitionIndex = _partitionIdx, .consumerPartitionIndex = consumerPartitionIdx });
      } 
    }
  }

  void initialize()
  {
    // Running role-specific implementation
    initializeImpl();

    //////// Adding heartbeat service
    _taskrHeartbeatService.setInterval(_deployment.getHeartbeat().interval);

    // Adding service to taskr for heartbeat detection
    _taskr->addService(&_taskrHeartbeatService);

    // Disabling heartbeat service, if not needed for now
    if (_deployment.getHeartbeat().enabled == false) _taskrHeartbeatService.disable();

    ////////// Adding service to listen for incoming control messages
    _taskr->addService(&_taskrControlMessagesListeningService);

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
  std::vector<edgeInfo_t> _inputEdges;

  // Container for output edges for this partition
  std::vector<edgeInfo_t> _outputEdges;

  // Flag indicating whether the execution must keep running
  __volatile__ bool _continueRunning;

  // Function to subscribe an edge for the heartbeat service
  __INLINE__ void subscribeHeartbeatEdge(const std::shared_ptr<edge::Output> edge) { _heartbeatOutputEdges.push_back(edge); }

  // Function to subscribe a control message handler
  typedef std::function<void(const std::shared_ptr<edge::Input>, const hLLM::messages::Base*)> controlMessageHandler_t;
  __INLINE__ void subscribeControlMessageHandler(const hLLM::edge::Message::messageType_t type, const controlMessageHandler_t handler) { _controlMessageHandlers[type] = handler; }
  __INLINE__ void subscribeControlMessageEdge(const std::shared_ptr<edge::Input> edge) { _controlInputEdges.push_back(edge); }
  
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

  /////////// Control Message Hanlding Service
  // Control Message-listening service
  __INLINE__ void controlMessagesListeningService()
  {
    // Checking for all control input edges
    for (const auto& edge : _controlInputEdges)
    if (edge->hasMessage())
    {
      // Getting message from input edge
      const auto message = edge->getMessage();
      const auto messageType = message.getMetadata().type;
      const auto replicaIdx = edge->getReplicaIndex();

      // Checking whether the message type is subscribed to
      if (_controlMessageHandlers.contains(messageType) == false) HICR_THROW_RUNTIME("[Partition %lu / %lu] Received control message type %lu that has no subscribed handler\n", _partitionIdx, replicaIdx, messageType);

      // Decoding message based on type
      hLLM::messages::Base* decodedMessage;
      bool isRecognized = false;

      if (messageType == hLLM::messages::messageTypes::heartbeat)
      {
        isRecognized = true;
        decodedMessage = new hLLM::messages::Heartbeat(message);
      }

      // Checking whether the message type is even recognized
      if (isRecognized == false) HICR_THROW_RUNTIME("[Partition %lu / %lu] Received control message type %lu that is not recognized. This must be a bug in hLLM\n", _partitionIdx, replicaIdx, messageType);

      // Now calling the handler
      _controlMessageHandlers[messageType](edge, decodedMessage);

      // Immediately disposing (popping) of message out of the edge
      edge->popMessage();
    } 
  }
  taskr::Service::serviceFc_t _taskrControlMessagesListeningServiceFunction = [this](){ this->controlMessagesListeningService(); };
  taskr::Service _taskrControlMessagesListeningService = taskr::Service(_taskrControlMessagesListeningServiceFunction, 0);
  std::vector<std::shared_ptr<edge::Input>> _controlInputEdges;
  std::map<hLLM::edge::Message::messageType_t, controlMessageHandler_t> _controlMessageHandlers;

}; // class Coordinator

} // namespace hLLM::coordinator
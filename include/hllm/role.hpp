#pragma once

#include <memory>
#include <vector>
#include <hicr/core/exceptions.hpp>
#include <hicr/core/definitions.hpp>
#include <taskr/taskr.hpp>
#include "configuration/deployment.hpp"
#include "configuration/edge.hpp"

namespace hLLM
{

class Role
{
  public:

  Role() = delete;
  ~Role() = default;

  typedef std::function<void(const std::shared_ptr<edge::Input>, const hLLM::edge::Message&)> messageHandler_t;
  struct edgeHandlerSubscription_t
  {
    hLLM::edge::Message::messageType_t type;
    std::shared_ptr<edge::Input> edge;
    messageHandler_t handler;
  };

  Role(
    const configuration::Deployment deployment,
    taskr::Runtime* const taskr
  ) :
    _deployment(deployment),
    _taskr(taskr)
  {
    // Creating control edge configuration object
    const auto& controlBufferConfig = _deployment.getControlBufferConst();
    _controlEdgeConfig = std::make_shared<configuration::Edge>("Control Edge", controlBufferConfig.capacity, controlBufferConfig.size);
    _controlEdgeConfig->setCoordinationCommunicationManager(controlBufferConfig.communicationManager);
    _controlEdgeConfig->setCoordinationMemoryManager(controlBufferConfig.memoryManager);
    _controlEdgeConfig->setCoordinationMemorySpace(controlBufferConfig.memorySpace);
    _controlEdgeConfig->setPayloadCommunicationManager(controlBufferConfig.communicationManager);
    _controlEdgeConfig->setPayloadMemoryManager(controlBufferConfig.memoryManager);
    _controlEdgeConfig->setPayloadMemorySpace(controlBufferConfig.memorySpace);
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

    ////////// Adding service to listen for incoming messages in edge-handler subscriptions
    _taskr->addService(&_taskrEdgeSubscriptionListeningService);

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
  taskr::Runtime* const _taskr;

  // Flag indicating whether the execution must keep running
  __volatile__ bool _continueRunning;

  //  Configuration for control buffer edges
  std::shared_ptr<configuration::Edge> _controlEdgeConfig;

  // Function to subscribe an edge for the heartbeat service
  __INLINE__ void subscribeHeartbeatEdge(const std::shared_ptr<edge::Output> edge) { _heartbeatOutputEdges.push_back(edge); }

  // Function to subscribe a  message handler
  __INLINE__ void subscribeEdgeMessageHandler(const edgeHandlerSubscription_t subscription) { _edgeHandlerSubscriptions.push_back(subscription); }
  
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
      // If the edge is not full, send a heartbeat
      if (edge->isFull(message.getSize()) == false)
      {
        edge->pushMessage(message);
      } 
      else // Otherwise, report it's full
      {
        printf("[Warning] Heartbeat buffer is full!\n");
      }
    } 
  }
  taskr::Service::serviceFc_t _taskrHeartbeatServiceFunction = [this](){ this->heartbeatService(); };
  taskr::Service _taskrHeartbeatService = taskr::Service(_taskrHeartbeatServiceFunction);
  std::vector<std::shared_ptr<edge::Output>> _heartbeatOutputEdges;

  /////////// Message Hanlding Service
  __INLINE__ void edgeSubscriptionListeningService()
  {
    // Checks if a given edge has a pending message that was not handled by any subscription
    std::set<configuration::Edge::edgeIndex_t> _pendingEdges;

    // Checking for all input edges
    for (const auto& subscription : _edgeHandlerSubscriptions)
    {
      const auto type = subscription.type;
      const auto edge = subscription.edge;
      const auto edgeIdx = edge->getEdgeIndex();
      const auto& handler = subscription.handler;

      if (edge->hasMessage())
      {
        // Getting message from input edge
        const auto message = edge->getMessage();
        const auto messageType = message.getMetadata().type;

        // If the message type coincides with that of the subscription, then run and pop. Otherwise pass
        if (messageType == type)
        {
          // Now calling the handler
          handler(edge, message);

          // Immediately disposing (popping) of message out of the edge
          edge->popMessage();

          // Removing edge from the set of pending edges, if set
          if (_pendingEdges.contains(edgeIdx)) _pendingEdges.erase(edgeIdx);
        }
        else
        {
          // Add edge to the set of pending edges
          _pendingEdges.insert(edge->getEdgeIndex());
        }
      } 
    }

    // Checking if there were any unhandled edges
    if (_pendingEdges.empty() == false) HICR_THROW_RUNTIME("Some edges (%lu) cointained messages that were unaccounted for.\n", _pendingEdges.size());
  }
  taskr::Service::serviceFc_t _taskrEdgeSubscriptionListeningServiceFunction = [this](){ this->edgeSubscriptionListeningService(); };
  taskr::Service _taskrEdgeSubscriptionListeningService = taskr::Service(_taskrEdgeSubscriptionListeningServiceFunction, 0);
  std::vector<edgeHandlerSubscription_t> _edgeHandlerSubscriptions;

}; // class Role

} // namespace hLLM
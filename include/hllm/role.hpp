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

    ////////// Set to continue running until the deployment is stopped
    _continueRunning = true;
  }

  /// This function completes the initialization of the edges, after the memory slot exchanges are completed
  virtual void initializeEdges(const HiCR::GlobalMemorySlot::tag_t tag) = 0;
  virtual void getMemorySlotsToExchange(std::vector<hLLM::edge::memorySlotExchangeInfo_t>& memorySlots) = 0;

  protected: 

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
  __INLINE__ void subscribeEdgeMessageHandler(const edgeHandlerSubscription_t subscription)
  {
     _subscribedEdges.insert(subscription.edge);
     _subscriptionToHandlerMap.insert( { { subscription.edge->getEdgeIndex(), subscription.type }, subscription.handler } );
  }
  
  private:

  ///////////// Heartbeat sending service
  __INLINE__ void heartbeatService()
  {
    // Checking, for all replicas' edges, whether any of them has a pending message
    const auto message = messages::Heartbeat();
    const auto rawMessage = message.encode();
    for (const auto& edge : _heartbeatOutputEdges) edge->pushMessageLocking(rawMessage);
  }
  taskr::Service::serviceFc_t _taskrHeartbeatServiceFunction = [this](){ this->heartbeatService(); };
  taskr::Service _taskrHeartbeatService = taskr::Service(_taskrHeartbeatServiceFunction);
  std::vector<std::shared_ptr<edge::Output>> _heartbeatOutputEdges;

  /////////// Message Hanlding Service
  __INLINE__ void edgeSubscriptionListeningService()
  {
    for (const auto& edge : _subscribedEdges)
    { 
      // Locking thread from concurrent access
      edge->lock();

      if (edge->hasMessage())
      {
          // Getting message from input edge
          const auto edgeIdx = edge->getEdgeIndex();
          const auto message = edge->getMessage();
          const auto messageType = message.getMetadata().type;
          const auto subscriptionMapKey = std::make_pair(edgeIdx, messageType);
          
          // Checking if a subscription has been registered for that edge
          if (_subscriptionToHandlerMap.contains(subscriptionMapKey) == false) HICR_THROW_RUNTIME("Edge Idx %lu cointains message of type %lu that has no subscribed handler.\n", edgeIdx, messageType);

          // If it is registered, get handler
          const auto& handler = _subscriptionToHandlerMap.at(subscriptionMapKey);

          // Running handler
          handler(edge, message);
      
          // Immediately disposing (popping) of message out of the edge
          edge->popMessage();
      }

      // Unlocking edge
      edge->unlock();
    }
  }
  taskr::Service::serviceFc_t _taskrEdgeSubscriptionListeningServiceFunction = [this](){ this->edgeSubscriptionListeningService(); };
  taskr::Service _taskrEdgeSubscriptionListeningService = taskr::Service(_taskrEdgeSubscriptionListeningServiceFunction, 0);
  std::set<std::shared_ptr<edge::Input>> _subscribedEdges;
  std::map<std::pair<hLLM::configuration::Edge::edgeIndex_t, hLLM::edge::Message::messageType_t>, messageHandler_t> _subscriptionToHandlerMap;

}; // class Role

} // namespace hLLM
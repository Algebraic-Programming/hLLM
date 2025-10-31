#pragma once

#include <memory>
#include <vector>
#include <chrono>

#include <hicr/core/exceptions.hpp>
#include <hicr/core/definitions.hpp>
#include <hicr/core/globalMemorySlot.hpp>
#include <taskr/taskr.hpp>
#include "../role.hpp"
#include "../edge/base.hpp"
#include "../edge/output.hpp"
#include "../configuration/deployment.hpp"
#include "../messages/data.hpp"
#include "../session.hpp"
#include "../realTimeAnalysis.hpp"
#include "../../../extern/cpp-httplib/httplib.h"

using Clock = std::chrono::steady_clock;  // Monotonic clock for precise timing
using Secs  = std::chrono::seconds;       // Convenience alias for seconds

namespace hLLM::roles
{

class RequestManager final : public hLLM::Role
{
  public:

  RequestManager() = delete;
  ~RequestManager() = default;

  RequestManager(
    const configuration::Deployment deployment,
    taskr::Runtime* const taskr
  ) : Role(deployment, taskr), _rTA("0.0.0.0", 5003), _cli("localhost", 5003), num_responses(0), prev_num_responses(0)
  {
    // Name of the prompt input
    const auto& promptInputName = _deployment.getRequestManager()->getInput();
    const auto& resultOutputName = _deployment.getRequestManager()->getOutput();

    // Getting partition list
    const auto& partitions = _deployment.getPartitions();

    // Getting list of edges in the deployment
    const auto& edgeConfigs = _deployment.getEdges();

    // Looking for the edge corresponding to a partition's prompt input 
    for (size_t edgeIdx = 0; edgeIdx < edgeConfigs.size(); edgeIdx++)
    {
      const auto& edgeConfig = edgeConfigs[edgeIdx];
      const auto& edgeName = edgeConfig->getName();
        
      // If this is the prompt input, then create the outgoing edge to the corresponding partition
      if (edgeConfig->isPromptEdge() == true)
      {
        // Looking for the partition who needs the prompt input
        for (configuration::Partition::partitionIndex_t idx = 0; idx < partitions.size(); idx++)
        {
          const auto& partition = partitions[idx];
          for (const auto& task : partition->getTasks())
            for (const auto& input : task->getInputs())
              if (input == promptInputName) _promptConsumerPartitionIdx = idx;
        }

        // Creating the prompt sending edge
        _promptOutputEdge = std::make_shared<edge::Output>(*edgeConfig, edge::edgeType_t::requestManagerToCoordinator, edgeIdx, _promptConsumerPartitionIdx, _promptConsumerPartitionIdx, edge::Base::coordinatorReplicaIndex);
        // printf("[Request Manager] Prompt Output Edge: Type: %u, EdgeIdx: %lu, CP: %lu, PP: %lu, RI: %lu\n", edge::edgeType_t::requestManagerToCoordinator, edgeIdx, _promptConsumerPartitionIdx, _promptConsumerPartitionIdx, edge::Base::coordinatorReplicaIndex);
      }
    } 

    // Looking for the edge corresponding to a partition's prompt input 
    for (size_t edgeIdx = 0; edgeIdx < edgeConfigs.size(); edgeIdx++)
    {
      const auto& edgeConfig = edgeConfigs[edgeIdx];
      const auto& edgeName = edgeConfig->getName();
        
      // If this is the result output, then create the incoming edge from the corresponding partition
      if (edgeConfig->isResultEdge() == true)
      {
        // Looking for the partition who needs the prompt input
        for (configuration::Partition::partitionIndex_t idx = 0; idx < partitions.size(); idx++)
        {
          const auto& partition = partitions[idx];
          for (const auto& task : partition->getTasks())
            for (const auto& output : task->getOutputs())
              if (output == resultOutputName) _resultProducerPartitionIdx = idx;
        }

        // Creating the result-receiving edge
        _resultInputEdge = std::make_shared<edge::Input>(*edgeConfig, edge::edgeType_t::coordinatorToRequestManager, edgeIdx, _resultProducerPartitionIdx, _resultProducerPartitionIdx, edge::Base::coordinatorReplicaIndex);
        // printf("[Request Manager] Result Input Edge: Type: %u, EdgeIdx: %lu, CP: %lu, PP: %lu, RI: %lu\n", edge::edgeType_t::coordinatorToRequestManager, edgeIdx, _resultProducerPartitionIdx, _resultProducerPartitionIdx, edge::Base::coordinatorReplicaIndex);
      }
    }

    // set the previous time tracker to now.
    prev_time = Clock::now();
  }

  // Gets the memory slots required by the edges
  __INLINE__ void getMemorySlotsToExchange(std::vector<hLLM::edge::memorySlotExchangeInfo_t>& memorySlots)
  {
    _promptOutputEdge->getMemorySlotsToExchange(memorySlots);
    _resultInputEdge->getMemorySlotsToExchange(memorySlots);
  }

  /// This function completes the initialization of the edges, after the memory slot exchanges are completed
  __INLINE__ void initializeEdges(const HiCR::GlobalMemorySlot::tag_t tag)
  {
    _promptOutputEdge->initialize(tag);
    _resultInputEdge->initialize(tag);
  }

  __INLINE__ void pushPrompt(const std::shared_ptr<Prompt> prompt)
  {
    // Adding prompt to new prompt queue
    _promptManagementMutex.lock();
    _pendingNewPromptsQueue.push(prompt);
    _promptManagementMutex.unlock();
  }

  [[nodiscard]] __INLINE__ std::shared_ptr<Session> createSession()
  {
    // Getting unique session id, atomically increasing its value in the process
    const auto sessionId = _currentSessionId.fetch_add(1);

    // Creating new session
    auto session = std::make_shared<Session>(sessionId);

    // Registering session
    _sessionManagementMutex.lock();
    _pendingSessionConnectionsQueue.push(session);
    _sessionManagementMutex.unlock();

    // Wait until session is connected
    while(session->isConnected() == false);

    // Returning session
    return session;
  }

  private:

  /// This function subscribes the handlers and services for the coordinator role
  __INLINE__ void initializeImpl() override
  {
    // If this is the prompt management partition, then set up a service for its handling
    _taskr->addService(&_taskrPromptHandlingService);

    // Subscribing data input edges for the incoming prompt responses
    subscribeEdgeMessageHandler(hLLM::Role::edgeHandlerSubscription_t { hLLM::messages::messageTypes::data,
    _resultInputEdge,
    [this](const std::shared_ptr<edge::Input> edge, const hLLM::edge::Message& message){ responseDataMessageHandler(edge, std::make_shared<hLLM::messages::Data>(message)); } });

    // Adding session management service
    _taskr->addService(&_taskrSessionManagementService);
  }

  __INLINE__ void responseDataMessageHandler(const std::shared_ptr<edge::Input> edge, const std::shared_ptr<hLLM::messages::Data> message)
  {
    // Getting prompt id from data
    const auto promptId = message->getPromptId();
    const auto data = message->getData();
    const auto size = message->getSize();
    
    const std::string response = std::string((const char*)data, size);
    // printf("[Request Manager] Received response '%s' for prompt %lu/%lu, edge '%s'.\n", response.c_str(), promptId.first, promptId.second, edge->getEdgeConfig().getName().c_str());

    // Getting prompt object and removing it from the active prompt map. We've got the response now
    _activePromptMapMutex.lock();
    
    // Check that the prompt actually exists  
    if (_activePromptMap.contains(promptId) == false) HICR_THROW_RUNTIME("Prompt map entry for prompt %lu/%lu not found. This must be a bug in hLLM", promptId.first, promptId.second);

    // Recovering prompt from the map
    auto prompt = _activePromptMap.at(promptId);

    // Removing map entry
    _activePromptMap.erase(promptId);

    // Unlocking prompt map lock
    _activePromptMapMutex.unlock();

    // Setting response into the prompt now
    prompt->setResponse(response);
    
    // printf("Prompt Map Size: %lu\n", _activePromptMap.size());
    // usleep(10000);

    // Increasing the counter
    // Compute the new average response per minute value
    now_time = Clock::now();
    
    auto time_diff_sec = std::chrono::duration<double>(now_time - prev_time).count();

    const double resp_diff = double(++num_responses - prev_num_responses);
    
    const double avg_res_per_minute = resp_diff/time_diff_sec * 60.0;

    // Update the prev_time if it was longer than a second
    if(time_diff_sec > 10.0)
    {
      prev_time = now_time;
      prev_num_responses = num_responses;
    }

    std::string json_data =
        "{\n"
        "  \"partition\": {\n"
        "  \"name\": \"partition1\",\n" // For now, fake partition value
        "  \"status\": \"active\",\n"
        "  \"num_responses\": " + std::to_string(num_responses) + ",\n"
        "  \"avg_responses_per_min\": " + std::to_string(avg_res_per_minute) + "\n"
        "  }\n"
        "}";

    auto res = _cli.Post("/data", json_data, "application/json");

    // Error handling to check if the HTTP post was successfull
    if(!res) std::cerr << "Failed to connect to server.\n";
  }

  ///////////// Prompt handling service
  __INLINE__ void promptHandlingService()
  {
    // Checking for new prompts to be added to the list
    std::lock_guard<std::mutex> lock(_promptManagementMutex);

    // Accepting incoming session connection requests
    if (_pendingNewPromptsQueue.empty() == false)
    {
      // Getting next pending session to connect
      const auto prompt = _pendingNewPromptsQueue.front();
      const auto promptId = prompt->getPromptId();
      const auto& promptData = prompt->getPrompt();
      
      // Sending data to the partition that takes the prompt as input
      const auto messageData = (const uint8_t*)promptData.data();
      const size_t messageSize = promptData.size()+1;

      // Interrupt service if the output edge (connecting to the entry partition) is full
      _promptOutputEdge->lock();
      const auto isPromptOutputEdgeFull = _promptOutputEdge->isFull(messageSize);
      _promptOutputEdge->unlock(); 
      if (isPromptOutputEdgeFull == true) return;

      // Registering prompt
      _activePromptMapMutex.lock();
      _activePromptMap.insert({promptId, prompt});
      _activePromptMapMutex.unlock();

      // Creating message object
      const auto message = messages::Data(messageData, messageSize, promptId);

      // Encoding message
      const auto rawMessage = message.encode();

      // Send message once the recipient is ready
      _promptOutputEdge->pushMessageLocking(rawMessage);

      // printf("[Request ManageR] Added Prompt Id: %lu/%lu\n", promptId.first, promptId.second);

      // Freeing entry in the pending session connection queue
      _pendingNewPromptsQueue.pop();
    }
  }
  taskr::Service::serviceFc_t _promptHandlingServiceFunction = [this](){ this->promptHandlingService(); };
  taskr::Service _taskrPromptHandlingService = taskr::Service(_promptHandlingServiceFunction);
  
  ///////////// Session management service (no concurrent access active service map)
  __INLINE__ void sessionManagementServiceFunction()
  {
    std::lock_guard<std::mutex> lock(_sessionManagementMutex);

    // Accepting incoming session connection requests
    if (_pendingSessionConnectionsQueue.empty() == false)
    {
      // Getting next pending session to connect
      auto session = _pendingSessionConnectionsQueue.front();
      
      // Registering session
      _activeSessionMap.insert({session->getSessionId(), session});

      // Setting session as connected
      session->connect();

      // Freeing entry in the pending session connection queue
      _pendingSessionConnectionsQueue.pop();
    }

    // Iterating over the active sessions in search for the next message to parse
    for (const auto& entry : _activeSessionMap)
    {
      // Getting session
      const auto& session = entry.second;

      // Getting next prompt from the session, if any
      const auto prompt = session->getPrompt();

      // If no prompts are available, continue onto the next session
      if (prompt == nullptr) continue;

      // Otherwise, process it
      pushPrompt(prompt);
    } 
  }
  taskr::Service::serviceFc_t _taskrSessionManagementFunction = [this](){ this->sessionManagementServiceFunction(); };
  taskr::Service _taskrSessionManagementService = taskr::Service(_taskrSessionManagementFunction);
  
  // Edge to copy a prompt to a coordinator
  std::shared_ptr<edge::Output> _promptOutputEdge;
  
  // Edge to receive a result from a coordinator
  std::shared_ptr<edge::Input> _resultInputEdge;

  // Prompt management mutex
  std::mutex _promptManagementMutex;

  // Queue of pending new prompts
  std::queue<std::shared_ptr<Prompt>> _pendingNewPromptsQueue;

  // Active prompt map
  std::mutex _activePromptMapMutex;
  std::map<Prompt::promptId_t, std::shared_ptr<Prompt>> _activePromptMap;

  // Index within the deployment of the partition who will consume the initial prompt
  configuration::Partition::partitionIndex_t _promptConsumerPartitionIdx; 

  // Index within the deployment of the partition who will produce the prompt result
  configuration::Partition::partitionIndex_t _resultProducerPartitionIdx; 

  // Global counter for session ids
  std::atomic<sessionId_t> _currentSessionId = 0;

  // Session management mutex
  std::mutex _sessionManagementMutex;

  // Queue of pending sessions for connection
  std::queue<std::shared_ptr<Session>> _pendingSessionConnectionsQueue;

  // Active session map
  std::map<sessionId_t, std::shared_ptr<Session>> _activeSessionMap;

  // Initializing the realTimeAnalysis instance to start listening number of requests
  RealTimeAnalysis _rTA;

  // requestManager's own HTTP client as it will pass the number of requests per minute
  httplib::Client _cli;

  // Number of responses tracker
  size_t num_responses, prev_num_responses;

  // time tracker for the number of responses
  Clock::time_point now_time, prev_time;

}; // class RequestManager

} // namespace hLLM
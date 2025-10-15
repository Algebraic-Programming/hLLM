#pragma once

#include <memory>
#include <vector>
#include <hicr/core/exceptions.hpp>
#include <hicr/core/definitions.hpp>
#include <hicr/core/globalMemorySlot.hpp>
#include <taskr/taskr.hpp>
#include "../role.hpp"
#include "../edge/base.hpp"
#include "../edge/output.hpp"
#include "../configuration/deployment.hpp"

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
  ) : Role(deployment, taskr)
  {
    // Name of the prompt input
    const auto& promptInputName = _deployment.getUserInterface().input;
    const auto& resultOutputName = _deployment.getUserInterface().output;

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
      if (edgeName == promptInputName)
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
      if (edgeName == resultOutputName)
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

  private:

  /// This function subscribes the handlers and services for the coordinator role
  __INLINE__ void initializeImpl() override
  {
    // Registering a handler for the prompt message 
    subscribeMessageHandler(hLLM::messages::messageTypes::prompt, [this](const std::shared_ptr<edge::Input> edge, const hLLM::edge::Message& message){ promptMessageHandler(edge, std::make_shared<hLLM::messages::Prompt>(message)); });

    // If this is the prompt management partition, then set up a service for its handling
    _taskr->addService(&_taskrPromptHandlingService);
  }

  __INLINE__ void pushPrompt(const std::shared_ptr<hLLM::messages::Prompt> prompt)
  {
    // Finding the corresponding prompt edge
    // const auto& promptEdge = _partitionDataOutputs[_edgeIndexToVectorPositionMap[_promptEdgeIdx]];

    // Encoding raw message
    // const auto rawMessage = prompt->encode();

    // printf("Pushing Prompt A\n");
    // Waiting until it is freed
    // while (promptEdge->isFull(rawMessage.getSize()) == true);
    // printf("Pushing Prompt B\n");

    // Pushing prompt to the corresponding edge
    // promptEdge->pushMessage(rawMessage);
  }

  __INLINE__ void promptMessageHandler(const std::shared_ptr<edge::Input> edge, const std::shared_ptr<hLLM::messages::Prompt> message)
  {
    // Creating new prompt object
    const auto input = message->getInput();
    const auto sessionId = message->getSessionId();
    const auto messageId = message->getMessageId();
    const auto promptId = Prompt::promptId_t(sessionId, messageId);
    printf("[Request Manager] Received prompt '%s' from session %lu, message: %lu\n", input.c_str(), sessionId, messageId);

    // Creating new prompt
    auto prompt = std::make_shared<Prompt>(promptId, input);

    // Adding prompt to new prompt queue
    _promptManagementMutex.lock();
    _pendingNewPromptsQueue.push(prompt);
    _promptManagementMutex.unlock();
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


  // Edge to copy a prompt to a coordinator
  edge::edgeInfo_t _promputEdgeInfo;
  std::shared_ptr<edge::Output> _promptOutputEdge;
  
  // Edge to receive a result from a coordinator
  edge::edgeInfo_t _resultEdgeInfo;
  std::shared_ptr<edge::Input> _resultInputEdge;

  // Prompt management mutex
  std::mutex _promptManagementMutex;

  // Queue of pending new prompts
  std::queue<std::shared_ptr<Prompt>> _pendingNewPromptsQueue;

  // Active prompt map
  std::map<Prompt::promptId_t, std::shared_ptr<Prompt>> _activePromptMap;

  // Index within the deployment of the partition who will consume the initial prompt
  configuration::Partition::partitionIndex_t _promptConsumerPartitionIdx; 

  // Index within the deployment of the partition who will produce the prompt result
  configuration::Partition::partitionIndex_t _resultProducerPartitionIdx; 

}; // class RequestManager

} // namespace hLLM
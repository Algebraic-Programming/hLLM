#pragma once

#include <atomic>
#include <memory>
#include <vector>
#include <hicr/core/exceptions.hpp>
#include <hicr/core/definitions.hpp>
#include <taskr/taskr.hpp>
#include "../../configuration/deployment.hpp"
#include "../../edge/input.hpp"
#include "../../edge/output.hpp"
#include "../../messages/heartbeat.hpp"
#include "../../task.hpp"
#include "../../prompt.hpp"
#include "base.hpp"

namespace hLLM::roles::partition
{

class Replica final : public Base
{
  public:

  Replica() = delete;

  Replica(
    const configuration::Deployment deployment,
    const configuration::Partition::partitionIndex_t  partitionIdx,
    const configuration::Replica::replicaIndex_t replicaIdx,
    taskr::Runtime* const taskr,
    const std::map<std::string, Task::taskFunction_t>& registeredFunctions
  ) : Base(deployment, partitionIdx, taskr),
    _replicaIdx(replicaIdx),
    _registeredFunctions(registeredFunctions)
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
    _coordinatorControlInput = std::make_shared<edge::Input>(*_controlEdgeConfig, edge::edgeType_t::coordinatorToReplica, edge::Base::controlEdgeIndex, _partitionIdx, _partitionIdx, _replicaIdx);
    _coordinatorControlOutput = std::make_shared<edge::Output>(*_controlEdgeConfig, edge::edgeType_t::replicaToCoordinator, edge::Base::controlEdgeIndex, _partitionIdx, _partitionIdx, _replicaIdx);

    ////// Building execution graph
    const auto &tasks = partitionConfiguration->getTasks();

    // Creating general TaskR function for all execution graph tasks
    _taskrFunction = std::make_unique<taskr::Function>([this](taskr::Task *task) { runTaskRFunction(task); });

    // Checking the execution graph functions have been registered
    taskr::taskId_t taskrLabelCounter = 0;
    for (const auto &task : tasks)
    {
      // Checking the requested function was registered
      const auto taskFunctionName = task->getFunctionName();
      if (_registeredFunctions.contains(taskFunctionName) == false) HICR_THROW_LOGIC("The requested function name '%s' is not registered. Please register it before running the hLLM.", taskFunctionName.c_str());

      // Getting label for taskr function
      const auto taskId = taskrLabelCounter++;

      // Creating taskr Task corresponding to the LLM engine task
      auto taskrTask = std::make_unique<taskr::Task>(taskId, _taskrFunction.get());

      // Getting function pointer
      const auto &fc = _registeredFunctions.at(taskFunctionName);

      // Creating hLLM Task object
      auto newTask = std::make_shared<hLLM::Task>(*task, fc, std::move(taskrTask));

      // Adding task to the label->Task map
      _taskLabelMap.insert({taskId, newTask});

      // Adding task to TaskR itself
      _taskr->addTask(newTask->getTaskRTask());
    }
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
  
  __INLINE__ void runTaskRFunction(taskr::Task *task)
  {
    const auto &taskId       = task->getTaskId();
    const auto &taskObject   = _taskLabelMap.at(taskId);
    const auto &function = taskObject->getFunction();

    // Function to check for pending operations
    auto pendingOperationsCheck = [&]() {
      // If execution must stop now, return true to go back to the task and finish it
      if (_continueRunning == false) return true;

      // usleep(5000);
      // printf("Task %s Not ready yet\n", taskObject->getConfig().getFunctionName().c_str());
      return false;

      // All dependencies are satisfied, enable this task for execution
      return true;
    };

    // Initiate infinite loop
    while (_continueRunning)
    {
      // Adding task dependencies
      task->addPendingOperation(pendingOperationsCheck);

      // Suspend execution until all dependencies are met
      task->suspend();

      // Another exit point (the more the better)
      if (_continueRunning == false) break;

      // Actually run the function now
      function(taskObject.get());

      // Another exit point (the more the better)
      if (_continueRunning == false) break;
    }
  }

  /// This function initializes the workload of the partition replica role
  __INLINE__ void initializeImpl() override
  {
    printf("[Replica %lu / %lu] Initializing...\n", _partitionIdx, _replicaIdx);

    // Indicate we have no current active prompt being processed
    _activePrompt = nullptr;

    //////// Adding heartbeat service for my coordinator's control edge
    subscribeHeartbeatEdge(_coordinatorControlOutput);

    // Registering a handler for the handshake message 
    subscribeMessageEdge(_coordinatorControlInput);
    subscribeMessageHandler(hLLM::messages::messageTypes::heartbeat, [this](const std::shared_ptr<edge::Input> edge, const hLLM::edge::Message& message){ heartbeatMessageHandler(edge, std::make_shared<hLLM::messages::Heartbeat>(message)); });

    ////// Adding data inputs to the message handler
    subscribeMessageHandler(hLLM::messages::messageTypes::data, [this](const std::shared_ptr<edge::Input> edge, const hLLM::edge::Message& message){ dataMessageHandler(edge, std::make_shared<hLLM::messages::Data>(message)); });
    for (const auto& edge : _coordinatorDataInputs) subscribeMessageEdge(edge);
  }

  void heartbeatMessageHandler(const std::shared_ptr<edge::Input> edge, const std::shared_ptr<hLLM::messages::Heartbeat> message)
  {
    if(_deployment.getHeartbeat().visible == true)  printf("[Replica %lu / %lu] Received heartbeat from coordinator.\n", _partitionIdx, _replicaIdx);
  }

    __INLINE__ void dataMessageHandler(const std::shared_ptr<edge::Input> edge, const std::shared_ptr<hLLM::messages::Data> message)
  {
    // Getting prompt id from data
    const auto promptId = message->getPromptId();
    const auto data = message->getData();
    const auto size = message->getSize();
    const auto edgeIdx = edge->getEdgeIndex();
    const auto edgePos = _edgeIndexToVectorPositionMap[edgeIdx];
    
    printf("[Replica %lu] Received data for prompt %lu/%lu, edge '%s'.\n", _partitionIdx, promptId.first, promptId.second, edge->getEdgeConfig().getName().c_str());
  }

  // Identifier for the replica index
  const configuration::Replica::replicaIndex_t _replicaIdx;

  // Data Input/Output edges from/to the coordinator
  std::vector<std::shared_ptr<edge::Input>> _coordinatorDataInputs;
  std::vector<std::shared_ptr<edge::Output>> _coordinatorDataOutputs;

  // Control Input/Output edges from/to the coordinator
  std::shared_ptr<edge::Input> _coordinatorControlInput;
  std::shared_ptr<edge::Output> _coordinatorControlOutput;

  // Map relating task labels to their hLLM task
  std::map<taskr::taskId_t, std::shared_ptr<Task>> _taskLabelMap;

  // The set of registered functions to use as targets for tasks
  const std::map<std::string, Task::taskFunction_t> _registeredFunctions;

  // The main driver for running tasks
  std::unique_ptr<taskr::Function> _taskrFunction;

  // Pointer for the current active prompt being processed
  std::atomic<std::shared_ptr<Prompt>> _activePrompt;

  

}; // class Replica

} // namespace hLLM::replica
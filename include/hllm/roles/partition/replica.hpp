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

    // Creating general TaskR function for all execution graph tasks
    _taskrFunction = std::make_unique<taskr::Function>([this](taskr::Task *task) { runTaskRFunction(task); });

    // Resetting label count
    _taskrLabelCounter = 0;

    // Getting partition's tasks
    const auto &tasks = partitionConfiguration->getTasks();

    // Calculating, for each of this partition's tasks, what are the edge indexes that correspond to their inputs
    for (const auto& task : tasks)
      for (const auto& taskInput : task->getInputs())
        for (size_t edgePos = 0; edgePos < _inputEdges.size(); edgePos++)
          if (taskInput == _inputEdges[edgePos].config->getName())
            { _taskInputEdgePositions[task->getFunctionName()].push_back(edgePos); break; }

    // Similarly, for each edge, store which tasks need to be notified of their arrival
    for (const auto& input : _inputEdges)
     for (const auto& task : tasks)
       for (const auto& taskInput : task->getInputs())
       {
        const auto& inputName = input.config->getName();
        if (taskInput == inputName)
          { _inputEdgeTaskDependencies[inputName].push_back(task->getFunctionName()); break; }
       }

    // Calculating, for each of this partition's tasks, what are the edge indexes that correspond to their outputs
    for (const auto& task : tasks)
      for (const auto& taskOutput : task->getOutputs())
        for (size_t edgePos = 0; edgePos < _inputEdges.size(); edgePos++)
          if (taskOutput == _outputEdges[edgePos].config->getName())
            { _taskOutputEdgePositions[taskOutput] = edgePos; break; }
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
  
  __INLINE__ void runTaskRFunction(taskr::Task *taskrTask)
  {
    const auto &taskId     = taskrTask->getTaskId();
    const auto &task       = _taskLabelMap.at(taskId);
    const auto &taskConfig = task->getConfig();
    const auto &function   = task->getFunction();

    // Actually run the function now
    function(task.get());

    // Once the task has finished, push all its outputs to the active job output edges
    for (const auto& output : taskConfig.getOutputs())
    {
      // Getting edge position corresponding to the task output
      const auto outputEdgePos = _taskOutputEdgePositions[output];

      // Getting corresponding output edge
      auto& outputEdge = _activeJob->getOutputEdges()[outputEdgePos];

      // Getting output from task
      printf("[Replica] Getting data slot for output '%s'\n", output.c_str());
      const auto& outputDataSlot = task->getOutput(output);

      // Setting output edge's data
      outputEdge.setDataSlot(outputDataSlot);

      // Getting active job's prompt id
      const auto promptId = _activeJob->getPromptId();

      // Pushing message back to the coordinator immediately
      const auto message = messages::Data((const uint8_t*)outputDataSlot->getPointer(), outputDataSlot->getSize(), promptId);
      _coordinatorDataOutputs[outputEdgePos]->pushMessage(message.encode());

      // Deregistering data slot
      outputEdge.deregisterDataSlot();

      // Marking output as satisfied
      outputEdge.setSatisfied();
    }
  }

  /// This function initializes the workload of the partition replica role
  __INLINE__ void initializeImpl() override
  {
    printf("[Replica %lu / %lu] Initializing...\n", _partitionIdx, _replicaIdx);

    // Indicate we have no current active job being processed
    _activeJob = nullptr;

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

    printf("[Replica %lu/%lu] Received data for prompt %lu/%lu, edge '%s'.\n", _partitionIdx, _replicaIdx, promptId.first, promptId.second, edge->getEdgeConfig().getName().c_str());

    // If there is a current job assigned to this replica and the job corresponds to a different prompt, then fail
    if (_activeJob != nullptr) if (promptId != _activeJob->getPromptId())
     HICR_THROW_RUNTIME("[Replica %lu/%lu] Received data for prompt %lu/%lu, edge '%s' but currently prompt %lu/%lu is running.\n", _partitionIdx, _replicaIdx, promptId.first, promptId.second, edge->getEdgeConfig().getName().c_str(), _activeJob->getPromptId().first, _activeJob->getPromptId().second);

    // If there is no current active job
    if (_activeJob == nullptr)
    {
      // Create a new one
      _activeJob = std::make_shared<Job>(promptId, _inputEdges, _outputEdges);

      // And set it in motion
      startJob(_activeJob);
    } 

    // Getting input corresponding to the message that arrived
    auto& input = _activeJob->getInputEdges()[edgePos];
    const auto& inputName = input.getEdgeInfo().config->getName();
    const auto& inputData = input.getDataSlot();

    // Store a reference to the provided data into the edge (no extra copies required)
    input.storeDataByReference(data, size);

    // Assigning all interested tasks the input data
    for (const auto& task : _inputEdgeTaskDependencies[inputName]) _taskFunctionNameMap[task]->setInput(inputName, inputData);

    // Marking input as satisfied
    input.setSatisfied();
  }

  __INLINE__ void startJob(std::shared_ptr<Job>& job)
  {
    // Get my partition configuration
    const auto& partitionConfiguration = _deployment.getPartitions()[_partitionIdx];
    const auto &tasks = partitionConfiguration->getTasks();

    // Clearing previous job's maps
    _taskLabelMap.clear();
    _taskFunctionNameMap.clear();

    // Building execution graph
    for (const auto &task : tasks)
    {
      // Checking the requested function was registered
      const auto taskFunctionName = task->getFunctionName();
      if (_registeredFunctions.contains(taskFunctionName) == false) HICR_THROW_LOGIC("The requested function name '%s' is not registered. Please register it before running the hLLM.", taskFunctionName.c_str());

      // Getting label for taskr function
      const auto taskId = _taskrLabelCounter++;

      // Creating taskr Task corresponding to the LLM engine task
      auto taskrTask = std::make_unique<taskr::Task>(taskId, _taskrFunction.get());

      // Getting function pointer
      const auto &fc = _registeredFunctions.at(taskFunctionName);

      // Creating hLLM Task object
      auto newTask = std::make_shared<hLLM::Task>(*task, fc, std::move(taskrTask));

      // Get ahold of the task dependency edge positions for dependency checking
      const auto& taskInputEdgePositions = _taskInputEdgePositions[taskFunctionName];

      // Function to check the tasks inputs are present before executing it
      auto taskInputsCheck = [&]() {
      // If there is an active job, check whether the inputs for this tasks are satisfied within it
      for (const auto edgePos : taskInputEdgePositions) if (_activeJob->getInputEdges()[edgePos].isSatisfied() == false) return false;

      // All dependencies are satisfied, enable this task for execution
      return true;
      };

      // Adding task to the label->Task map
      _taskLabelMap.insert({taskId, newTask});

      // Adding task to the functionName->Task map
      _taskFunctionNameMap.insert({taskFunctionName, newTask});

      // Adding task dependencies
      newTask->getTaskRTask()->addPendingOperation(taskInputsCheck);

      // Adding task to TaskR itself
      _taskr->addTask(newTask->getTaskRTask());
    }
  }

  // Identifier for the replica index
  const configuration::Replica::replicaIndex_t _replicaIdx;

  // Data Input/Output edges from/to the coordinator
  std::vector<std::shared_ptr<edge::Input>> _coordinatorDataInputs;
  std::vector<std::shared_ptr<edge::Output>> _coordinatorDataOutputs;

  // Control Input/Output edges from/to the coordinator
  std::shared_ptr<edge::Input> _coordinatorControlInput;
  std::shared_ptr<edge::Output> _coordinatorControlOutput;

  // Map relating task function names to their input edge positions (for data dependency checking)
  std::map<std::string, std::vector<size_t>> _taskInputEdgePositions;

  // Map relating task output names to their input edge positions
  std::map<std::string, size_t> _taskOutputEdgePositions;

  // Map relating input edges to the task function names that depend on them
  std::map<std::string, std::vector<std::string>> _inputEdgeTaskDependencies;

  // Map relating task ids to their hLLM task
  std::map<taskr::taskId_t, std::shared_ptr<Task>> _taskLabelMap;

  // Map relating task function names to their hLLM task
  std::map<std::string, std::shared_ptr<Task>> _taskFunctionNameMap;

  // The set of registered functions to use as targets for tasks
  const std::map<std::string, Task::taskFunction_t> _registeredFunctions;

  // A global count to set unique label ids to each task
  taskr::taskId_t _taskrLabelCounter;

  // The main driver for running tasks
  std::unique_ptr<taskr::Function> _taskrFunction;

  // Pointer for the current active job being processed
  std::shared_ptr<Job> _activeJob;
}; // class Replica

} // namespace hLLM::replica
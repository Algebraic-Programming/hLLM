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
#include "../../messages/replicaReady.hpp"
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
      {
        bool foundEdge = false;
        printf("Looking for task output: %s\n", taskOutput.c_str());

        // Finding the edge corresponding to this task output
        for (size_t edgePos = 0; edgePos < _outputEdges.size(); edgePos++)
          if (taskOutput == _outputEdges[edgePos].config->getName())
            {
              _taskOutputEdgePositions[taskOutput] = edgePos;
              foundEdge = true;
              break;
            }

        // Sanity check    
        if (foundEdge == false) HICR_THROW_RUNTIME("[Replica %lu / %lu] Could not find the edge for output: %s. This must be a bug in hLLM", _partitionIdx, _replicaIdx, taskOutput.c_str());
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
  
  __INLINE__ void runTaskRFunction(taskr::Task *taskrTask)
  {
    const auto &taskId     = taskrTask->getTaskId();
    const auto &task       = _taskLabelMap.at(taskId);
    const auto &taskConfig = task->getConfig();
    const auto &function   = task->getFunction();
    const auto promptId    = _activeJob->getPromptId();

    // Setting tasks's partition and replica idxs
    task->setPartitionIdx(_partitionIdx);
    task->setReplicaIdx(_replicaIdx);
    task->setPromptId(promptId);

    // Setting the edges that the task needs to send outputs through
    for (const auto& output : taskConfig.getOutputs())
    {
      // Getting edge position corresponding to the task output
      if (_taskOutputEdgePositions.contains(output) == false) HICR_THROW_RUNTIME("[Replica %lu / %lu] Could not find output: %s's position...", _partitionIdx, _replicaIdx, output.c_str());
      const auto outputEdgePos = _taskOutputEdgePositions.at(output);

      // Setting output edge
      task->setOutputEdge(output, _coordinatorDataOutputs[outputEdgePos]);
    } 

    // Actually run the function now
    function(task.get());

    // Verify all outputs have been sent by this task
    task->verifyOutputsSent();

    // Preventing concurrent access
    _jobManagementMutex.lock();

    // Once the task has finished, mark all its outputs as finished in the job's output set
    for (const auto& output : taskConfig.getOutputs())
    {
      // Getting edge position corresponding to the task output
      if (_taskOutputEdgePositions.contains(output) == false) HICR_THROW_RUNTIME("[Replica %lu / %lu] Could not find output: %s's position...", _partitionIdx, _replicaIdx, output.c_str());
      const auto outputEdgePos = _taskOutputEdgePositions.at(output);

      // Getting corresponding output edge
      // printf("[Replica %lu / %lu] Pushing Output: %s (Pos: %lu)...\n", _partitionIdx, _replicaIdx, output.c_str(), outputEdgePos);
      auto& outputEdge = _activeJob->getOutputEdges()[outputEdgePos];

      // Getting output from task
      outputEdge.setSatisfied();
    }

    // Now Check if the job is now finished
    if (_activeJob != nullptr)
    {
      // If any of the outputs is not yet satisfied, then the job is not finished
      bool isJobFinished = true;
      for (const auto& output : _activeJob->getOutputEdges()) if (output.isSatisfied() == false) isJobFinished = false;

      // If it is finished, remove it as active job
      if (isJobFinished == true)
      {
        // printf("[Replica] Job %lu/%lu is now finished\n", _activeJob->getPromptId().first, _activeJob->getPromptId().second);
        _activeJob = nullptr;

        // Now send the coordinator the signal that we're again ready
        hLLM::messages::ReplicaReady message;
        auto rawMessage = message.encode();

        // Pushing message
        _coordinatorControlOutput->pushMessageLocking(rawMessage);
      } 
    }

    _jobManagementMutex.unlock();
  }

  /// This function initializes the workload of the partition replica role
  __INLINE__ void initializeImpl() override
  {
    // printf("[Replica %lu / %lu] Initializing...\n", _partitionIdx, _replicaIdx);

    // Indicate we have no current active job being processed
    _activeJob = nullptr;

    //////// Adding heartbeat service for my coordinator's control edge
    subscribeHeartbeatEdge(_coordinatorControlOutput);

    // Subscribing control edges to the message service for my replicas
    subscribeEdgeMessageHandler(hLLM::Role::edgeHandlerSubscription_t { hLLM::messages::messageTypes::heartbeat,
    _coordinatorControlInput,
    [this](const std::shared_ptr<edge::Input> edge, const hLLM::edge::Message& message){ heartbeatMessageHandler(edge, std::make_shared<hLLM::messages::Heartbeat>(message)); } });

    // Subscribing control edges to the message service for my replicas
    for (const auto& dataEdge : _coordinatorDataInputs)
     subscribeEdgeMessageHandler(hLLM::Role::edgeHandlerSubscription_t { hLLM::messages::messageTypes::data,
     dataEdge,
     [this](const std::shared_ptr<edge::Input> edge, const hLLM::edge::Message& message){ inputDataMessageHandler(edge, std::make_shared<hLLM::messages::Data>(message)); } });
  }

  void heartbeatMessageHandler(const std::shared_ptr<edge::Input> edge, const std::shared_ptr<hLLM::messages::Heartbeat> message)
  {
    if(_deployment.getHeartbeat().visible == true)  printf("[Replica %lu / %lu] Received heartbeat from coordinator.\n", _partitionIdx, _replicaIdx);
  }

  __INLINE__ void inputDataMessageHandler(const std::shared_ptr<edge::Input> edge, const std::shared_ptr<hLLM::messages::Data> message)
  {
    // Getting prompt id from data
    const auto promptId = message->getPromptId();
    const auto data = message->getData();
    const auto size = message->getSize();
    const auto edgeIdx = edge->getEdgeIndex();
    const auto edgePos = _edgeIndexToVectorPositionMap[edgeIdx];

    // Preventing concurrent access
    std::lock_guard lockGuard(_jobManagementMutex);

    // printf("[Replica %lu/%lu] Received data for prompt %lu/%lu, edge '%s'.\n", _partitionIdx, _replicaIdx, promptId.first, promptId.second, edge->getEdgeConfig().getName().c_str());

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

    // Wait for taskR to finish all tasks before clearing the maps (to avoid premature freeing of task metadata)
    while (_taskr->getActiveTaskCounter() > 0);
    
    // Clearing previous job's maps
    _taskLabelMap.clear();
    _taskFunctionNameMap.clear();

    // Re-building execution graph
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

      // Adding task input dependencies
      newTask->getTaskRTask()->addPendingOperation(taskInputsCheck);
    }

    // Adding task <-> task dependencies
    for (const auto &task : tasks) 
    {
      const auto& dependentTaskName = task->getFunctionName();
      const auto& dependentTask = _taskFunctionNameMap[dependentTaskName];
      const auto& dependentTaskRTask = dependentTask->getTaskRTask();

      for (const auto& dependedTaskName : task->getDependencies())
      {
        const auto& dependedTask = _taskFunctionNameMap[dependedTaskName];
        const auto& dependedTaskRTask = dependedTask->getTaskRTask();
        dependentTaskRTask->addDependency(dependedTaskRTask);
      } 
    }

    // Adding tasks to TaskR itself
    for (const auto &task : tasks) 
    {
      const auto& taskName = task->getFunctionName();
      const auto& taskObject = _taskFunctionNameMap[taskName];
      const auto& taskRTask = taskObject->getTaskRTask();
      _taskr->addTask(taskRTask);
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

  // Mutual exclusion mechanism for job/task management
  std::mutex _jobManagementMutex;
}; // class Replica

} // namespace hLLM::replica
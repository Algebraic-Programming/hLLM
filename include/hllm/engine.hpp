#pragma once

#include <hicr/core/exceptions.hpp>
#include <hicr/core/definitions.hpp>
#include <taskr/taskr.hpp>
#include "configuration/deployment.hpp"
#include "task.hpp"
#include "partitionCoordinator.hpp"

#define __HLLM_WORKER_ENTRY_POINT_RPC_NAME "[hLLM] Worker Entry Point"
#define __HLLM_REQUEST_DEPLOYMENT_CONFIGURATION_RPC_NAME "[hLLM] Request Deployment Configuration"
#define __HLLM_REQUEST_DEPLOYMENT_STOP_RPC_NAME "[hLLM] Request Deployment Stop"
#define __HLLM_BROADCAST_DEPLOYMENT_STOP_RPC_NAME "[hLLM] Broadcast Deployment Stop"

namespace hLLM
{

class Engine final
{
  public:

  Engine(HiCR::InstanceManager             *instanceManager,
         HiCR::frontend::RPCEngine         *rpcEngine,
         taskr::Runtime                    *taskr)
    : _instanceManager(instanceManager),
      _rpcEngine(rpcEngine),
      _taskr(taskr),
      _instanceId(_instanceManager->getCurrentInstance()->getId())
  {
    // Registering entry point function for partition coordinators / replicas. Not for the deployment coordinator
    _rpcEngine->addRPCTarget(__HLLM_WORKER_ENTRY_POINT_RPC_NAME, HiCR::backend::pthreads::ComputeManager::createExecutionUnit([this](void*) { workerEntryPoint(); }));

    // Registering deployment information request
    _rpcEngine->addRPCTarget(__HLLM_REQUEST_DEPLOYMENT_CONFIGURATION_RPC_NAME, HiCR::backend::pthreads::ComputeManager::createExecutionUnit([this](void *) { attendDeploymentConfigurationRequest(); }));

    // Registering finalization function (for root to execute)
    _rpcEngine->addRPCTarget(__HLLM_BROADCAST_DEPLOYMENT_STOP_RPC_NAME, HiCR::backend::pthreads::ComputeManager::createExecutionUnit([this](void *) { doLocalTermination(); }));

    // Registering finalization request function (for non-root to request roots to end the entire execution)
    _rpcEngine->addRPCTarget(__HLLM_REQUEST_DEPLOYMENT_STOP_RPC_NAME, HiCR::backend::pthreads::ComputeManager::createExecutionUnit([this](void *) { broadcastTermination(); }));
  }

  ~Engine() = default;

  /**
   * The instances that call this function will wait for the deployment to start
   */
  __INLINE__ void awaitDeployment()
  {
    // Await for the deployment coordinator to request us to start
    _rpcEngine->listen();
  }

  __INLINE__ void deploy(const configuration::Deployment deployment)
  {
    // Establishing myself as coordinator instance
    _deploymentCoordinatorInstanceId = _instanceManager->getCurrentInstance()->getId();

    // Making a copy of the deployment object
    _deployment = deployment;

    // Considering whether the deployment coordinator is actually part of the deployment
    bool isDeploymentCoordinatorInDeployment = false;

    // Getting the instances involved in the deployment
    std::unordered_set<HiCR::Instance::instanceId_t> instanceSet;
    for (const auto& partition : _deployment.getPartitions())
    {
      // Getting the instance id assigned to the partition coordinator
      const auto partitionCoordinatorInstanceId = partition->getInstanceId();

      // Adding it to the set
      instanceSet.insert(partitionCoordinatorInstanceId);

      // Now getting all replicas for the replica
      for (const auto& replica : partition->getReplicas())
      {
        // Getting the instance id assigned to the replica
        const auto replicaInstanceId = replica->getInstanceId();

        // Adding it to the set
        instanceSet.insert(replicaInstanceId);
      }
    }

    // Iterating over the instances involved in the deployment
    for (const auto instanceId : instanceSet)
    {
      // If it's not me, then request the partition to start executing (worker entry point)
      if (instanceId != _deploymentCoordinatorInstanceId)
      {
        // Requesting them to enter the entry point
        _rpcEngine->requestRPC(instanceId, __HLLM_WORKER_ENTRY_POINT_RPC_NAME);

        // Listening for a deployment configuration request
        _rpcEngine->listen();
      }

      // If it's me, I am part of the execution so remember to launch later
      if (instanceId == _deploymentCoordinatorInstanceId) isDeploymentCoordinatorInDeployment = true;
    }

    // If I (the coordinator) am part of the deployment, run my instance now
    if (isDeploymentCoordinatorInDeployment == true) commonEntryPoint();
  }

  /**
   * Registers a function that can be a target as initial function for one or more requested instances.
   * 
   * If a requested initial function is not registered by this function, deployment will fail.
   * 
   * @param[in] functionName The name of the function to register. This value will be used to match against the requested partition functions
   * @param[in] fc The actual function to register
   * 
   */
  __INLINE__ void registerFunction(const std::string &functionName, const function_t fc)
  {
    // Checking if the RPC name was already used
    if (_registeredFunctions.contains(functionName) == true)
    {
      fprintf(stderr, "The function '%s' was already registered.\n", functionName.c_str());
      abort();
    }

    // Adding new RPC to the set
    _registeredFunctions.insert({functionName, fc});
  }

  __INLINE__ void requestTermination()
  {
    const auto &currentInstance = *_instanceManager->getCurrentInstance();

    // If I am the deployer instance, broadcast termination directly
    if (currentInstance.getId() == _deploymentCoordinatorInstanceId) broadcastTermination();

    // If I am not the deployer instance, request the deployer to please broadcast terminationp
    printf("[hLLM] Instance %lu requesting deployer instance %lu to finish execution.\n", currentInstance.getId(), _deploymentCoordinatorInstanceId);
    _rpcEngine->requestRPC(_deploymentCoordinatorInstanceId, __HLLM_REQUEST_DEPLOYMENT_STOP_RPC_NAME);
  }

  __INLINE__ void finalize()
  {
    // Finished execution, then finish deployment
    const auto &currentInstance = *_instanceManager->getCurrentInstance();
    printf("[hLLM] Instance %lu finalizing deployr.\n", currentInstance.getId());
  }

  [[nodiscard]] __INLINE__ configuration::Deployment getDeployment() const { return _deployment; }
  [[nodiscard]] __INLINE__ size_t getPartitionIdx() const { return _partitionIdx; }

  private:

  __INLINE__ void doLocalTermination()
  {
    printf("[hLLM] Instance %lu terminating TaskR...\n", _deploymentCoordinatorInstanceId);
    // Stopping the execution of the current and new tasks
    _continueRunning = false;
    _taskr->forceTermination();
    printf("[hLLM] Instance %lu terminated TaskR.\n", _deploymentCoordinatorInstanceId);
  }

  __INLINE__ void broadcastTermination()
  {
    const auto &currentInstance = *_instanceManager->getCurrentInstance();

    if (currentInstance.getId() != _deploymentCoordinatorInstanceId) HICR_THROW_RUNTIME("Only the deployer instance %lu can broadcast termination", _deploymentCoordinatorInstanceId);

    // Send a finalization signal to all other non-root instances
    auto &instances = _instanceManager->getInstances();
    for (auto &instance : instances)
      if (instance->getId() != _deploymentCoordinatorInstanceId)
      {
        printf("[hLLM] Instance %lu sending stop RPC to instance %lu.\n", _deploymentCoordinatorInstanceId, instance->getId());
        _rpcEngine->requestRPC(instance->getId(), __HLLM_BROADCAST_DEPLOYMENT_STOP_RPC_NAME);
      }

    // (deployer) Executing local termination myself now
    doLocalTermination();
  }

  __INLINE__ nlohmann::json parseConfiguration(const nlohmann::json &config)
  {
    // Copy the configuration
    nlohmann::json parsedConfig = config;

    // Getting the partitions vector
    const auto &partitionsJs = hicr::json::getArray<nlohmann::json>(parsedConfig, "Partitions");

    // Creating configuration for TaskR
    nlohmann::json taskrConfig;
    taskrConfig["Task Worker Inactivity Time (Ms)"] = 100;   // Suspend workers if a certain time of inactivity elapses
    taskrConfig["Task Suspend Interval Time (Ms)"]  = 100;   // Workers suspend for this time before checking back
    taskrConfig["Minimum Active Task Workers"]      = 1;     // Have at least one worker active at all times
    taskrConfig["Service Worker Count"]             = 1;     // Have one dedicated service workers at all times to listen for incoming messages
    taskrConfig["Make Task Workers Run Services"]   = false; // Workers will check for meta messages in between executions

    // Adding taskr configuration
    parsedConfig["TaskR Configuration"] = taskrConfig;

    // Creating deployR request object
    return parsedConfig;
  }

  __INLINE__ void workerEntryPoint()
  {
      // Getting the instance id of the coordinator instance by the RPC argument
      _deploymentCoordinatorInstanceId = (HiCR::Instance::instanceId_t) _rpcEngine->getRPCRequester()->getId();
      // printf("[Instance %lu] I am the coordinator of partition %lu at deployPartitionCoordinator() - my deployment coordinator is: %lu\n", _instanceId, _partitionIdx, _deploymentCoordinatorInstanceId);

      // Send request RPC
      _rpcEngine->requestRPC(_deploymentCoordinatorInstanceId, __HLLM_REQUEST_DEPLOYMENT_CONFIGURATION_RPC_NAME);

      // Wait for serialized information
      auto returnValue = _rpcEngine->getReturnValue();

      // Receiving raw serialized topology information from the worker
      std::string deploymentString = (char *)returnValue->getPointer();

      // Parsing serialized raw topology into a json object
      auto deploymentJs = nlohmann::json::parse(deploymentString);

      // Creating the deployment object from the json
      _deployment = configuration::Deployment(deploymentJs);

      // Getting my partition id
      _partitionIdx = 0;

      // Freeing return value
      _rpcEngine->getMemoryManager()->freeLocalMemorySlot(returnValue);

      // Deploying partition
      commonEntryPoint();
  }

  __INLINE__ void commonEntryPoint()
  {
    printf("[Instance %lu] At Common Entry Point\n", _instanceId);

    size_t myPartitionIndex = 0;
    size_t myReplicaIndex = 0;
    bool isPartitionCoordinator = false;
    bool isPartitionReplica = false;

    // Perusing the deployment to see what my role(s) is(are)
    for (size_t pIdx = 0; pIdx < _deployment.getPartitions().size(); pIdx++)
    {
      // Getting partition object
      const auto partition = _deployment.getPartitions()[pIdx];

      // Getting the instance id assigned to the partition
      const auto partitionCoordinatorInstanceId = partition->getInstanceId();

      // If I am a partition coordinator, mark it now
      if (_instanceId == partitionCoordinatorInstanceId)
      {
        isPartitionCoordinator = true;
        myPartitionIndex = pIdx;
      } 

      // Now getting all replicas for the replica
      for (size_t rIdx = 0; rIdx < partition->getReplicas().size(); rIdx++)
      {
        // Getting the instance id assigned to the replica
        const auto replicaInstanceId = partition->getReplicas()[rIdx]->getInstanceId();

        // If I am a partition 
        if (_instanceId == replicaInstanceId)
        {
          isPartitionReplica = true;
          myReplicaIndex = rIdx;
        } 
      }
    }

    if (isPartitionCoordinator == true)
    {
      printf("[Instance %lu] I am a partition %lu coordinator\n", _instanceId, myPartitionIndex);
    }

    if (isPartitionReplica == true)
    {
      printf("[Instance %lu] I am a partition %lu replica %lu\n", _instanceId, myPartitionIndex, myReplicaIndex);
    }
  }

  __INLINE__ void deployPartitionCoordinator()
  {
    // Creating partition coordinator object -- it will live throughout the deployment time
    PartitionCoordinator pc(_deployment, _partitionIdx);

    // Deploy coordinator
    pc.deploy();
  }

    // // Starting a new deployment
    // _continueRunning = true;

    // // Initializing TaskR
    // _taskr->initialize();

    // // Getting relevant information
    // const auto &executionGraph = hicr::json::getArray<nlohmann::json>(partitionConfig, "Execution Graph");

    // // Creating general TaskR function for all execution graph functions
    // _taskrFunction = std::make_unique<taskr::Function>([this](taskr::Task *task) { runTaskRFunction(task); });

    // // Checking the execution graph functions have been registered
    // taskr::taskId_t taskrLabelCounter = 0;
    // for (const auto &functionJs : executionGraph)
    // {
    //   const auto &fcName = hicr::json::getString(functionJs, "Name");

    //   // Checking the requested function was registered
    //   if (_registeredFunctions.contains(fcName) == false)
    //   {
    //     fprintf(stderr, "The requested function name '%s' is not registered. Please register it before running the hLLM.\n", fcName.c_str());
    //     abort();
    //   }

    //   // Getting inputs and outputs
    //   std::unordered_map<std::string, channel::Producer> producers;
    //   std::unordered_map<std::string, channel::Consumer> consumers;

    //   const auto &dependencies = hicr::json::getObject(_config, "Dependencies");

    //   const auto &inputsJs = hicr::json::getArray<nlohmann::json>(functionJs, "Inputs");
    //   for (const auto &inputJs : inputsJs)
    //   {
    //     const auto &inputName            = hicr::json::getString(inputJs, "Name");
    //     const auto &dependency           = hicr::json::getObject(dependencies, inputName);
    //     const auto &dependencyTypeString = hicr::json::getString(dependency, "Type");

    //     // Determine the dependency type
    //     auto                               dependencyType = channel::dependencyType::buffered;
    //     HiCR::MemoryManager               *memoryManager  = _bufferedMemoryManager;
    //     std::shared_ptr<HiCR::MemorySpace> memorySpace    = _bufferedMemorySpace;
    //     if (dependencyTypeString == "Unbuffered")
    //     {
    //       dependencyType = channel::dependencyType::unbuffered;
    //       memoryManager  = _unbufferedMemoryManager;
    //       memorySpace    = _unbufferedMemorySpace;
    //     }

    //     consumers.try_emplace(inputName, inputName, memoryManager, memorySpace, dependencyType, moveConsumer(inputName));
    //   }

    //   const auto &outputsJs = hicr::json::getArray<std::string>(functionJs, "Outputs");
    //   for (const auto &outputName : outputsJs)
    //   {
    //     const auto &dependency           = hicr::json::getObject(dependencies, outputName);
    //     const auto &dependencyTypeString = hicr::json::getString(dependency, "Type");

    //     // Determine the dependency type
    //     auto                               dependencyType = channel::dependencyType::buffered;
    //     HiCR::MemoryManager               *memoryManager  = _bufferedMemoryManager;
    //     std::shared_ptr<HiCR::MemorySpace> memorySpace    = _bufferedMemorySpace;
    //     if (dependencyTypeString == "Unbuffered")
    //     {
    //       dependencyType = channel::dependencyType::unbuffered;
    //       memoryManager  = _unbufferedMemoryManager;
    //       memorySpace    = _unbufferedMemorySpace;
    //     }
    //     producers.try_emplace(outputName, outputName, memoryManager, memorySpace, dependencyType, moveProducer(outputName));
    //   }

    //   // Getting label for taskr function
    //   const auto taskId = taskrLabelCounter++;

    //   // Creating taskr Task corresponding to the LLM engine task
    //   auto taskrTask = std::make_unique<taskr::Task>(taskId, _taskrFunction.get());

    //   // Getting function pointer
    //   const auto &fc = _registeredFunctions[fcName];

    //   // Creating Engine Task object
    //   auto newTask = std::make_shared<hLLM::Task>(fcName, fc, std::move(consumers), std::move(producers), std::move(taskrTask));

    //   // Adding task to the label->Task map
    //   _taskLabelMap.insert({taskId, newTask});

    //   // Adding task to TaskR itself
    //   _taskr->addTask(newTask->getTaskRTask());
    // }

    // // Instruct TaskR to re-add suspended tasks
    // _taskr->setTaskCallbackHandler(HiCR::tasking::Task::callback_t::onTaskSuspend, [&](taskr::Task *task) { _taskr->resumeTask(task); });

    // // Setting service to listen for incoming administrative messages
    // std::function<void()> RPCListeningService = [this]() {
    //   if (_rpcEngine->hasPendingRPCs()) _rpcEngine->listen();
    // };
    // _taskr->addService(&RPCListeningService);

    // // Running TaskR
    // _taskr->run();
    // _taskr->await();
    // _taskr->finalize();

    // printf("[hLLM] Instance '%s' TaskR Stopped\n", _partitionName.c_str());
  // }

  // For every new partition instance created, we send it the serialized deployment configuration
  __INLINE__ void attendDeploymentConfigurationRequest()
  {
      // Serializing
      const auto serializedDeployment = _deployment.serialize().dump();

      // Returning serialized topology
      _rpcEngine->submitReturnValue((void *)serializedDeployment.c_str(), serializedDeployment.size() + 1);
  }

  __INLINE__ void runTaskRFunction(taskr::Task *task)
  {
    const auto &taskId       = task->getTaskId();
    const auto &taskObject   = _taskLabelMap.at(taskId);
    const auto &function     = taskObject->getFunction();
    const auto &functionName = taskObject->getName();

    // Function to check for pending operations
    auto pendingOperationsCheck = [&]() {
      // If execution must stop now, return true to go back to the task and finish it
      if (_continueRunning == false) return true;

      // The task is not ready if any of its inputs are not yet available
      if (taskObject->checkInputsReadiness() == false) return false;

      // The task is not ready if any of its output channels are still full
      if (taskObject->checkOutputsReadiness() == false) return false;

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

      // Inputs dependencies must be satisfied by now; getting them values
      taskObject->setInputs();

      // Actually run the function now
      function(taskObject.get());

      // Another exit point (the more the better)
      if (_continueRunning == false) break;

      // Checking input messages were properly consumed and popping the used token
      taskObject->popInputs();

      // Validating and pushing output messages
      taskObject->pushOutputs();

      // Clearing output token maps
      taskObject->clearOutputs();
    }
  }

  // A system-wide flag indicating that we should continue executing
  bool _continueRunning;

  // Storage for the partition index within the deployment corresponding to this instance
  size_t _partitionIdx;

  // The main driver for taskr
  std::unique_ptr<taskr::Function> _taskrFunction;

  /// A map of registered functions, targets for an partition's initial function
  std::map<std::string, function_t> _registeredFunctions;

  // Copy of the initial deployment configuration
  configuration::Deployment _deployment;

  // Map relating task labels to their hLLM task
  std::map<taskr::taskId_t, std::shared_ptr<hLLM::Task>> _taskLabelMap;

  // The instance manager to use for creating / relinquishing  replicas
  HiCR::InstanceManager *const _instanceManager;

  // Pointer to the HiCR RPC Engine
  HiCR::frontend::RPCEngine *_rpcEngine;

  // TaskR instance
  taskr::Runtime* const _taskr;

  // My instance Id
  const HiCR::Instance::instanceId_t _instanceId;

  // HiCR instance id associated to the instance that will coordinate the deployment
  HiCR::Instance::instanceId_t _deploymentCoordinatorInstanceId;
}; // class Engine

} // namespace hLLM

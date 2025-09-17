#pragma once

#include <ranges>
#include <unordered_map>

#include <hicr/core/exceptions.hpp>
#include <hicr/core/definitions.hpp>
#include <deployr/deployr.hpp>
#include <taskr/taskr.hpp>
#include <hicr/backends/hwloc/topologyManager.hpp>
#include <hicr/backends/boost/computeManager.hpp>
#include <hicr/backends/pthreads/computeManager.hpp>

#include "configuration/deployment.hpp"
#include "task.hpp"

#define __HLLM_ENTRY_POINT_RPC_NAME "[hLLM] Entry Point"
#define __HLLM_ENTRY_REQUEST_STOP_RPC_NAME "[hLLM] Request Stop"
#define __HLLM_ENTRY_STOP_RPC_NAME "[hLLM] Stop"


  // Name of the hLLM entry point after deployment
  const std::string _entryPointName       = "__hLLM Entry Point__";
  const std::string _requestDeploymentConfiguration = "__hLLM Request Deployment Configuration__";
  const std::string _broadcastStopRPCName = "__hLLM Broadcast Stop__";
  const std::string _requestStopRPCName   = "__hLLM Request Stop__";

namespace hLLM
{

typedef uint64_t partitionId_t;

class Engine final
{
  public:

  Engine(HiCR::InstanceManager             *instanceManager,
         HiCR::CommunicationManager        *bufferedCommunicationManager,
         HiCR::CommunicationManager        *unbufferedCommunicationManager,
         HiCR::MemoryManager               *bufferedMemoryManager,
         HiCR::MemoryManager               *unbufferedMemoryManager,
         HiCR::frontend::RPCEngine         *rpcEngine,
         std::shared_ptr<HiCR::MemorySpace> bufferedMemorySpace,
         std::shared_ptr<HiCR::MemorySpace> unbufferedMemorySpace,
         const HiCR::Topology              &localTopology)
    : _instanceManager(instanceManager),
      _bufferedCommunicationManager(bufferedCommunicationManager),
      _unbufferedCommunicationManager(unbufferedCommunicationManager),
      _bufferedMemoryManager(bufferedMemoryManager),
      _unbufferedMemoryManager(unbufferedMemoryManager),
      _rpcEngine(rpcEngine),
      _bufferedMemorySpace(bufferedMemorySpace),
      _unbufferedMemorySpace(unbufferedMemorySpace)
  {
    // Registering entry point function 
    _rpcEngine->addRPCTarget(_entryPointName, HiCR::backend::pthreads::ComputeManager::createExecutionUnit([this](void*) { nonCoordinatorEntryPoint(); }));

    // Registering deployment information request
    _rpcEngine->addRPCTarget(_requestDeploymentConfiguration, HiCR::backend::pthreads::ComputeManager::createExecutionUnit([this](void *) { attendDeploymentConfigurationRequest(); }));

    // Registering finalization function (for root to execute)
    _rpcEngine->addRPCTarget(_broadcastStopRPCName, HiCR::backend::pthreads::ComputeManager::createExecutionUnit([this](void *) { doLocalTermination(); }));

    // Registering finalization request function (for non-root to request roots to end the entire execution)
    _rpcEngine->addRPCTarget(_requestStopRPCName, HiCR::backend::pthreads::ComputeManager::createExecutionUnit([this](void *) { broadcastTermination(); }));
  }

  ~Engine() = default;

  /**
   * The instances that call this function will wait for the deployment to start
   */
  __INLINE__ void awaitDeployment()
  {
    _rpcEngine->listen();
  }

  __INLINE__ void deploy(const configuration::Deployment deployment)
  {
    // Establishing myself as coordinator instance
    _coordinatorInstanceId = _instanceManager->getCurrentInstance()->getId();

    // Making a copy of the deployment object
    _deployment = deployment;

    // Check if I (coordinator) am assigned any partitions
    bool isAssignedPartition = false;

    // Launch each one of the partitions, indicating by argument which partition Id they are
    for (const auto& partition : _deployment.getPartitions())
    {
      // Getting the partition id
      const auto partitionId = partition->getId();

      // Getting the instance id assigned to the partition
      const auto partitionInstanceId = partition->getInstanceId();

      // If it's not me, then request the partition to start executing (non-coordinator entry point)
      if (partitionInstanceId != _coordinatorInstanceId)
      {
        // Requesting them to enter the entry point
        _rpcEngine->requestRPC(partitionInstanceId, _entryPointName, partitionId);

        // Listening for a deployment configuration request
        _rpcEngine->listen();
      }

      // If it's me, I am part of the execution so remember my partition id
      if (partitionInstanceId == _coordinatorInstanceId)
      {
        isAssignedPartition = true;
        _partitionId = partitionId;
      }
    }

    // If I am part of the execution, jump straight into the partition entry point
    partitionEntryPoint();
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
    if (currentInstance.getId() == _coordinatorInstanceId) broadcastTermination();

    // If I am not the deployer instance, request the deployer to please broadcast terminationp
    printf("[hLLM] Instance %lu requesting deployer instance %lu to finish execution.\n", currentInstance.getId(), _coordinatorInstanceId);
    _rpcEngine->requestRPC(_coordinatorInstanceId, _requestStopRPCName);
  }

  __INLINE__ void finalize()
  {
    // Finished execution, then finish deployment
    const auto &currentInstance = *_instanceManager->getCurrentInstance();
    printf("[hLLM] Instance %lu finalizing deployr.\n", currentInstance.getId());
  }

  private:

  //////////////////////////////// Configuration parsing

  /**
   * Validate the configuration
   * 
   * @param[in] config hLLM configuration
  */
  __INLINE__ static void validateConfiguration(const nlohmann::json_abi_v3_11_2::json &config);

  /**
   * Parse dependencies. Contains information on the dependencies to be created
   * 
   * @param instanceJs Instance information in JSON format
   * 
   * @return a map with the dependencies information in JSON format 
  */
  __INLINE__ std::unordered_map<std::string, nlohmann::json> parseDependencies(const std::vector<nlohmann::json> &instancesJS);

  //////////////////////////////// Channels management

  /**
   * Retrieves one of the producer channels creates during deployment.
   * 
   * If the provided name is not registered for this partition, the function will produce an exception.
   * 
   * @param[in] name The channel name
   * @return The requested producer
   */
  __INLINE__ std::unique_ptr<channel::channelProducerInterface_t> moveProducer(const std::string &name);

  /**
   * Retrieves one of the consumer channels creates during deployment.
   * 
   * If the provided name is not registered for this partition, the function will produce an exception.
   * 
   * @param[in] name The channel name
   * @return The requested consumer
   */
  __INLINE__ std::unique_ptr<channel::channelConsumerInterface_t> moveConsumer(const std::string &name);

  /**
  * Function to create all the channels based on the previously passed configuration
  * */
  __INLINE__ void createDependencies();

  __INLINE__ void doLocalTermination()
  {
    printf("[hLLM] Instance %lu terminating TaskR...\n", _coordinatorInstanceId);
    // Stopping the execution of the current and new tasks
    _continueRunning = false;
    _taskr->forceTermination();
    printf("[hLLM] Instance %lu terminated TaskR.\n", _coordinatorInstanceId);
  }

  __INLINE__ void broadcastTermination()
  {
    const auto &currentInstance = *_instanceManager->getCurrentInstance();

    if (currentInstance.getId() != _coordinatorInstanceId) HICR_THROW_RUNTIME("Only the deployer instance %lu can broadcast termination", _coordinatorInstanceId);

    // Send a finalization signal to all other non-root instances
    auto &instances = _instanceManager->getInstances();
    for (auto &instance : instances)
      if (instance->getId() != _coordinatorInstanceId)
      {
        printf("[hLLM] Instance %lu sending stop RPC to instance %lu.\n", _coordinatorInstanceId, instance->getId());
        _rpcEngine->requestRPC(instance->getId(), _broadcastStopRPCName);
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

    // Extract dependencies
    const auto &dependencies = parseDependencies(partitionsJs);

    // Add dependencies to request
    parsedConfig["Dependencies"] = dependencies;

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

  __INLINE__ void nonCoordinatorEntryPoint()
  {
      // Getting my instance id from the instance manager
      _instanceId = _instanceManager->getCurrentInstance()->getId();

      // Getting my partition id from the RPC argument
      _partitionId = (configuration::Partition::partitionId_t) _rpcEngine->getRPCArgument();
      
      // Getting the instance id of the coordinator instance by the RPC argument
      _coordinatorInstanceId = (HiCR::Instance::instanceId_t) _rpcEngine->getRPCRequester()->getId();

      // Send request RPC
      _rpcEngine->requestRPC(_coordinatorInstanceId, _requestDeploymentConfiguration);

      // Wait for serialized information
      auto returnValue = _rpcEngine->getReturnValue();

      // Receiving raw serialized topology information from the worker
      std::string deploymentString = (char *)returnValue->getPointer();

      // Parsing serialized raw topology into a json object
      auto deploymentJs = nlohmann::json::parse(deploymentString);

      // Creating the deployment object from the json
      _deployment = configuration::Deployment(deploymentJs);

      // Freeing return value
      _rpcEngine->getMemoryManager()->freeLocalMemorySlot(returnValue);

      // Now enter the partition entry point
      partitionEntryPoint();
  }

  __INLINE__ void partitionEntryPoint()
  {
    printf("Partition %lu - I am at the entry point - Coordinator is: %lu\n", _partitionId, _coordinatorInstanceId);

    // // Create channels
    // createChannels(
    //   *_bufferedCommunicationManager, *_bufferedMemoryManager, _bufferedMemorySpace, *_unbufferedCommunicationManager, *_unbufferedMemoryManager, _unbufferedMemorySpace);

    // // Starting a new deployment
    // _continueRunning = true;

    // // Creating HWloc topology object
    // hwloc_topology_t topology;

    // // initializing hwloc topology object
    // hwloc_topology_init(&topology);

    // // Initializing HWLoc-based host (CPU) topology manager
    // HiCR::backend::hwloc::TopologyManager tm(&topology);

    // // Asking backend to check the available devices
    // const auto t = tm.queryTopology();

    // // Compute resources to use
    // HiCR::Device::computeResourceList_t computeResources;

    // // Considering all compute devices of NUMA Domain type
    // const auto &devices = t.getDevices();
    // for (const auto &device : devices)
    //   if (device->getType() == "NUMA Domain")
    //   {
    //     // Getting compute resources in this device
    //     auto crs = device->getComputeResourceList();

    //     // Adding it to the list
    //     for (const auto &cr : crs) computeResources.push_back(cr);
    //   }

    // // Freeing up memory
    // hwloc_topology_destroy(topology);

    // // Creating taskr object
    // const auto &taskRConfig = hicr::json::getObject(_config, "TaskR Configuration");
    // _taskr                  = std::make_unique<taskr::Runtime>(&_boostComputeManager, &_pthreadsComputeManager, computeResources, taskRConfig);

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
  }

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

  // The instance id assigned to this instance
  HiCR::Instance::instanceId_t _instanceId;

  // The partition id assigned to this instance
  configuration::Partition::partitionId_t _partitionId;

  std::unique_ptr<taskr::Function> _taskrFunction;

  /// A map of registered functions, targets for an partition's initial function
  std::map<std::string, function_t> _registeredFunctions;

  // Copy of the initial deployment configuration
  configuration::Deployment _deployment;

  // TaskR instance
  std::unique_ptr<taskr::Runtime> _taskr;

  // Map relating task labels to their hLLM task
  std::map<taskr::taskId_t, std::shared_ptr<hLLM::Task>> _taskLabelMap;

  HiCR::InstanceManager *const      _instanceManager;
  HiCR::CommunicationManager *const _bufferedCommunicationManager;
  HiCR::CommunicationManager *const _unbufferedCommunicationManager;
  HiCR::MemoryManager *const        _bufferedMemoryManager;
  HiCR::MemoryManager *const        _unbufferedMemoryManager;

  // Pointer to the HiCR RPC Engine
  HiCR::frontend::RPCEngine *_rpcEngine;
  std::shared_ptr<HiCR::MemorySpace> _bufferedMemorySpace;
  std::shared_ptr<HiCR::MemorySpace> _unbufferedMemorySpace;

  ///// HiCR Objects for TaskR

  // Initializing Boost-based compute manager to instantiate suspendable coroutines
  HiCR::backend::boost::ComputeManager _boostComputeManager;

  // Initializing Pthreads-based compute manager to instantiate processing units
  HiCR::backend::pthreads::ComputeManager _pthreadsComputeManager;

  /// Map of created producers and consumers
  std::unordered_map<std::string, std::unique_ptr<channel::channelConsumerInterface_t>> _consumers;
  std::unordered_map<std::string, std::unique_ptr<channel::channelProducerInterface_t>> _producers;

  // Global topology
  std::vector<HiCR::Topology> _globalTopology;

  // HiCR instance id associated to the instance that will coordinate the deployment
  HiCR::Instance::instanceId_t _coordinatorInstanceId;
}; // class Engine

} // namespace hLLM

// #include "channelCreationImpl.hpp"
// #include "parseConfigImpl.hpp"
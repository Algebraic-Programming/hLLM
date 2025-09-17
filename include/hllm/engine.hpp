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

#include "task.hpp"

#define __HLLM_BROADCAST_CONFIGURATION "[hLLM] Broadcast Configuration"

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
    : _deployr(instanceManager, rpcEngine, localTopology),
      _instanceManager(instanceManager),
      _bufferedCommunicationManager(bufferedCommunicationManager),
      _unbufferedCommunicationManager(unbufferedCommunicationManager),
      _bufferedMemoryManager(bufferedMemoryManager),
      _unbufferedMemoryManager(unbufferedMemoryManager),
      _rpcEngine(rpcEngine),
      _bufferedMemorySpace(bufferedMemorySpace),
      _unbufferedMemorySpace(unbufferedMemorySpace)
  {}

  ~Engine() {}

  __INLINE__ void initialize(HiCR::Instance::instanceId_t deployerInstanceId)
  {
    // Store the id of the coordinator partition
    _deployerInstanceId = deployerInstanceId;

    // Registering entry point function in DeployR
    _deployr.registerFunction(_entryPointName, [this]() { entryPoint(); });

    // Initializing DeployR
    _deployr.initialize();
  }

  __INLINE__ void deploy(const nlohmann::json &config)
  {
    // Validate configuration
    validateConfiguration(config);

    // Storing configuration
    _config = parseConfiguration(config);

    // Get global Topology
    gatherGlobalTopology();

    // Broadcast the global topology to everyone else
    // broadcastGlobalTopology();

    // Sort partitions according to the partition id that serves as a priority
    sortPartitions(_config["Partitions"], "Partition ID");

    // Deploying
    _deploy(hicr::json::getArray<nlohmann::json>(_config, "Partitions"));
  }

  __INLINE__ void abort() { _deployr.abort(); }

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

  __INLINE__ void terminate()
  {
    const auto &currentInstance = _deployr.getCurrentHiCRInstance();

    // If I am the deployer instance, broadcast termination directly
    if (currentInstance.getId() == _deployerInstanceId) broadcastTermination();

    // If I am not the deployer instance, request the deployer to please broadcast terminationp
    printf("[hLLM] Instance %lu requesting deployer instance %lu to finish execution.\n", currentInstance.getId(), _deployerInstanceId);
    _rpcEngine->requestRPC(_deployerInstanceId, _requestStopRPCName);
  }

  __INLINE__ void finalize()
  {
    // Finished execution, then finish deployment
    const auto &currentInstance = _deployr.getCurrentHiCRInstance();
    printf("[hLLM] Instance %lu finalizing deployr.\n", currentInstance.getId());
    _deployr.finalize();
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
  void createChannels(HiCR::CommunicationManager         &bufferedCommunicationManager,
                      HiCR::MemoryManager                &bufferedMemoryManager,
                      std::shared_ptr<HiCR::MemorySpace> &bufferedMemorySpace,
                      HiCR::CommunicationManager         &unbufferedCommunicationManager,
                      HiCR::MemoryManager                &unbufferedMemoryManager,
                      std::shared_ptr<HiCR::MemorySpace> &unbufferedMemorySpace);

  /**
      * Function to create a variable-sized token locking channel between N producers and 1 consumer
      *
      * @note This is a collective operation. All instances must participate in this call, even if they don't play a producer or consumer role
      *
      * @param[in] channelTag The unique identifier for the channel. This tag should be unique for each channel
      * @param[in] channelName The name of the channel. This will be the identifier used to retrieve the channel
      * @param[in] isProducer whether a producer should be created
      * @param[in] isConsumer whether a consumer should be created
      * @param[in] bufferCapacity The number of tokens that can be simultaneously held in the channel's buffer
      * @param[in] bufferSize The size (bytes) of the buffer.
      * @return A shared pointer of the newly created channel
      */
  __INLINE__ std::pair<std::unique_ptr<channel::channelProducerInterface_t>, std::unique_ptr<channel::channelConsumerInterface_t>> createChannel(
    const size_t                        channelTag,
    const std::string                   channelName,
    const bool                          isProducer,
    const bool                          isConsumer,
    HiCR::CommunicationManager         &communicationManager,
    HiCR::MemoryManager                &memoryManager,
    std::shared_ptr<HiCR::MemorySpace> &memorySpace,
    const size_t                        bufferCapacity,
    const size_t                        bufferSize);

  __INLINE__ void doLocalTermination()
  {
    printf("[hLLM] Instance %lu terminating TaskR...\n", _deployerInstanceId);
    // Stopping the execution of the current and new tasks
    _continueRunning = false;
    _taskr->forceTermination();
    printf("[hLLM] Instance %lu terminated TaskR.\n", _deployerInstanceId);
  }

  __INLINE__ void broadcastTermination()
  {
    if (_deployr.getCurrentHiCRInstance().getId() != _deployerInstanceId) HICR_THROW_RUNTIME("Only the deployer instance %lu can broadcast termination", _deployerInstanceId);

    // Send a finalization signal to all other non-root instances
    auto &instances = _instanceManager->getInstances();
    for (auto &instance : instances)
      if (instance->getId() != _deployerInstanceId)
      {
        printf("[hLLM] Instance %lu sending stop RPC to instance %lu.\n", _deployerInstanceId, instance->getId());
        _rpcEngine->requestRPC(instance->getId(), _stopRPCName);
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

  __INLINE__ void sortPartitions(nlohmann::json &partitions, std::string field)
  {
    // Sort according to partition Id
    std::sort(partitions.begin(), partitions.end(), [&](const nlohmann::json &a, const nlohmann::json &b) {
      return hicr::json::getNumber<partitionId_t>(a, field) < hicr::json::getNumber<partitionId_t>(b, field);
    });
  }

  __INLINE__ void gatherGlobalTopology()
  {
    std::vector<HiCR::Instance::instanceId_t> instanceIds;
    for (const auto &instance : _instanceManager->getInstances()) instanceIds.push_back(instance->getId());
    _globalTopology = _deployr.gatherGlobalTopology(_instanceManager->getRootInstanceId(), instanceIds);
  }

  __INLINE__ void _deploy(const std::vector<nlohmann::json> &partitionRequestsJs)
  {
    bool isDeployer = _instanceManager->getCurrentInstance()->getId() == _deployerInstanceId;

    // If am the one designated to coordinate the deployment
    if (isDeployer)
    {
      // Getting requested topologies from the json file
      std::vector<HiCR::Topology> requestedTopologies;

      // Parse the single topologies
      for (const auto &partition : partitionRequestsJs) { requestedTopologies.push_back(HiCR::Topology(partition["Topology"])); }

      // Determine best pairing between the detected instances
      const auto matching = deployr::DeployR::doBipartiteMatching(requestedTopologies, _globalTopology);

      // Check matching
      if (matching.size() != requestedTopologies.size())
      {
        fprintf(stderr, "Error: The provided instances do not have the sufficient hardware resources to run this job.\n");
        abort();
      }

      // Creating the runner objects
      for (size_t i = 0; i < partitionRequestsJs.size(); i++)
      {
        // Get partition data
        const auto &partition = partitionRequestsJs[i];

        // Add runner to deployment
        _deployment.addRunner(deployr::Runner(i, _entryPointName, matching[i]));

        // Populate the partition runner map
        _partitionRunnerIdMap[hicr::json::getNumber<partitionId_t>(partition, "Partition ID")] = i;
      }
    }

    // Deploying request
    try
    {
      _deployr.deploy(_deployment, _deployerInstanceId);
    }
    catch (const std::exception &e)
    {
      fprintf(stderr, "[hLLM] Error: Failed to deploy. Reason:\n + '%s'", e.what());
      _deployr.abort();
    }
  }

#define __HLLM_REQUEST_PARTITION_ID "[hLLM] Request Partition ID"

  void broadcastPartitionId()
  {
    auto storePartitionIdExecutionUnit = HiCR::backend::pthreads::ComputeManager::createExecutionUnit([&](void *) {
      const auto &instance = _rpcEngine->getRPCRequester();
      size_t      rpcRequesterRunnerId;
      for (rpcRequesterRunnerId = 0; rpcRequesterRunnerId < _deployment.getRunners().size(); rpcRequesterRunnerId++)
      {
        auto runnerInstanceId = _deployment.getRunners()[rpcRequesterRunnerId].getInstanceId();
        if (runnerInstanceId == instance->getId()) { break; }
      }

      partitionId_t rpcRequesterPartitionId;

      for (const auto &[partitionId, runnerId] : _partitionRunnerIdMap)
      {
        if (runnerId == rpcRequesterRunnerId)
        {
          rpcRequesterPartitionId = partitionId;
          break;
        }
      }
      _rpcEngine->submitReturnValue(&rpcRequesterPartitionId, sizeof(rpcRequesterPartitionId));
    });
    _rpcEngine->addRPCTarget(__HLLM_REQUEST_PARTITION_ID, storePartitionIdExecutionUnit);

    if (_deployerInstanceId == _instanceManager->getCurrentInstance()->getId())
    {
      // If I am the deployer, search for its own partition id
      for (const auto &[partitionId, runnerId] : _partitionRunnerIdMap)
      {
        if (runnerId == _deployr.getRunnerId())
        {
          _partitionId = partitionId;
        }
      }

      // Wait for incoming rpc from all the other_ =nner_
      for (size_t _ = 0; _ < _deployment.getRunners().size() - 1; _++)
      {
        _rpcEngine->listen();
        printf("listened to request %zu\n", _);
      }

      printf("finished listening\n");
    }
    else
    {
      _rpcEngine->requestRPC(_deployerInstanceId, __HLLM_REQUEST_PARTITION_ID);
      _partitionId = *((partitionId_t *)_rpcEngine->getReturnValue()->getPointer());
    }
  }

  __INLINE__ void entryPoint()
  {
    // Broadcast the partition id to all the instances
    broadcastPartitionId();

    // Finding partition information on the configuration
    const auto    &partitionsJs = hicr::json::getArray<nlohmann::json>(_config, "Partitions");
    nlohmann::json partitionConfig;
    for (const auto &partition : partitionsJs)
    {
      const auto &partitionId = hicr::json::getNumber<partitionId_t>(partition, "Partition ID");
      if (partitionId == _partitionId) partitionConfig = partition;
    }

    // Set partition name
    _partitionName = hicr::json::getString(partitionConfig, "Name");

    // Create channels
    createChannels(
      *_bufferedCommunicationManager, *_bufferedMemoryManager, _bufferedMemorySpace, *_unbufferedCommunicationManager, *_unbufferedMemoryManager, _unbufferedMemorySpace);

    // Starting a new deployment
    _continueRunning = true;

    // Registering finalization function (for root to execute)
    _rpcEngine->addRPCTarget(_stopRPCName, HiCR::backend::pthreads::ComputeManager::createExecutionUnit([this](void *) {
                               printf("[hLLM] Received finalization RPC\n");
                               doLocalTermination();
                             }));

    // Registering finalization request function (for non-root to request roots to end the entire execution)
    _rpcEngine->addRPCTarget(_requestStopRPCName, HiCR::backend::pthreads::ComputeManager::createExecutionUnit([this](void *) {
                               printf("[hLLM] Partition %s - received RPC request to finalize\n", _partitionName.c_str());
                               broadcastTermination();
                             }));

    // Creating HWloc topology object
    hwloc_topology_t topology;

    // initializing hwloc topology object
    hwloc_topology_init(&topology);

    // Initializing HWLoc-based host (CPU) topology manager
    HiCR::backend::hwloc::TopologyManager tm(&topology);

    // Asking backend to check the available devices
    const auto t = tm.queryTopology();

    // Compute resources to use
    HiCR::Device::computeResourceList_t computeResources;

    // Considering all compute devices of NUMA Domain type
    const auto &devices = t.getDevices();
    for (const auto &device : devices)
      if (device->getType() == "NUMA Domain")
      {
        // Getting compute resources in this device
        auto crs = device->getComputeResourceList();

        // Adding it to the list
        for (const auto &cr : crs) computeResources.push_back(cr);
      }

    // Freeing up memory
    hwloc_topology_destroy(topology);

    // Creating taskr object
    const auto &taskRConfig = hicr::json::getObject(_config, "TaskR Configuration");
    _taskr                  = std::make_unique<taskr::Runtime>(&_boostComputeManager, &_pthreadsComputeManager, computeResources, taskRConfig);

    // Initializing TaskR
    _taskr->initialize();

    // Getting relevant information
    const auto &executionGraph = hicr::json::getArray<nlohmann::json>(partitionConfig, "Execution Graph");

    // Creating general TaskR function for all execution graph functions
    _taskrFunction = std::make_unique<taskr::Function>([this](taskr::Task *task) { runTaskRFunction(task); });

    // Checking the execution graph functions have been registered
    taskr::taskId_t taskrLabelCounter = 0;
    for (const auto &functionJs : executionGraph)
    {
      const auto &fcName = hicr::json::getString(functionJs, "Name");

      // Checking the requested function was registered
      if (_registeredFunctions.contains(fcName) == false)
      {
        fprintf(stderr, "The requested function name '%s' is not registered. Please register it before running the hLLM.\n", fcName.c_str());
        abort();
      }

      // Getting inputs and outputs
      std::unordered_map<std::string, channel::Producer> producers;
      std::unordered_map<std::string, channel::Consumer> consumers;

      const auto &dependencies = hicr::json::getObject(_config, "Dependencies");

      const auto &inputsJs = hicr::json::getArray<nlohmann::json>(functionJs, "Inputs");
      for (const auto &inputJs : inputsJs)
      {
        const auto &inputName            = hicr::json::getString(inputJs, "Name");
        const auto &dependency           = hicr::json::getObject(dependencies, inputName);
        const auto &dependencyTypeString = hicr::json::getString(dependency, "Type");

        // Determine the dependency type
        auto                               dependencyType = channel::dependencyType::buffered;
        HiCR::MemoryManager               *memoryManager  = _bufferedMemoryManager;
        std::shared_ptr<HiCR::MemorySpace> memorySpace    = _bufferedMemorySpace;
        if (dependencyTypeString == "Unbuffered")
        {
          dependencyType = channel::dependencyType::unbuffered;
          memoryManager  = _unbufferedMemoryManager;
          memorySpace    = _unbufferedMemorySpace;
        }

        consumers.try_emplace(inputName, inputName, memoryManager, memorySpace, dependencyType, moveConsumer(inputName));
      }

      const auto &outputsJs = hicr::json::getArray<std::string>(functionJs, "Outputs");
      for (const auto &outputName : outputsJs)
      {
        const auto &dependency           = hicr::json::getObject(dependencies, outputName);
        const auto &dependencyTypeString = hicr::json::getString(dependency, "Type");

        // Determine the dependency type
        auto                               dependencyType = channel::dependencyType::buffered;
        HiCR::MemoryManager               *memoryManager  = _bufferedMemoryManager;
        std::shared_ptr<HiCR::MemorySpace> memorySpace    = _bufferedMemorySpace;
        if (dependencyTypeString == "Unbuffered")
        {
          dependencyType = channel::dependencyType::unbuffered;
          memoryManager  = _unbufferedMemoryManager;
          memorySpace    = _unbufferedMemorySpace;
        }
        producers.try_emplace(outputName, outputName, memoryManager, memorySpace, dependencyType, moveProducer(outputName));
      }

      // Getting label for taskr function
      const auto taskId = taskrLabelCounter++;

      // Creating taskr Task corresponding to the LLM engine task
      auto taskrTask = std::make_unique<taskr::Task>(taskId, _taskrFunction.get());

      // Getting function pointer
      const auto &fc = _registeredFunctions[fcName];

      // Creating Engine Task object
      auto newTask = std::make_shared<hLLM::Task>(fcName, fc, std::move(consumers), std::move(producers), std::move(taskrTask));

      // Adding task to the label->Task map
      _taskLabelMap.insert({taskId, newTask});

      // Adding task to TaskR itself
      _taskr->addTask(newTask->getTaskRTask());
    }

    // Instruct TaskR to re-add suspended tasks
    _taskr->setTaskCallbackHandler(HiCR::tasking::Task::callback_t::onTaskSuspend, [&](taskr::Task *task) { _taskr->resumeTask(task); });

    // Setting service to listen for incoming administrative messages
    std::function<void()> RPCListeningService = [this]() {
      if (_rpcEngine->hasPendingRPCs()) _rpcEngine->listen();
    };
    _taskr->addService(&RPCListeningService);

    // Running TaskR
    _taskr->run();
    _taskr->await();
    _taskr->finalize();

    printf("[hLLM] Instance '%s' TaskR Stopped\n", _partitionName.c_str());
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

  std::unique_ptr<taskr::Function> _taskrFunction;

  /// A map of registered functions, targets for an partition's initial function
  std::map<std::string, function_t> _registeredFunctions;

  // Copy of the initial configuration file
  nlohmann::json _config;

  // Name of the hLLM entry point after deployment
  const std::string _entryPointName     = "__hLLM Entry Point__";
  const std::string _stopRPCName        = "__hLLM Stop__";
  const std::string _requestStopRPCName = "__LLM Request Engine Stop__";

  // DeployR instance
  deployr::DeployR _deployr;

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

  std::string   _partitionName;
  partitionId_t _partitionId;

  // Creating deployment object
  deployr::Deployment _deployment;

  // HiCR instance id associated to the instance that will coordinate the deployment
  HiCR::Instance::instanceId_t _deployerInstanceId;

  std::map<partitionId_t, deployr::Runner::runnerId_t> _partitionRunnerIdMap;
}; // class Engine

} // namespace hLLM

#include "channelCreationImpl.hpp"
#include "parseConfigImpl.hpp"
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

class Engine final
{
  public:

  Engine(HiCR::InstanceManager             *instanceManager,
         HiCR::CommunicationManager        *communicationManager,
         HiCR::MemoryManager               *memoryManager,
         HiCR::frontend::RPCEngine         *rpcEngine,
         std::shared_ptr<HiCR::MemorySpace> channelMemSpace)
    : _deployr(instanceManager, communicationManager, memoryManager, rpcEngine),
      _instanceManager(instanceManager),
      _communicationManager(communicationManager),
      _memoryManager(memoryManager),
      _rpcEngine(rpcEngine),
      _channelMemSpace(channelMemSpace)
  {}

  ~Engine() {}

  __INLINE__ bool initialize()
  {
    // Registering entry point function in DeployR
    _deployr.registerFunction(_entryPointName, [this]() { entryPoint(); });

    // Register broadcast configuration function in hLLM
    auto broadcastConfigurationExecutionUnit = HiCR::backend::pthreads::ComputeManager::createExecutionUnit([this](void *) {
      // Serializing configuration
      const auto serializedConfiguration = _config.dump();

      // Returning serialized configuration
      _rpcEngine->submitReturnValue((void *)serializedConfiguration.c_str(), serializedConfiguration.size() + 1);
    });
    _rpcEngine->addRPCTarget(__HLLM_BROADCAST_CONFIGURATION, broadcastConfigurationExecutionUnit);

    // Initializing DeployR
    _deployr.initialize();

    // Return whether the current instance is root
    return _deployr.getCurrentInstance().isRootInstance();
  }

  __INLINE__ void deploy(const nlohmann::json &config)
  {
    // Storing configuration
    _config = config;

    // Deploying
    _deploy(_config);
  }

  __INLINE__ void run()
  {
    // Broadcast the config to all instances
    broadcastConfiguration();

    // Create channels
    createChannels();

    // Start computation
    _deployr.runInitialFunction();
  }

  __INLINE__ void abort() { _deployr.abort(); }

  /**
   * Registers a function that can be a target as initial function for one or more requested instances.
   * 
   * If a requested initial function is not registered by this function, deployment will fail.
   * 
   * @param[in] functionName The name of the function to register. This value will be used to match against the requested instance functions
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
    const auto &currentInstance = _deployr.getCurrentInstance();
    auto       &rootInstance    = _deployr.getRootInstance();

    // If I am the root instance, broadcast termination directly
    if (currentInstance.isRootInstance() == true) broadcastTermination();

    // If I am not the root instance, request the root to please broadcast termination
    if (currentInstance.isRootInstance() == false)
    {
      printf("[hLLM] Instance %lu requesting root instance %lu to finish execution.\n", currentInstance.getId(), rootInstance.getId());
      _rpcEngine->requestRPC(rootInstance, _requestStopRPCName);
    }
  }

  __INLINE__ void finalize()
  {
    // Finished execution, then finish deployment
    const auto &currentInstance = _deployr.getCurrentInstance();
    printf("[hLLM] Instance %lu finalizing deployr.\n", currentInstance.getId());
    _deployr.finalize();
  }

  private:

  //////////////////////////////// Configuration parsing

  /**
   * Parse instance information
   * 
   * @param[in] config hLLM configuration
   * @param[out] requestJs The outoput configuration populated with instance information
  */
  void parseInstances(const nlohmann::json_abi_v3_11_2::json &config, nlohmann::json_abi_v3_11_2::json &requestJs);

  /**
   * Parse dependencies. Contains information on the channels to be created
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
   * If the provided name is not registered for this instance, the function will produce an exception.
   * 
   * @param[in] name The channel name
   * @return The requested producer
   */
  __INLINE__ std::unique_ptr<channel::channelProducerInterface_t> moveProducer(const std::string &name);

  /**
   * Retrieves one of the consumer channels creates during deployment.
   * 
   * If the provided name is not registered for this instance, the function will produce an exception.
   * 
   * @param[in] name The channel name
   * @return The requested consumer
   */
  __INLINE__ std::unique_ptr<channel::channelConsumerInterface_t> moveConsumer(const std::string &name);

  /**
  * Function to create all the channels based on the previously passed configuration
  * */
  void createChannels();

  /**
      * Function to create a variable-sized token locking channel between N producers and 1 consumer
      *
      * @note This is a collective operation. All instances must participate in this call, even if they don't play a producer or consumer role
      *
      * @param[in] channelTag The unique identifier for the channel. This tag should be unique for each channel
      * @param[in] channelName The name of the channel. This will be the identifier used to retrieve the channel
      * @param[in] producerIds Ids of the instances within the HiCR instance list to serve as producers
      * @param[in] consumerId Ids of the instance within the HiCR instance list to serve as consumer
      * @param[in] bufferCapacity The number of tokens that can be simultaneously held in the channel's buffer
      * @param[in] bufferSize The size (bytes) of the buffer.
      * @return A shared pointer of the newly created channel
      */
  __INLINE__ std::pair<std::unique_ptr<channel::channelProducerInterface_t>, std::unique_ptr<channel::channelConsumerInterface_t>> createChannel(const size_t      channelTag,
                                                                                                                                                 const std::string channelName,
                                                                                                                                                 const size_t      producerId,
                                                                                                                                                 const size_t      consumerId,
                                                                                                                                                 const size_t      bufferCapacity,
                                                                                                                                                 const size_t      bufferSize);

  // TODO: not all the instances need the configuration. Find a way to pass channel data to all the other instances
  // TODO: move to main
  __INLINE__ void broadcastConfiguration()
  {
    if (_deployr.getCurrentInstance().isRootInstance() == true)
    {
      // Listen for the incoming RPCs and return the hLLM configuration
      for (size_t i = 0; i < _deployr.getInstances().size() - 1; ++i) _rpcEngine->listen();
    }
    else
    {
      auto &rootInstance = _deployr.getRootInstance();

      // Request the root instance to send the configuration
      _rpcEngine->requestRPC(rootInstance, __HLLM_BROADCAST_CONFIGURATION);

      // Get the return value as a memory slot
      auto returnValue = _rpcEngine->getReturnValue(rootInstance);

      // Receive raw information from root
      std::string serializedConfiguration = static_cast<char *>(returnValue->getPointer());

      // Parse the configuration
      _config = nlohmann::json::parse(serializedConfiguration);

      // Free return value
      _rpcEngine->getMemoryManager()->freeLocalMemorySlot(returnValue);
    }
  }

  __INLINE__ nlohmann::json createDeployrRequest(const nlohmann::json &requestJs)
  {
    // Getting the instance vector
    const auto &instancesJs = hicr::json::getArray<nlohmann::json>(_config, "Instances");

    // Creating deployR initial request
    nlohmann::json deployrRequestJs;
    deployrRequestJs["Name"] = requestJs["Name"];

    // Adding a different host type and instance entries per instance requested
    auto hostTypeArray = nlohmann::json::array();
    auto instanceArray = nlohmann::json::array();
    for (const auto &instance : instancesJs)
    {
      const auto &instanceName = hicr::json::getString(instance, "Name");
      const auto &hostTypeName = instanceName + std::string(" Host Type");

      // Creating host type entry
      nlohmann::json hostType;
      hostType["Name"]                = instanceName + std::string(" Host Type");
      hostType["Topology"]["Devices"] = instance["Host Topology"];
      hostTypeArray.push_back(hostType);

      // Creating instance entry
      nlohmann::json newInstance;
      newInstance["Name"]      = instanceName;
      newInstance["Host Type"] = hostTypeName;
      newInstance["Function"]  = _entryPointName;
      instanceArray.push_back(newInstance);
    }
    deployrRequestJs["Host Types"] = hostTypeArray;
    deployrRequestJs["Instances"]  = instanceArray;
    deployrRequestJs["Channels"]   = requestJs["Channels"];

    return deployrRequestJs;
  }

  __INLINE__ void doLocalTermination()
  {
    // Stopping the execution of the current and new tasks
    _continueRunning = false;
    _taskr->forceTermination();
  }

  __INLINE__ void broadcastTermination()
  {
    if (_deployr.getCurrentInstance().isRootInstance() == false) HICR_THROW_RUNTIME("Only the root instance can broadcast termination");

    // Send a finalization signal to all other non-root instances
    const auto &currentInstance = _deployr.getCurrentInstance();
    auto        instances       = _deployr.getInstances();
    for (auto &instance : instances)
      if (instance->getId() != currentInstance.getId())
      {
        printf("[hLLM] Instance %lu sending stop RPC to instance %lu.\n", instance->getId(), currentInstance.getId());
        _rpcEngine->requestRPC(*instance, _stopRPCName);
      }

    // (Root) Executing local termination myself now
    doLocalTermination();
  }

  __INLINE__ void _deploy(const nlohmann::json &config)
  {
    // Create request json
    nlohmann::json requestJs;

    // Some information can be passed directly
    requestJs["Name"] = config["Name"];

    parseInstances(config, requestJs);

    // Getting the instance vector
    const auto &instancesJs = hicr::json::getArray<nlohmann::json>(config, "Instances");

    // Extract dependencies
    const auto &dependencies = parseDependencies(instancesJs);

    // Add dependencies to request
    requestJs["Channels"] = dependencies;

    // Creating configuration for TaskR
    nlohmann::json taskrConfig;
    taskrConfig["Task Worker Inactivity Time (Ms)"] = 100;   // Suspend workers if a certain time of inactivity elapses
    taskrConfig["Task Suspend Interval Time (Ms)"]  = 100;   // Workers suspend for this time before checking back
    taskrConfig["Minimum Active Task Workers"]      = 1;     // Have at least one worker active at all times
    taskrConfig["Service Worker Count"]             = 1;     // Have one dedicated service workers at all times to listen for incoming messages
    taskrConfig["Make Task Workers Run Services"]   = false; // Workers will check for meta messages in between executions

    // Creating deployR request object
    auto deployrRequest = createDeployrRequest(requestJs);

    // Adding hLLM as metadata in the request object
    deployrRequest["hLLM Configuration"] = _config;

    // Adding Deployr configuration
    // deployrRequest["DeployR Configuration"] = deployrConfig;

    // Adding taskr configuration
    deployrRequest["TaskR Configuration"] = taskrConfig;

    _config = deployrRequest;

    // Creating request object
    deployr::Request request(deployrRequest);

    // Deploying request
    try
    {
      _deployr.deploy(request);
    }
    catch (const std::exception &e)
    {
      fprintf(stderr, "[hLLM] Error: Failed to deploy. Reason:\n + '%s'", e.what());
      _deployr.abort();
    }
  }

  __INLINE__ void entryPoint()
  {
    // Starting a new deployment
    _continueRunning = true;

    // Registering finalization function (for root to execute)
    _rpcEngine->addRPCTarget(_stopRPCName, HiCR::backend::pthreads::ComputeManager::createExecutionUnit([this](void *) {
                               printf("[hLLM] Received finalization RPC\n");
                               doLocalTermination();
                             }));

    // Registering finalization request function (for non-root to request roots to end the entire execution)
    _rpcEngine->addRPCTarget(_requestStopRPCName, HiCR::backend::pthreads::ComputeManager::createExecutionUnit([this](void *) {
                               const auto &currentInstance = _deployr.getCurrentInstance();
                               printf("[hLLM] Instance (%lu) - received RPC request to finalize\n", currentInstance.getId());
                               broadcastTermination();
                             }));

    // Getting my instance name after deployment
    const auto &myInstance     = _deployr.getLocalInstance();
    const auto &myInstanceName = myInstance.getName();

    // Getting deployment
    const auto &deployment = _deployr.getDeployment();

    // Getting request from deployment
    const auto &request = deployment.getRequest();

    // Getting request configuration from the request ibject
    const auto &requestJs = request.serialize();

    // Getting LLM engine configuration from request configuration
    // TODO: maybe it is better to save the whole request, which holds also the channel informations
    _config = hicr::json::getObject(requestJs, "hLLM Configuration");

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
    const auto &taskRConfig = hicr::json::getObject(requestJs, "TaskR Configuration");
    _taskr                  = std::make_unique<taskr::Runtime>(&_boostComputeManager, &_pthreadsComputeManager, computeResources, taskRConfig);

    // Initializing TaskR
    _taskr->initialize();

    // Finding instance information on the configuration
    const auto    &instancesJs = hicr::json::getArray<nlohmann::json>(_config, "Instances");
    nlohmann::json myInstanceConfig;
    std::string    instanceName;
    for (const auto &instance : instancesJs)
    {
      instanceName = hicr::json::getString(instance, "Name");
      if (instanceName == myInstanceName) myInstanceConfig = instance;
    }

    // Getting relevant information
    const auto &executionGraph = hicr::json::getArray<nlohmann::json>(myInstanceConfig, "Execution Graph");

    // Creating general TaskR function for all execution graph functions
    _taskrFunction = std::make_unique<taskr::Function>([this](taskr::Task *task) { runTaskRFunction(task); });

    // Checking the execution graph functions have been registered
    taskr::label_t taskrLabelCounter = 0;
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

      const auto &channels = hicr::json::getObject(requestJs, "Channels");

      const auto &inputsJs = hicr::json::getArray<nlohmann::json>(functionJs, "Inputs");
      for (const auto &inputJs : inputsJs)
      {
        const auto &inputName            = hicr::json::getString(inputJs, "Name");
        const auto &channel              = hicr::json::getObject(channels, inputName);
        const auto &dependencyTypeString = hicr::json::getString(channel, "Type");

        // Determine the dependency type
        auto dependencyType = channel::buffered;
        if (dependencyTypeString == "Unbuffered") { dependencyType = channel::unbuffered; }

        consumers.try_emplace(inputName, inputName, _memoryManager, _channelMemSpace, dependencyType, moveConsumer(inputName));
      }

      const auto &outputsJs = hicr::json::getArray<std::string>(functionJs, "Outputs");
      for (const auto &outputName : outputsJs)
      {
        const auto &channel              = hicr::json::getObject(channels, outputName);
        const auto &dependencyTypeString = hicr::json::getString(channel, "Type");

        // Determine the dependency type
        auto dependencyType = channel::buffered;
        if (dependencyTypeString == "Unbuffered") { dependencyType = channel::unbuffered; }

        producers.try_emplace(outputName, outputName, _memoryManager, _channelMemSpace, dependencyType, moveProducer(outputName));
      }

      // Getting label for taskr function
      const auto taskLabel = taskrLabelCounter++;

      // Creating taskr Task corresponding to the LLM engine task
      auto taskrTask = std::make_unique<taskr::Task>(_taskrFunction.get());
      taskrTask->setLabel(taskLabel);

      // Getting function pointer
      const auto &fc = _registeredFunctions[fcName];

      // Creating Engine Task object
      auto newTask = std::make_shared<hLLM::Task>(fcName, fc, std::move(consumers), std::move(producers), std::move(taskrTask));

      // Adding task to the label->Task map
      _taskLabelMap.insert({taskLabel, newTask});

      // Adding task to the function name->Task map
      _taskNameMap.insert({fcName, newTask});

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

    printf("[hLLM] Instance '%s' TaskR Stopped\n", myInstanceName.c_str());
  }

  __INLINE__ void runTaskRFunction(taskr::Task *task)
  {
    const auto &taskLabel    = task->getLabel();
    const auto &taskObject   = _taskLabelMap.at(taskLabel);
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

  /// A map of registered functions, targets for an instance's initial function
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
  std::map<taskr::label_t, std::shared_ptr<hLLM::Task>> _taskLabelMap;

  // Map relating function name to their hLLM task
  std::map<std::string, std::shared_ptr<hLLM::Task>> _taskNameMap;

  HiCR::InstanceManager *const      _instanceManager;
  HiCR::CommunicationManager *const _communicationManager;
  HiCR::MemoryManager *const        _memoryManager;
  // Pointer to the HiCR RPC Engine
  HiCR::frontend::RPCEngine *_rpcEngine;

  std::shared_ptr<HiCR::MemorySpace> _channelMemSpace;

  ///// HiCR Objects for TaskR

  // Initializing Boost-based compute manager to instantiate suspendable coroutines
  HiCR::backend::boost::ComputeManager _boostComputeManager;

  // Initializing Pthreads-based compute manager to instantiate processing units
  HiCR::backend::pthreads::ComputeManager _pthreadsComputeManager;

  /// Map of created producers and consumers
  std::unordered_map<std::string, std::unique_ptr<channel::channelConsumerInterface_t>> _consumers;
  std::unordered_map<std::string, std::unique_ptr<channel::channelProducerInterface_t>> _producers;
}; // class Engine

} // namespace hLLM

#include "channelCreationImpl.hpp"
#include "parseConfigImpl.hpp"
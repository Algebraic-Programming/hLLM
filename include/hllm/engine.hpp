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

namespace hLLM
{

class Engine final
{
  public:

  Engine() {}

  ~Engine() {}

  __INLINE__ bool initialize(int *pargc, char ***pargv)
  {
    // Registering entry point function
    _deployr.registerFunction(_entryPointName, [this]() { entryPoint(); });

    // Initializing DeployR
    return _deployr.initialize(pargc, pargv);
  }

  __INLINE__ void run(const nlohmann::json &config)
  {
    // Storing configuration
    _config = config;

    // Deploying
    deploy(_config);
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
    deployrRequestJs["Host Types"]              = hostTypeArray;
    deployrRequestJs["Instances"]               = instanceArray;
    deployrRequestJs["Channels"]                = requestJs["Channels"];
    deployrRequestJs["Unbuffered Dependencies"] = requestJs["Unbuffered Dependencies"];

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

  __INLINE__ void deploy(const nlohmann::json &config)
  {
    // Create request json
    nlohmann::json requestJs;

    // Some information can be passed directly
    requestJs["Name"] = config["Name"];

    parseInstances(config, requestJs);

    // Getting the instance vector
    const auto &instancesJs = hicr::json::getArray<nlohmann::json>(config, "Instances");

    // Extract buffered and unbuffered dependencies
    const auto &[bufferedDependencies, unbufferedDependencies] = splitDependencies(instancesJs);

    // Add buffered and unbuffered dependencies
    auto bufferedDependenciesValues      = bufferedDependencies | std::views::values;
    requestJs["Channels"]                = std::vector<nlohmann::json>(bufferedDependenciesValues.begin(), bufferedDependenciesValues.end());
    requestJs["Unbuffered Dependencies"] = unbufferedDependencies;

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

  void parseInstances(const nlohmann::json_abi_v3_11_2::json &config, nlohmann::json_abi_v3_11_2::json &requestJs)
  {
    ////// Instances information
    std::vector<nlohmann::json> newInstances;
    for (const auto &instance : config["Instances"])
    {
      nlohmann::json newInstance;
      newInstance["Name"]          = instance["Name"];
      newInstance["Host Topology"] = instance["Host Topology"];
      newInstance["Function"]      = _entryPointName;
      newInstances.push_back(newInstance);
    }
    requestJs["Instances"] = newInstances;
  }

  __INLINE__ std::pair<std::map<std::string, nlohmann::json>, std::map<std::string, nlohmann::json>> splitDependencies(const std::vector<nlohmann::json> &instancesJS)
  {
    std::map<std::string, nlohmann::json> bufferedConsumers;
    std::map<std::string, nlohmann::json> unbufferedConsumers;

    // Get all the consumers first since they hold information on the dependency type
    for (const auto &instance : instancesJS)
    {
      const auto &executionGraph = hicr::json::getArray<nlohmann::json>(instance, "Execution Graph");
      for (const auto &function : executionGraph)
      {
        const auto &inputs = hicr::json::getArray<nlohmann::json>(function, "Inputs");
        for (const auto &input : inputs)
        {
          const auto &inputName = hicr::json::getString(input, "Name");
          if (input["Type"] == "Buffered")
          {
            bufferedConsumers[inputName]             = input;
            bufferedConsumers[inputName]["Consumer"] = hicr::json::getString(instance, "Name");
          }
          else if (input["Type"] == "Unbuffered")
          {
            unbufferedConsumers[inputName]             = input;
            unbufferedConsumers[inputName]["Consumer"] = hicr::json::getString(instance, "Name");
          }
          else
          {
            HICR_THROW_RUNTIME("Input %s defined in instance %s type not supported: %s",
                               hicr::json::getString(input, "Name").c_str(),
                               hicr::json::getString(instance, "Name").c_str(),
                               hicr::json::getString(input, "Type").c_str());
          }
        }
      }
    }

    size_t bufferedProducersCount   = 0;
    size_t unbufferedProducersCount = 0;
    for (const auto &instance : instancesJS)
    {
      const auto &executionGraph = hicr::json::getArray<nlohmann::json>(instance, "Execution Graph");
      for (const auto &function : executionGraph)
      {
        const auto &outputs = hicr::json::getArray<nlohmann::json>(function, "Outputs");
        for (const auto &output : outputs)
        {
          if (bufferedConsumers.contains(output))
          {
            bufferedProducersCount++;
            bufferedConsumers[output]["Producers"] = nlohmann::json::array({hicr::json::getString(instance, "Name")});
          }
          else if (unbufferedConsumers.contains(output))
          {
            unbufferedProducersCount++;
            unbufferedConsumers[output]["Producers"] = nlohmann::json::array({hicr::json::getString(instance, "Name")});
          }
          else { HICR_THROW_RUNTIME("Producer %s defined in instance %s has no consumers defined", output.get<std::string>(), instance["Name"]); }
        }
      }
    }

    // Check set equality
    if (bufferedProducersCount != bufferedConsumers.size())
    {
      HICR_THROW_RUNTIME("Inconsistent buffered connections #producers: %zud #consumers: %zu", bufferedProducersCount, bufferedConsumers.size());
    }
    if (unbufferedProducersCount != unbufferedConsumers.size())
    {
      HICR_THROW_RUNTIME("Inconsistent unbuffered connections #producers: %zu #consumers: %zu", unbufferedProducersCount, unbufferedConsumers.size());
    }

    return {bufferedConsumers, unbufferedConsumers};
  }

  __INLINE__ void entryPoint()
  {
    // Starting a new deployment
    _continueRunning = true;

    // Getting pointer to RPC engine
    _rpcEngine = _deployr.getRPCEngine();

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
    _config = hicr::json::getObject(requestJs, "hLLM Configuration");

    // Getting unbuffered dependencies
    const auto &unbufferedDependencies = hicr::json::getObject(requestJs, "Unbuffered Dependencies");

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
    for (const auto &instance : instancesJs)
    {
      const auto &instanceName = hicr::json::getString(instance, "Name");
      if (instanceName == myInstanceName) myInstanceConfig = instance;
    }

    // Initialize unbuffered dependencies access map
    for (const auto &dependency : unbufferedDependencies)
    {
      const auto &dependencyName = hicr::json::getString(dependency, "Name");
      const auto &producers      = hicr::json::getArray<std::string>(dependency, "Producers");
      const auto &consumer       = hicr::json::getString(dependency, "Consumer");

      // TODO: adjust when producer will not be an array anymore
      if (consumer == myInstanceName || producers[0] == myInstanceName) { _unbufferedMemorySlotsAccessMap[dependencyName] = nullptr; }
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
      std::vector<std::pair<std::string, std::string>> inputs;
      const auto                                      &inputsJs = hicr::json::getArray<nlohmann::json>(functionJs, "Inputs");
      for (const auto &inputJs : inputsJs)
      {
        const auto &inputName = hicr::json::getString(inputJs, "Name");
        if (unbufferedDependencies.contains(inputName)) { inputs.push_back({inputName, "Unbuffered"}); }
        else { inputs.push_back({inputName, "Buffered"}); }
      }

      std::vector<std::pair<std::string, std::string>> outputs;
      const auto                                      &outputsJs = hicr::json::getArray<std::string>(functionJs, "Outputs");
      for (const auto &outputName : outputsJs)
      {
        if (unbufferedDependencies.contains(outputName)) { outputs.push_back({outputName, "Unbuffered"}); }
        else { outputs.push_back({outputName, "Buffered"}); }
      }

      // Getting label for taskr function
      const auto taskLabel = taskrLabelCounter++;

      // Creating taskr Task corresponding to the LLM engine task
      auto taskrTask = std::make_unique<taskr::Task>(_taskrFunction.get());
      taskrTask->setLabel(taskLabel);

      // Getting function pointer
      const auto &fc = _registeredFunctions[fcName];

      // Creating Engine Task object
      auto newTask = std::make_shared<hLLM::Task>(fcName, fc, inputs, outputs, std::move(taskrTask));

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

  __INLINE__ static bool isValueNull(const std::unordered_map<std::string, deployr::Channel::token_t *> &collection, const std::string &key)
  {
    return collection.at(key) == nullptr;
  }

  __INLINE__ void runTaskRFunction(taskr::Task *task)
  {
    const auto &taskLabel    = task->getLabel();
    const auto &taskObject   = _taskLabelMap.at(taskLabel);
    const auto &function     = taskObject->getFunction();
    const auto &functionName = taskObject->getName();
    const auto &inputs       = taskObject->getInputs();
    const auto &outputs      = taskObject->getOutputs();

    // Resolving pointers to all the required input channels
    // and gather unbuffered inputs
    std::map<std::string, deployr::Channel *> inputChannels;
    std::vector<std::string>                  unbufferedInputs;
    for (const auto &[input, type] : inputs)
    {
      if (type == "Buffered") { inputChannels[input] = &_deployr.getChannel(input); }
      else { unbufferedInputs.push_back(input); }
    }

    // Resolving pointers to all the required output channels
    // and gather unbuffered outputs
    std::map<std::string, deployr::Channel *> outputChannels;
    std::vector<std::string>                  unbufferedOutputs;
    for (const auto &[output, type] : outputs)
    {
      if (type == "Buffered") { outputChannels[output] = &_deployr.getChannel(output); }
      else { unbufferedOutputs.push_back(output); }
    }

    // Function to check for pending operations
    auto pendingOperationsCheck = [&]() {
      // If execution must stop now, return true to go back to the task and finish it
      if (_continueRunning == false) return true;

      // The task is not ready if any of its inputs are not yet available
      for (const auto &[_, inputChannel] : inputChannels)
        if (inputChannel->isEmpty()) return false;

      // The task is not ready if any of its output channels are still full
      for (const auto &[_, outputChannel] : outputChannels)
        if (outputChannel->isFull()) return false;

      // The task is not ready if any of the unbuffered inputs are not available
      for (const auto &input : unbufferedInputs)
      {
        if (isValueNull(_unbufferedMemorySlotsAccessMap, input) == true) return false;
      }

      // The task is not ready if any of the unbuffered outputs are available to be consumed
      for (const auto &output : unbufferedOutputs)
        if (isValueNull(_unbufferedMemorySlotsAccessMap, output) == false) return false;

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
      for (const auto &[input, type] : inputs)
      {
        deployr::Channel::token_t token;

        if (type == "Buffered")
        {
          // Peeking token from input channel
          token = inputChannels[input]->peek();
        }
        else
        {
          // Get from unbuffered memory
          token = *_unbufferedMemorySlotsAccessMap[input];

          // Set to null the entry in the unbuffered map so new task will wait for the input to become available
          _unbufferedMemorySlotsAccessMap[input] = nullptr;
        }
        // Pushing token onto the task
        taskObject->setInput(input, token);
      }

      // Actually run the function now
      // printf("Running Task: %lu - Function: (%s)\n", taskLabel, functionName.c_str());
      function(taskObject.get());

      // Another exit point (the more the better)
      if (_continueRunning == false) break;

      // Checking input messages were properly consumed and popping the used token
      for (const auto &[input, type] : inputs)
      {
        if (taskObject->hasInput(input) == true)
        {
          fprintf(stderr, "Function '%s' has not consumed required input '%s'\n", functionName.c_str(), input.c_str());
          abort();
        }

        // Popping consumed channel from channel
        if (type == "Buffered") { inputChannels[input]->pop(); }
      }

      // Validating and pushing output messages
      for (const auto &[output, type] : outputs)
      {
        // Checking if output has been produced by the task
        if (taskObject->hasOutput(output) == false)
        {
          fprintf(stderr, "Function '%s' has not pushed required output '%s'\n", functionName.c_str(), output.c_str());
          abort();
        }

        // Now getting output token
        const auto outputToken = taskObject->getOutput(output);

        if (type == "Buffered")
        {
          // Pushing token onto channel
          outputChannels[output]->push(outputToken.buffer, outputToken.size);
        }
        else
        {
          // Set token in unbuffered memory slots
          if (_unbufferedMemorySlotsAccessMap.at(output) != nullptr)
          {
            HICR_THROW_RUNTIME("Output %s not null %s", output, (char *)_unbufferedMemorySlotsAccessMap[output]->buffer);
          }
          _unbufferedMemorySlotsPool[output]      = outputToken;
          _unbufferedMemorySlotsAccessMap[output] = &_unbufferedMemorySlotsPool.at(output);
        }
      }

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

  // Pointer to the HiCR RPC Engine
  HiCR::frontend::RPCEngine *_rpcEngine;

  ///// HiCR Objects for TaskR

  // Initializing Boost-based compute manager to instantiate suspendable coroutines
  HiCR::backend::boost::ComputeManager _boostComputeManager;

  // Initializing Pthreads-based compute manager to instantiate processing units
  HiCR::backend::pthreads::ComputeManager _pthreadsComputeManager;

  // Map storing pointers to access unbuffered memory slots
  std::unordered_map<std::string, deployr::Channel::token_t *> _unbufferedMemorySlotsAccessMap;

  // Map storing pointers to access unbuffered memory slots
  std::unordered_map<std::string, deployr::Channel::token_t> _unbufferedMemorySlotsPool;

}; // class Engine

} // namespace hLLM
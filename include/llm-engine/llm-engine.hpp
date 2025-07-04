
#pragma once

#include <hicr/core/exceptions.hpp>
#include <hicr/core/definitions.hpp>
#include <deployr/deployr.hpp>
#include <taskr/taskr.hpp>
#include <hicr/backends/hwloc/topologyManager.hpp>
#include <hicr/backends/boost/computeManager.hpp>
#include <hicr/backends/pthreads/computeManager.hpp>
#include "task.hpp"

namespace llmEngine
{

class LLMEngine final
{
  public:

  LLMEngine()
  {
  }

  ~LLMEngine() {}

  __INLINE__ bool initialize(int *pargc, char ***pargv)
  {
    // Registering entry point function
    _deployr.registerFunction(_entryPointName, [this]() { entryPoint(); });

    // Initializing DeployR
    return _deployr.initialize(pargc, pargv);
  }

  __INLINE__ void run(const nlohmann::json& config)
  {
    // Storing configuration
    _config = config;

    // Deploying
    deploy(_config);
  }

  __INLINE__ void abort()
   {
    _deployr.abort();
   }

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
    // Send a finalization signal to all instances
    auto& instances = _rpcEngine->getInstanceManager()->getInstances();
    for (auto& instance : instances) if (instance->getId() != _rpcEngine->getInstanceManager()->getCurrentInstance()->getId())
    {
      _rpcEngine->requestRPC(*instance, _stopRPCName);
    } 

    // Stop running ourselves.
    _continueRunning = false;
  }


  __INLINE__ void finalize()
  {
    // Finished execution, then finish deployment
     _deployr.finalize();
  }

  private:

  __INLINE__ void deploy(const nlohmann::json& config)
  {
     // Create request json
     nlohmann::json requestJs;

     // Some information can be passed directly
     requestJs["Name"] = config["Name"];
     requestJs["Host Types"] = config["Host Types"];

     ////// Instances information
     std::vector<nlohmann::json> newInstances;
     for (const auto& instance : config["Instances"])
     {
      nlohmann::json newInstance;
      newInstance["Name"] = instance["Name"];
      newInstance["Host Type"] = instance["Host Type"];     
      newInstance["Function"] = _entryPointName;     
      newInstances.push_back(newInstance);
     }
     requestJs["Instances"] = newInstances;

     // Channel information
     std::vector<nlohmann::json> channels;

     // Getting the instance vector
     const auto& instancesJs = hicr::json::getArray<nlohmann::json>(config, "Instances");

     // Going through instances and checking producer instances
     std::map<std::string, std::vector<std::string>> producers;
     for (const auto& instance : instancesJs)
     {
      const auto& instanceName = hicr::json::getString(instance, "Name");
      const auto& executionGraph = hicr::json::getArray<nlohmann::json>(instance, "Execution Graph");
      for (const auto& function : executionGraph)
      {
        const auto& outputNames = hicr::json::getArray<std::string>(function, "Outputs");
        for (const auto& outputName : outputNames)  producers[outputName].push_back(instanceName);
      }
     }

     // Going through instances and checking inputs buffers 
     std::vector<nlohmann::json> newChannels;
     for (const auto& instance : instancesJs)
     {
      const auto& instanceName = hicr::json::getString(instance, "Name");
      const auto& executionGraph = hicr::json::getArray<nlohmann::json>(instance, "Execution Graph");
      for (const auto& function : executionGraph)
      {
       const auto& inputs = hicr::json::getArray<nlohmann::json>(function, "Inputs");
       for (const auto& input : inputs)
       {
          nlohmann::json newChannel;
          const auto& inputName = hicr::json::getString(input, "Name");

          // Checking somebody produces this input
          if (producers.contains(inputName) == false)
          {
            fprintf(stderr, "Input '%s' defined for instance '%s', but no outputs for it have been defined.\n", inputName.c_str(), instanceName.c_str());
            abort();
          }

          // Checking the consumer is not also a producer
          const auto& producersList = producers.at(inputName);
          if (std::find(producersList.begin(), producersList.end(), instanceName) != producersList.end()) 
          {
            fprintf(stderr, "Input '%s' defined for instance '%s', but no outputs for it have been defined.\n", inputName.c_str(), instanceName.c_str());
            abort();
          }

          newChannel["Name"] = inputName; 
          newChannel["Producers"] = producers.at(inputName);
          newChannel["Consumer"] = instanceName;
          newChannel["Buffer Capacity (Tokens)"] = hicr::json::getNumber<size_t>(input, "Buffer Capacity (Tokens)");
          newChannel["Buffer Size (Bytes)"] = hicr::json::getNumber<size_t>(input, "Buffer Size (Bytes)");
          newChannels.push_back(newChannel);
       }
      }
     }
     requestJs["Channels"] = newChannels;

     // Adding LLM Engine as metadata in the request object
     requestJs["LLM Engine Configuration"] = _config;

     // Creating configuration for TaskR
     nlohmann::json taskrConfig;
     taskrConfig["Task Worker Inactivity Time (Ms)"] = 10; // Suspend workers if a certain time of inactivity elapses
     taskrConfig["Task Suspend Interval Time (Ms)"] = 10; // Workers suspend for this time before checking back
     taskrConfig["Minimum Active Task Workers"] = 1; // Have at least one worker active at all times
     taskrConfig["Service Worker Count"] = 1; // Have one dedicated service workers at all times to listen for incoming messages
     taskrConfig["Make Task Workers Run Services"] = true; // Workers will check for meta messages in between executions

     // Adding taskr configuration
     requestJs["TaskR Configuration"] = taskrConfig;

     // Creating deployR request object
     deployr::Request request(requestJs);

     // Deploying
     _deployr.deploy(request);
  }

  __INLINE__ void entryPoint()
  {
    // Starting a new deployment
    _continueRunning = true;

    // Getting pointer to RPC engine
    _rpcEngine = _deployr.getRPCEngine();

    // Registering finalization function
    _rpcEngine->addRPCTarget(_stopRPCName, HiCR::backend::pthreads::ComputeManager::createExecutionUnit([this](void*) 
    {
      _continueRunning = false;
    }));

    // Getting my instance name after deployment
    const auto& myInstance = _deployr.getLocalInstance();
    const auto& myInstanceName = myInstance.getName();

    // Getting deployment
    const auto& deployment = _deployr.getDeployment();

    // Getting request from deployment
    const auto& request = deployment.getRequest();

    // Getting request configuration from the request ibject
    const auto& requestJs = request.serialize();

    // Getting LLM engine configuration from request configuration
    _config = hicr::json::getObject(requestJs, "LLM Engine Configuration");

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
    const auto& devices = t.getDevices();
    for (const auto& device : devices) if (device->getType() == "NUMA Domain")
    {
      // Getting compute resources in this device
      auto crs = device->getComputeResourceList();

      // Adding it to the list
      for (const auto& cr : crs) computeResources.push_back(cr);
    }

    // Freeing up memory
    hwloc_topology_destroy(topology);
    
    // Creating taskr object
    const auto& taskRConfig = hicr::json::getObject(requestJs, "TaskR Configuration");
    _taskr = std::make_unique<taskr::Runtime>(&_boostComputeManager, &_pthreadsComputeManager, computeResources, taskRConfig);

    // Initializing TaskR
    _taskr->initialize();

    // Finding instance information on the configuration
    const auto& instancesJs = hicr::json::getArray<nlohmann::json>(_config, "Instances");
    nlohmann::json myInstanceConfig;
    for (const auto& instance : instancesJs)
    {
      const auto& instanceName = hicr::json::getString(instance, "Name");
      if (instanceName == myInstanceName) myInstanceConfig = instance;
    }

    // Getting relevant information 
    const auto& executionGraph = hicr::json::getArray<nlohmann::json>(myInstanceConfig, "Execution Graph");

    // Getting execution graph functions into a set. This is necessary for dependency checking
    std::set<std::string> executionGraphFunctions;
    for (const auto& functionJs : executionGraph)
    {
      const auto& fcName = hicr::json::getString(functionJs, "Name");
      executionGraphFunctions.insert(fcName);
    }

    // Creating general TaskR function for all execution graph functions
    _taskrFunction = std::make_unique<taskr::Function>([this](taskr::Task* task) { runTaskRFunction(task); });

    // Checking the execution graph functions have been registered
    taskr::label_t taskrLabelCounter = 0;
    for (const auto& functionJs : executionGraph)
    {
      const auto& fcName = hicr::json::getString(functionJs, "Name");

      // Checking the requested function was registered
      if (_registeredFunctions.contains(fcName) == false)
      {
        fprintf(stderr, "The requested function name '%s' is not registered. Please register it before running the LLM Engine.\n", fcName.c_str());
        abort();
      }

      // Checking all dependencies have been declared
      const auto& dependencies = hicr::json::getArray<std::string>(functionJs, "Dependencies");
      for (const auto& dependency : dependencies)
        if (executionGraphFunctions.contains(dependency) == false)
        {
          fprintf(stderr, "Function '%s' declared a dependency '%s' that is part of the execution graph\n", fcName.c_str(), dependency.c_str());
          abort();
        }

      // Getting inputs and outputs
      std::vector<std::string> inputs;
      const auto& inputsJs = hicr::json::getArray<nlohmann::json>(functionJs, "Inputs");
      for (const auto& inputJs : inputsJs)
      {
        const auto& inputName = hicr::json::getString(inputJs, "Name");
        inputs.push_back(inputName);
      } 
      const auto& outputs = hicr::json::getArray<std::string>(functionJs, "Outputs");

      // Getting label for taskr function
      const auto taskLabel = taskrLabelCounter++;

      // Creating taskr Task corresponding to the LLM engine task
      auto taskrTask =  std::make_unique<taskr::Task>(_taskrFunction.get());
      taskrTask->setLabel(taskLabel);

      // Getting function pointer
      const auto &fc = _registeredFunctions[fcName];

      // Creating LLMEngine Task object
      auto newTask = std::make_shared<llmEngine::Task>(fcName, fc, inputs, outputs, dependencies, std::move(taskrTask));

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
    std::function<void()> RPCListeningService = [this]()
    {
      if (_rpcEngine->hasPendingRPCs())
        _rpcEngine->listen();
    };
    _taskr->addService(&RPCListeningService);

    // Running TaskR
    _taskr->run();
    _taskr->await();
    _taskr->finalize();

    // printf("[Instance '%s'] stopped\n", myInstanceName.c_str());
  }

  __INLINE__ void runTaskRFunction(taskr::Task* task)
  {
    const auto& taskLabel = task->getLabel();
    const auto& taskObject = _taskLabelMap.at(taskLabel);
    const auto& function = taskObject->getFunction();
    const auto& functionName = taskObject->getName();
    const auto& inputs = taskObject->getInputs();
    const auto& outputs = taskObject->getOutputs();
    const auto& dependencies = taskObject->getDependencies();

    // Resolving pointers to all the required input channels
    std::vector<deployr::Channel*> inputChannels;
    for (const auto& input : inputs) inputChannels.push_back(&_deployr.getChannel(input));

    // Resolving pointers to all the required output channels
    std::vector<deployr::Channel*> outputChannels;
    for (const auto& output : outputs) outputChannels.push_back(&_deployr.getChannel(output));

    // Resolving pointers to all the required LLM Task dependencies
    std::vector<llmEngine::Task*> dependencyTasks;
    for (const auto& dependency : dependencies) dependencyTasks.push_back(_taskNameMap.at(dependency).get());

    // Function to check for pending operations
    auto pendingOperationsCheck = [&](){
          // If execution must stop now, return true to go back to the task and finish it
          if (_continueRunning == false) return true;

          // The task is not ready if any of its inputs are not yet available
          for (const auto& inputChannel : inputChannels) if (inputChannel->isEmpty()) return false;

          // The task is not ready if any of its output channels are still full
          for (const auto& outputChannel : outputChannels) if (outputChannel->isFull()) return false;

          // The task is not ready if any of its dependencies haven't exceeded its execution counter (return false)
          for (const auto& dependencyTask : dependencyTasks) if (dependencyTask->getExecutionCounter() <= taskObject->getExecutionCounter()) return false;

          // All dependencies are satisfied, enable this task for execution 
          return true;
    };

    // Initiate infinite loop
    while(_continueRunning)
    {
      // Adding task dependencies
      task->addPendingOperation(pendingOperationsCheck);

      // Suspend execution until all dependencies are met
      task->suspend();

      // Another exit point (the more the better)
      if (_continueRunning == false) break;

      // Inputs dependencies must be satisfied by now; getting them values
      for (size_t i = 0; i < inputs.size(); i++)
      {
        // Getting input name
        const auto& input = inputs[i];
        
        // Peeking token from input channel
        const auto token = inputChannels[i]->peek();

        // Pushing token onto the task
        taskObject->setInput(input, token);
      }

      // Actually run the function now
      // printf("Running Task: %lu - Function: (%s)\n", taskLabel, functionName.c_str());
      function(taskObject.get());

      // Another exit point (the more the better)
      if (_continueRunning == false) break;

      // Checking input messages were properly consumed and popping the used token
      for (size_t i = 0; i < inputs.size(); i++)
      {
        // Getting output name
        const auto& input = inputs[i];

        if (taskObject->hasInput(input) == true) 
        {
          fprintf(stderr, "Function '%s' has not consumed required input '%s'\n", functionName.c_str(), input.c_str());
          abort();
        }

        // Popping consumed channel from channel
        inputChannels[i]->pop();
      }

      // Validating and pushing output messages
      for (size_t i = 0; i < outputs.size(); i++)
      {
        // Getting output name
        const auto& output = outputs[i];

        // Checking if output has been produced by the task
        if (taskObject->hasOutput(output) == false) 
        {
          fprintf(stderr, "Function '%s' has not pushed required output '%s'\n", functionName.c_str(), output.c_str());
          abort();
        }

        // Now getting output token
        const auto& outputToken = taskObject->getOutput(output);

        // Pushing token onto channel
        outputChannels[i]->push(outputToken.buffer, outputToken.size);
      }

      // Clearing output token maps
      taskObject->clearOutputs();

      // Advance execution counter (do last to avoid any concurrency issues)
      taskObject->advanceExecutionCounter();
    }
  }

  // A system-wide flag indicating that we should continue executing
  bool _continueRunning;

  std::unique_ptr<taskr::Function> _taskrFunction;

  /// A map of registered functions, targets for an instance's initial function
  std::map<std::string, function_t> _registeredFunctions;

  // Copy of the initial configuration file
  nlohmann::json _config;
  
  // Name of the LLM Engine entry point after deployment
  const std::string _entryPointName = "__LLM Engine Entry Point__";
  const std::string _stopRPCName = "__LLM Engine Stop__";

  // DeployR instance
  deployr::DeployR _deployr;

  // TaskR instance
  std::unique_ptr<taskr::Runtime> _taskr;

  // Map relating task labels to their LLM Engine task
  std::map<taskr::label_t, std::shared_ptr<llmEngine::Task>> _taskLabelMap;

    // Map relating function name to their LLM Engine task
  std::map<std::string, std::shared_ptr<llmEngine::Task>> _taskNameMap;

  // Pointer to the HiCR RPC Engine
  HiCR::frontend::RPCEngine* _rpcEngine;

  ///// HiCR Objects for TaskR

  // Initializing Boost-based compute manager to instantiate suspendable coroutines
  HiCR::backend::boost::ComputeManager _boostComputeManager;

  // Initializing Pthreads-based compute manager to instantiate processing units
  HiCR::backend::pthreads::ComputeManager _pthreadsComputeManager;

}; // class LLMEngine

} // namespace llmEngine
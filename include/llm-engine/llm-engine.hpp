
#pragma once

#include <hicr/core/exceptions.hpp>
#include <hicr/core/definitions.hpp>
#include <deployr/deployr.hpp>
#include <taskr/taskr.hpp>
#include <hicr/backends/hwloc/topologyManager.hpp>
#include <hicr/backends/boost/computeManager.hpp>
#include <hicr/backends/pthreads/computeManager.hpp>

namespace llmEngine
{

class LLMEngine final
{
  public:

  LLMEngine()
  {
  }

  ~LLMEngine() {}

  __INLINE__ void initialize(int *pargc, char ***pargv)
  {
    // Initializing DeployR
    _deployr.initialize(pargc, pargv);
  }

  __INLINE__ void run(const nlohmann::json& config)
  {
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

    // Creating configuration for TaskR
    nlohmann::json taskrConfig;
    taskrConfig["Task Worker Inactivity Time (Ms)"] = 10; // Suspend workers if a certain time of inactivity elapses
    taskrConfig["Task Suspend Interval Time (Ms)"] = 10; // Workers suspend for this time before checking back
    taskrConfig["Minimum Active Task Workers"] = 1; // Have at least one worker active at all times
    taskrConfig["Service Worker Count"] = 1; // Have one dedicated service workers at all times to listen for incoming messages
    taskrConfig["Make Task Workers Run Services"] = false; // No need to have workers check for services

    // Creating taskr
    _taskr = std::make_unique<taskr::Runtime>(&_boostComputeManager, &_pthreadsComputeManager, computeResources, taskrConfig);

    // Initializing TaskR
    _taskr->initialize();

    // Freeing up memory
    hwloc_topology_destroy(topology);

    // Deploying
    deploy(config);
  }

  __INLINE__ void abort() { _deployr.abort(); }

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
      newInstance["Function"] = "__LLM Engine Entry Point__";     
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
          if (producers.contains(inputName) == false)
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

     // Creating deployR request object
     deployr::Request request(requestJs);

     // Deploying
     _deployr.deploy(request);
  }


  // DeployR instance
  deployr::DeployR _deployr;

  // TaskR instance
  std::unique_ptr<taskr::Runtime> _taskr;

  ///// HiCR Objects for TaskR

  // Initializing Boost-based compute manager to instantiate suspendable coroutines
  HiCR::backend::boost::ComputeManager _boostComputeManager;

  // Initializing Pthreads-based compute manager to instantiate processing units
  HiCR::backend::pthreads::ComputeManager _pthreadsComputeManager;

}; // class LLMEngine

} // namespace llmEngine
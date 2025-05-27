
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
    // initializing hwloc topology object
    hwloc_topology_init(&_topology);

    // Initializing HWLoc-based host (CPU) topology manager
    HiCR::backend::hwloc::TopologyManager tm(&_topology);

    // Asking backend to check the available devices
    const auto t = tm.queryTopology();

    // Compute resources to use
    HiCR::Device::computeResourceList_t computeResources;

    // Getting compute resources in this device
    auto cr = (*(t.getDevices().begin()))->getComputeResourceList();

    // Adding it to the list
    auto itr = cr.begin();
    for (int i = 0; i < 1; i++)
    {
      computeResources.push_back(*itr);
      itr++;
    }

    // Initializing Boost-based compute manager to instantiate suspendable coroutines
    HiCR::backend::boost::ComputeManager boostComputeManager;

    // Initializing Pthreads-based compute manager to instantiate processing units
    HiCR::backend::pthreads::ComputeManager pthreadsComputeManager;

    // Creating taskr
    _taskr = std::make_unique<taskr::Runtime>(&boostComputeManager, &pthreadsComputeManager, computeResources);
  }

  ~LLMEngine()
  {
    // Freeing up memory
    hwloc_topology_destroy(_topology);
  }

  __INLINE__ void initialize(int *pargc, char ***pargv)
  {
    // Initializing DeployR
    _deployr.initialize(pargc, pargv);

    // Initializing computing resources for TaskR deployment
    

  }

  private:

  // DeployR instance
  deployr::DeployR _deployr;

  // TaskR instance
  std::unique_ptr<taskr::Runtime> _taskr;

  // Creating HWloc topology object
  hwloc_topology_t _topology;

}; // class LLMEngine

} // namespace llmEngine
#include <stdio.h>
#include <thread>
#include <fstream>
#include <random>

#include <hicr/backends/hwloc/memoryManager.hpp>
#include <hicr/backends/mpi/memoryManager.hpp>
#include <hicr/backends/hwloc/topologyManager.hpp>
#include <hicr/backends/mpi/instanceManager.hpp>
#include <hicr/backends/mpi/communicationManager.hpp>
#include <hicr/backends/pthreads/computeManager.hpp>
#include <hicr/backends/pthreads/communicationManager.hpp>
#include <hicr/backends/boost/computeManager.hpp>
#include <hicr/frontends/RPCEngine/RPCEngine.hpp>

#include <modules/configuration/deployment.hpp>
#include <modules/taskScheduler/module.hpp>
#include <system/engine.hpp>

int main(int argc, char *argv[])
{
  // Creating HWloc topology object
  hwloc_topology_t hwlocTopologyObject;

  // Reserving memory for hwloc
  hwloc_topology_init(&hwlocTopologyObject);

  // Initializing host (CPU) topology manager
  HiCR::backend::hwloc::TopologyManager hwlocTopologyManager(&hwlocTopologyObject);

  // Gathering topology from the topology manager
  const auto topology = hwlocTopologyManager.queryTopology();

  auto d                  = *topology.getDevices().begin();
  auto memSpaces          = d->getMemorySpaceList();
  auto bufferMemorySpace  = *memSpaces.begin();
  auto computeResourcesIt = d->getComputeResourceList().begin();

  // Use only 2 cores
  std::vector<std::shared_ptr<HiCR::ComputeResource>> computeResources;
  computeResources.push_back(*computeResourcesIt);
  computeResourcesIt++;
  computeResources.push_back(*computeResourcesIt);
  computeResourcesIt++;
  auto computeResource = *computeResources.begin();

  // Getting managers
  auto instanceManager      = std::shared_ptr<HiCR::InstanceManager>(HiCR::backend::mpi::InstanceManager::createDefault(&argc, &argv));
  auto communicationManager = std::make_shared<HiCR::backend::mpi::CommunicationManager>();
  auto memoryManager        = std::make_shared<HiCR::backend::mpi::MemoryManager>();
  auto workerComputeManager = std::make_shared<HiCR::backend::pthreads::ComputeManager>();
  auto taskComputeManager   = std::make_shared<HiCR::backend::boost::ComputeManager>();

  // Instantiate RPC Engine
  auto rpcEngine = std::make_shared<HiCR::frontend::RPCEngine>(*communicationManager, *instanceManager, *memoryManager, *workerComputeManager, bufferMemorySpace, computeResource);

  // Initialize RPC Engine
  rpcEngine->initialize();

  // Creating taskr object
  nlohmann::json taskrConfig;
  taskrConfig["Task Worker Inactivity Time (Ms)"] = 100;  // Suspend workers if a certain time of inactivity elapses
  taskrConfig["Task Suspend Interval Time (Ms)"]  = 100;  // Workers suspend for this time before checking back
  taskrConfig["Minimum Active Task Workers"]      = 1;    // Have at least one worker active at all times
  taskrConfig["Service Worker Count"]             = 1;    // Have one dedicated service workers at all times to listen for incoming messages
  taskrConfig["Make Task Workers Run Services"]   = true; // Workers will check for meta messages in between executions
  auto taskr                                      = std::make_shared<taskr::Runtime>(taskComputeManager.get(), workerComputeManager.get(), computeResources, taskrConfig);

  // Creating hLLM Engine object
  hLLM::system::Engine hllm(instanceManager, taskComputeManager, rpcEngine, instanceManager->getRootInstanceId());

  // Check whether the instance is root
  const auto isRoot     = instanceManager->getCurrentInstance()->isRootInstance();
  const auto instanceId = instanceManager->getCurrentInstance()->getId();

  auto taskSchedulerModule = std::make_unique<hLLM::modules::taskScheduler::Module>(taskComputeManager, taskr);

  // Adding a simple task that prints "Hello world". Here we call terminate from within the task
  // to keep the application simple
  taskSchedulerModule->addTask("helloWorld", [&](taskr::Task *task) { printf("[Instance %lu] Hello World from task %lu!\n", instanceId, task->getTaskId()); });

  // Adding task scheduler module to hLLM
  hllm.addModule("taskScheduler", std::move(taskSchedulerModule));

  // Initializing hLLM
  hllm.initialize();

  // Running hLLM
  hllm.run();

  if (isRoot)
  {
    printf("[Instance %lu] issuing termination\n", instanceId);

    std::this_thread::sleep_for(std::chrono::seconds(1));
    // Finalizing hLLM
    hllm.terminate();
  }

  // Awaiting hLLM termination
  hllm.await();

  // Finalize Instance Manager
  instanceManager->finalize();
}

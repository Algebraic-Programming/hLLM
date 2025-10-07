#include <fstream>
#include <nlohmann_json/json.hpp>
#include <hicr/backends/cloudr/instanceManager.hpp>
#include <hicr/backends/cloudr/communicationManager.hpp>
#include <hicr/backends/mpi/communicationManager.hpp>
#include <hicr/backends/mpi/memoryManager.hpp>
#include <hicr/backends/mpi/instanceManager.hpp>
#include <hicr/backends/hwloc/memoryManager.hpp>
#include <hicr/backends/hwloc/topologyManager.hpp>
#include <hicr/backends/pthreads/computeManager.hpp>
#include <hicr/backends/pthreads/communicationManager.hpp>
#include <hllm/engine.hpp>
#include "basic.hpp"
#include "requester.hpp"

int main(int argc, char *argv[])
{
  // // Creating HWloc topology object
  // hwloc_topology_t hwlocTopologyObject;

  // // Reserving memory for hwloc
  // hwloc_topology_init(&hwlocTopologyObject);

  // // Initializing host (CPU) topology manager
  // HiCR::backend::hwloc::TopologyManager hwlocTopologyManager(&hwlocTopologyObject);

  // // Gathering topology from the topology manager
  // const auto topology = hwlocTopologyManager.queryTopology();

  // // Selecting first device
  // auto d = *topology.getDevices().begin();

  // // Getting memory space list from device
  // auto memSpaces = d->getMemorySpaceList();

  // // Grabbing first memory space for buffering
  // auto bufferMemorySpace = *memSpaces.begin();

  // // Now getting compute resource list from device
  // auto computeResources = d->getComputeResourceList();

  // // Grabbing first compute resource for computing incoming RPCs
  // auto computeResource = *computeResources.begin();

  // // Instantiating the other base managers
  // auto mpiInstanceManager      = HiCR::backend::mpi::InstanceManager::createDefault(&argc, &argv);
  // auto mpiCommunicationManager = HiCR::backend::mpi::CommunicationManager(MPI_COMM_WORLD);
  // auto mpiMemoryManager        = HiCR::backend::mpi::MemoryManager();
  // auto pthreadsComputeManager       = HiCR::backend::pthreads::ComputeManager();
  // auto pthreadsCommunicationManager = HiCR::backend::pthreads::CommunicationManager();
  // auto hwlocMemoryManager           = HiCR::backend::hwloc::MemoryManager(&hwlocTopologyObject);

  // // Getting my base instance id and index
  // const auto baseInstanceId  = mpiInstanceManager->getCurrentInstance()->getId();
  // uint64_t   baseInstanceIdx = 0;
  // for (size_t i = 0; i < mpiInstanceManager->getInstances().size(); i++)
  //   if (mpiInstanceManager->getInstances()[i]->getId() == baseInstanceId) baseInstanceIdx = i;

  // // Checking for parameters
  // if (argc != 3)
  // {
  //   fprintf(stderr, "Error: You need to pass a deployment.json and a cloudr.json file as parameters.\n");
  //   mpiInstanceManager->finalize();
  //   return -1;
  // }

  // // Reading cloudr config file
  // std::string cloudrConfigFilePath = std::string(argv[2]);

  // // Parsing deployment file contents to a JSON object
  // std::ifstream cloudrConfigFs(cloudrConfigFilePath);
  // auto          cloudrConfigJs = nlohmann::json::parse(cloudrConfigFs);

  // // Make sure we're running the number of base instances as emulated cloudr instances
  // if (mpiInstanceManager->getInstances().size() != cloudrConfigJs["Topologies"].size())
  // {
  //   fprintf(stderr,
  //           "Error: The number of requested cloudr instances (%lu) is different than the number of instances provided (%lu)\n",
  //           cloudrConfigJs["Topologies"].size(),
  //           mpiInstanceManager->getInstances().size());
  //   mpiInstanceManager->finalize();
  //   return -1;
  // }

  // // Getting my emulated topology from the cloudr configuration file
  // HiCR::Topology emulatedTopology(cloudrConfigJs["Topologies"][baseInstanceIdx]);

  // // Instantiating RPC engine
  // HiCR::frontend::RPCEngine rpcEngine(mpiCommunicationManager, *mpiInstanceManager, mpiMemoryManager, pthreadsComputeManager, bufferMemorySpace, computeResource);

  // // Initializing RPC engine
  // rpcEngine.initialize();

  // // Getting config file name from arguments
  // std::string configFilePath = std::string(argv[1]);

  // // Parsing request file contents to a JSON object
  // std::ifstream hllmConfigFs(configFilePath);
  // auto hllmConfigJs = nlohmann::json::parse(hllmConfigFs);

  // // Instantiating CloudR
  // HiCR::backend::cloudr::InstanceManager cloudrInstanceManager(&rpcEngine, emulatedTopology, [&]() {
  //   // Getting our current cloudr instance
  //   const auto &currentInstance = cloudrInstanceManager.getCurrentInstance();

  //   // Getting our instance's emulated topology
  //   const auto &emulatedTopology = dynamic_pointer_cast<HiCR::backend::cloudr::Instance>(currentInstance)->getTopology();

  //   // // Creating hLLM Engine object
  //   // hLLM::Engine engine(&cloudrInstanceManager,
  //   //                 &mpiCommunicationManager,
  //   //                 &pthreadsCommunicationManager,
  //   //                 &mpiMemoryManager,
  //   //                 &hwlocMemoryManager,
  //   //                 &rpcEngine,
  //   //                 bufferMemorySpace,
  //   //                 bufferMemorySpace,
  //   //                 emulatedTopology);

  //   // // Deploy all LLM Engine instances
  //   // engine.deploy(hllmConfigJs);
  // });

  // // Initializing CloudR -- this is a bifurcation point. Only the root instance advances now
  // cloudrInstanceManager.initialize();

  // // Finalizing cloudR
  // cloudrInstanceManager.finalize();

  // // Finalizing base instance manager
  // mpiInstanceManager->finalize();
}
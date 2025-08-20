#include <stdio.h>
#include <thread>
#include <fstream>

#include <hicr/backends/mpi/instanceManager.hpp>
#include <hicr/backends/mpi/communicationManager.hpp>
#include <hicr/backends/mpi/memoryManager.hpp>
#include <hicr/backends/pthreads/computeManager.hpp>
#include <hicr/frontends/RPCEngine/RPCEngine.hpp>
#include <hicr/backends/hwloc/topologyManager.hpp>

#include <hllm/engine.hpp>

#include "requester.hpp"

int main(int argc, char *argv[])
{
  // Creating HWloc topology object
  hwloc_topology_t topology;

  // Reserving memory for hwloc
  hwloc_topology_init(&topology);

  // Initializing host (CPU) topology manager
  HiCR::backend::hwloc::TopologyManager tm(&topology);

  // Gathering topology from the topology manager
  const auto t = tm.queryTopology();

  // Selecting first device
  auto d = *t.getDevices().begin();

  // Getting memory space list from device
  auto memSpaces = d->getMemorySpaceList();

  // Grabbing first memory space for buffering
  auto bufferMemorySpace = *memSpaces.begin();

  // Now getting compute resource list from device
  auto computeResources = d->getComputeResourceList();

  // Grabbing first compute resource for computing incoming RPCs
  auto computeResource = *computeResources.begin();

  // Getting MPI managers
  auto instanceManager      = HiCR::backend::mpi::InstanceManager::createDefault(&argc, &argv);
  auto communicationManager = std::make_shared<HiCR::backend::mpi::CommunicationManager>();
  auto memoryManager        = std::make_shared<HiCR::backend::mpi::MemoryManager>();
  auto computeManager       = std::make_shared<HiCR::backend::pthreads::ComputeManager>();

  auto rpcEngine =
    std::make_shared<HiCR::frontend::RPCEngine>(*communicationManager, *instanceManager, *memoryManager, *computeManager, bufferMemorySpace, computeResource);

  rpcEngine->initialize();

  // Creating hLLM Engine object
  hLLM::Engine engine(instanceManager.get(), communicationManager.get(), memoryManager.get(), rpcEngine.get(), bufferMemorySpace);

  // Instantiating request server (emulates live users)
  size_t requestCount   = 32;
  size_t requestDelayMs = 100;
  initializeRequestServer(&engine, requestCount);
  auto requestThread = std::thread([&]() { startRequestServer(requestDelayMs); });

  // Listen request function -- it expects an outside input and creates a request
  std::string requestOutput;
  engine.registerFunction("Listen Request", [&](hLLM::Task *task) {
    // Listening to incoming requests (emulates an http service)
    printf("Listening to incoming requests...\n");
    requestId_t requestId = 0;
    task->waitFor([&]() { return listenRequest(requestId); });
    printf("Request %lu received.\n", requestId);

    // Create and register request as output
    requestOutput = std::string("This is request ") + std::to_string(requestId);
    task->setOutput("Request", requestOutput.data(), requestOutput.size() + 1);
  });

  // Listen request function -- it expects an outside input and creates a request
  std::string decodedRequest1Output;
  std::string decodedRequest2Output;
  engine.registerFunction("Decode Request", [&](hLLM::Task *task) {
    // Getting incoming request
    const auto &requestMsg = task->getInput("Request");

    // Getting request
    const auto request = std::string((const char *)requestMsg.buffer);
    printf("Decoding request: '%s'\n", request.c_str());

    // Create and register decoded requests
    decodedRequest1Output = request + std::string(" [Decoded 1]");
    task->setOutput("Decoded Request 1", decodedRequest1Output.data(), decodedRequest1Output.size() + 1);

    decodedRequest2Output = request + std::string(" [Decoded 2]");
    task->setOutput("Decoded Request 2", decodedRequest2Output.data(), decodedRequest2Output.size() + 1);
  });

  // Request transformation functions
  std::string transformedRequest1Output;
  engine.registerFunction("Transform Request 1", [&](hLLM::Task *task) {
    // Getting incoming decoded request 1
    const auto &decodedRequest1Msg = task->getInput("Decoded Request 1");
    const auto  decodedRequest1    = std::string((const char *)decodedRequest1Msg.buffer);
    printf("Transforming decoded request 1: '%s'\n", decodedRequest1.c_str());

    // Create and register decoded requests
    transformedRequest1Output = decodedRequest1 + std::string(" [Transformed]");
    task->setOutput("Transformed Request 1", transformedRequest1Output.data(), transformedRequest1Output.size() + 1);
  });

  std::string preTransformedRequest;
  engine.registerFunction("Pre-Transform Request", [&](hLLM::Task *task) {
    // Getting incoming decoded request 1
    const auto &decodedRequest2Msg = task->getInput("Decoded Request 2");
    const auto  decodedRequest2    = std::string((const char *)decodedRequest2Msg.buffer);
    printf("Pre-Transforming decoded request 2: '%s'\n", decodedRequest2.c_str());

    // Create and register decoded requests
    preTransformedRequest = decodedRequest2 + std::string(" [Pre-Transformed]");
    task->setOutput("Pre-Transform Request Output", preTransformedRequest.data(), preTransformedRequest.size() + 1);
  });

  std::string transformedRequest2Output;
  engine.registerFunction("Transform Request 2", [&](hLLM::Task *task) {
    // Create and register decoded requests
    const auto &preTransformedRequestOutputMsg = task->getInput("Pre-Transform Request Output");
    const auto  preTransformedRequestOutput    = std::string((const char *)preTransformedRequestOutputMsg.buffer);
    printf("Transforming pre-transformed request 2: '%s'\n", preTransformedRequestOutput.c_str());
    transformedRequest2Output = preTransformedRequestOutput + std::string(" [Transformed]");
    task->setOutput("Transformed Request 2", transformedRequest2Output.data(), transformedRequest2Output.size() + 1);
  });

  std::string resultOutput;
  engine.registerFunction("Respond Request", [&](hLLM::Task *task) {
    // Getting incoming decoded request 1
    const auto &transformedRequest1Msg = task->getInput("Transformed Request 1");
    const auto  transformedRequest1    = std::string((const char *)transformedRequest1Msg.buffer);

    const auto &transformedRequest2Msg = task->getInput("Transformed Request 2");
    const auto  transformedRequest2    = std::string((const char *)transformedRequest2Msg.buffer);

    // Producing response and sending it
    resultOutput = transformedRequest1 + std::string(" + ") + transformedRequest2;
    printf("Joining '%s' + '%s' = '%s'\n", transformedRequest1.c_str(), transformedRequest2.c_str(), resultOutput.c_str());
    respondRequest(resultOutput);
  });

  // Initializing LLM engine
  auto isRoot = engine.initialize();

  std::cout << "THI" << std::endl;

  // Let only the root instance deploy the LLM engine
  if (isRoot)
  {
    // Checking arguments
    if (argc != 2)
    {
      fprintf(stderr, "Error: Must provide the request file as argument.\n");
      engine.abort();
      return -1;
    }

    // Getting config file name from arguments
    std::string configFilePath = std::string(argv[1]);

    // Parsing request file contents to a JSON object
    std::ifstream ifs(configFilePath);
    auto          configJs = nlohmann::json::parse(ifs);

    // Deploy all LLM Engine instances
    engine.deploy(configJs);
  }

  // Run the engine
  engine.run();

  // Waiting for request server to finish producing requests
  printf("[basic.cpp] Waiting for request engine thread to come back...\n");
  requestThread.join();

  // Finalizing LLM engine
  printf("[basic.cpp] Finalizing...\n");
  engine.finalize();
}
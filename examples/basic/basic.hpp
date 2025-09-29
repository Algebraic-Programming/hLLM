#pragma once

#include <string>
#include <array>

#include <hicr/core/memoryManager.hpp>
#include <hicr/core/memorySpace.hpp>
#include <hllm/engine.hpp>
#include "requester.hpp"

std::string requestOutput;
std::string processedRequestOutput;
std::string resultOutput;

#define BASIC_INSTANCE_COUNT 3

void createTasks(hLLM::Engine &engine, HiCR::MemoryManager *const memoryManager, std::shared_ptr<HiCR::MemorySpace> memorySpace)
{
  // Listen request function -- it expects an outside input and creates a request
  engine.registerFunction("Listen Request", [=](hLLM::Task *task) {
    // Listening to incoming requests (emulates an http service)
    printf("Listening to incoming requests...\n");
    requestId_t requestId = 0;
    task->waitFor([&]() { return listenRequest(requestId); });
    printf("Request %lu received.\n", requestId);

    // Create and register request as output
    requestOutput             = std::string("This is request ") + std::to_string(requestId);
    const auto requestMemSlot = memoryManager->registerLocalMemorySlot(memorySpace, requestOutput.data(), requestOutput.size() + 1);
    task->setOutput("Request", requestMemSlot);
  });

  // Listen request function -- it expects an outside input and creates a request
  engine.registerFunction("Process Request", [=](hLLM::Task *task) {
    // Getting incoming request
    const auto &requestMemSlot = task->getInput("Request");

    // Getting request
    const auto request = std::string((const char *)requestMemSlot->getPointer());
    printf("Decoding request: '%s'\n", request.c_str());

    // Create and register decoded requests
    processedRequestOutput             = request + std::string(" [Processed]");
    const auto processedRequestMemSlot = memoryManager->registerLocalMemorySlot(memorySpace, processedRequestOutput.data(), processedRequestOutput.size() + 1);
    task->setOutput("Processed Request", processedRequestMemSlot);
  });

  engine.registerFunction("Return Result", [=](hLLM::Task *task) {
    // Getting incoming decoded request 1
    const auto &processedRequestMemSlot = task->getInput("Processed Request");
    const auto  processedRequest        = std::string((const char *)processedRequestMemSlot->getPointer());

    // Producing response and sending it
    resultOutput = processedRequest + std::string(" [ Output ]");
    printf("Result '%s'\n", processedRequest.c_str());
    respondRequest(resultOutput);
  });
}
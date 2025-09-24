#pragma once

#include <string>

#include <hicr/core/memoryManager.hpp>
#include <hicr/core/memorySpace.hpp>
#include <hllm/engine.hpp>

#include "requester.hpp"

std::string requestOutput;
std::string decodedRequest1Output;
std::string decodedRequest2Output;
std::string transformedRequest1Output;
std::string preTransformedRequest;
std::string transformedRequest2Output;
std::string resultOutput;

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
  engine.registerFunction("Request Memory Allocate", [=](hLLM::Task *task) {
    // Getting incoming request
    const auto &requestMemSlot = task->getInput("Request");

    // Getting request
    const auto request = std::string((const char *)requestMemSlot->getPointer());
    printf("Request Memory Allocate: '%s'\n", request.c_str());

    // Create and register decoded requests
    decodedRequest1Output             = request + std::string(" [Decoded 1]");
    const auto decodedRequest1MemSlot = memoryManager->registerLocalMemorySlot(memorySpace, decodedRequest1Output.data(), decodedRequest1Output.size() + 1);
    task->setOutput("Request Memory Allocated", decodedRequest1MemSlot);
  });

  // Request transformation functions
  engine.registerFunction("Prompt Phase", [=](hLLM::Task *task) {
    // Getting incoming decoded request 1
    const auto &decodedRequest1MemSlot = task->getInput("Request Memory Allocated");
    const auto  decodedRequest1        = std::string((const char *)decodedRequest1MemSlot->getPointer());
    printf("Computing KV Cache for prompt: '%s'\n", decodedRequest1.c_str());

    // Create and register decoded requests
    transformedRequest1Output                   = decodedRequest1 + std::string(" [Transformed]");
    const auto transformedRequest1OutputMemSlot = memoryManager->registerLocalMemorySlot(memorySpace, transformedRequest1Output.data(), transformedRequest1Output.size() + 1);
    task->setOutput("KV Cache computed", transformedRequest1OutputMemSlot);
  });

  engine.registerFunction("Autoregressive Phase", [=](hLLM::Task *task) {
    // Getting incoming decoded request 1
    const auto &decodedRequest2MemSlot = task->getInput("KV Cache computed");
    const auto  decodedRequest2        = std::string((const char *)decodedRequest2MemSlot->getPointer());
    printf("Autoregression of Prompt: '%s'\n", decodedRequest2.c_str());

    // Create and register decoded requests
    preTransformedRequest                   = decodedRequest2 + std::string(" [Pre-Transformed]");
    const auto preTransformedRequestMemSlot = memoryManager->registerLocalMemorySlot(memorySpace, preTransformedRequest.data(), preTransformedRequest.size() + 1);
    task->setOutput("LLM Output", preTransformedRequestMemSlot);
  });

  engine.registerFunction("Respond Request", [=](hLLM::Task *task) {
    // Getting incoming decoded request 1
    const auto &transformedRequest1MemSlot = task->getInput("LLM Output");
    const auto  transformedRequest1        = std::string((const char *)transformedRequest1MemSlot->getPointer());

    // Producing response and sending it
    printf("Resulting Sequence '%s'\n", transformedRequest1.c_str());
    respondRequest(resultOutput);
  });
}
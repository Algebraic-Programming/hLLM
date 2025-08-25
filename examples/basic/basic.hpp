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
    requestOutput = std::string("This is request ") + std::to_string(requestId);
    const auto requestMemSlot = memoryManager->registerLocalMemorySlot(memorySpace, requestOutput.data(), requestOutput.size() + 1);
    task->setOutput("Request", requestMemSlot);
  });

  // Listen request function -- it expects an outside input and creates a request
  engine.registerFunction("Decode Request", [=](hLLM::Task *task) {
    // Getting incoming request
    const auto &requestMemSlot = task->getInput("Request");

    // Getting request
    const auto request = std::string((const char *)requestMemSlot->getPointer());
    printf("Decoding request: '%s'\n", request.c_str());

    // Create and register decoded requests
    decodedRequest1Output             = request + std::string(" [Decoded 1]");
    const auto decodedRequest1MemSlot = memoryManager->registerLocalMemorySlot(memorySpace, decodedRequest1Output.data(), decodedRequest1Output.size() + 1);
    task->setOutput("Decoded Request 1", decodedRequest1MemSlot);

    decodedRequest2Output             = request + std::string(" [Decoded 2]");
    const auto decodedRequest2MemSlot = memoryManager->registerLocalMemorySlot(memorySpace, decodedRequest2Output.data(), decodedRequest2Output.size() + 1);
    task->setOutput("Decoded Request 2", decodedRequest2MemSlot);
  });

  // Request transformation functions
  engine.registerFunction("Transform Request 1", [=](hLLM::Task *task) {
    // Getting incoming decoded request 1
    const auto &decodedRequest1MemSlot = task->getInput("Decoded Request 1");
    const auto  decodedRequest1        = std::string((const char *)decodedRequest1MemSlot->getPointer());
    printf("Transforming decoded request 1: '%s'\n", decodedRequest1.c_str());

    // Create and register decoded requests
    transformedRequest1Output                   = decodedRequest1 + std::string(" [Transformed]");
    const auto transformedRequest1OutputMemSlot = memoryManager->registerLocalMemorySlot(memorySpace, transformedRequest1Output.data(), transformedRequest1Output.size() + 1);
    task->setOutput("Transformed Request 1", transformedRequest1OutputMemSlot);
  });

  engine.registerFunction("Pre-Transform Request", [=](hLLM::Task *task) {
    // Getting incoming decoded request 1
    const auto &decodedRequest2MemSlot = task->getInput("Decoded Request 2");
    const auto  decodedRequest2        = std::string((const char *)decodedRequest2MemSlot->getPointer());
    printf("Pre-Transforming decoded request 2: '%s'\n", decodedRequest2.c_str());

    // Create and register decoded requests
    preTransformedRequest                   = decodedRequest2 + std::string(" [Pre-Transformed]");
    const auto preTransformedRequestMemSlot = memoryManager->registerLocalMemorySlot(memorySpace, preTransformedRequest.data(), preTransformedRequest.size() + 1);
    task->setOutput("Pre-Transform Request Output", preTransformedRequestMemSlot);
  });

  engine.registerFunction("Transform Request 2", [=](hLLM::Task *task) {
    // Create and register decoded requests
    const auto &preTransformedRequestOutputMemSlot = task->getInput("Pre-Transform Request Output");
    const auto  preTransformedRequestOutput        = std::string((const char *)preTransformedRequestOutputMemSlot->getPointer());
    printf("Transforming pre-transformed request 2: '%s'\n", preTransformedRequestOutput.c_str());

    transformedRequest2Output                   = preTransformedRequestOutput + std::string(" [Transformed]");
    const auto transformedRequest2OutputMemSlot = memoryManager->registerLocalMemorySlot(memorySpace, transformedRequest2Output.data(), transformedRequest2Output.size() + 1);
    task->setOutput("Transformed Request 2", transformedRequest2OutputMemSlot);
  });

  engine.registerFunction("Respond Request", [=](hLLM::Task *task) {
    // Getting incoming decoded request 1
    const auto &transformedRequest1MemSlot = task->getInput("Transformed Request 1");
    const auto  transformedRequest1        = std::string((const char *)transformedRequest1MemSlot->getPointer());

    const auto &transformedRequest2MemSlot = task->getInput("Transformed Request 2");
    const auto  transformedRequest2        = std::string((const char *)transformedRequest2MemSlot->getPointer());

    // Producing response and sending it
    resultOutput = transformedRequest1 + std::string(" + ") + transformedRequest2;
    printf("Joining '%s' + '%s' = '%s'\n", transformedRequest1.c_str(), transformedRequest2.c_str(), resultOutput.c_str());
    respondRequest(resultOutput);
  });
}
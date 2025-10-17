#pragma once

#include <string>
#include <array>

#include <hicr/core/memoryManager.hpp>
#include <hicr/core/memorySpace.hpp>
#include <hllm/engine.hpp>
#include "requester.hpp"

void createTasks(hLLM::Engine &engine, HiCR::MemoryManager *const memoryManager, std::shared_ptr<HiCR::MemorySpace> memorySpace)
{
  // Listen request function -- it expects an outside input and creates a request
  engine.registerFunction("Listen Request", [=](hLLM::Task *task) {
    // Getting input
    const auto &requestMemSlot = task->getInput("Prompt");
    const auto request = std::string((const char *)requestMemSlot->getPointer());

    // Create output
    std::string processedRequestOutput             = request + std::string(" [Processed]");
    const auto processedRequestMemSlot = memoryManager->registerLocalMemorySlot(memorySpace, processedRequestOutput.data(), processedRequestOutput.size() + 1);
    task->setOutput("Response", processedRequestMemSlot);
  });
}
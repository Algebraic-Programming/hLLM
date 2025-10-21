#pragma once

#include <string>
#include <array>

#include <hicr/core/memoryManager.hpp>
#include <hicr/core/memorySpace.hpp>
#include <hllm/engine.hpp>

// Permanent storage of the response output string (created only once to prevent memory leaking)
std::string responseOutput;

void createTasks(hLLM::Engine &engine, HiCR::MemoryManager *const memoryManager, std::shared_ptr<HiCR::MemorySpace> memorySpace)
{
  // Listen request function -- it expects an outside input and creates a request
  engine.registerFunction("Listen Request", [=](hLLM::Task *task) 
  {
    // Getting input
    const auto &requestMemSlot = task->getInput("Prompt");
    const auto request = std::string((const char *)requestMemSlot->getPointer());

    // Create output
    responseOutput             = request + std::string(" [Processed]");
    const auto responseMemSlot = memoryManager->registerLocalMemorySlot(memorySpace, responseOutput.data(), responseOutput.size() + 1);

    printf("[Basic Example] Returning response: '%s'\n", responseOutput.c_str());
    task->setOutput("Response", responseMemSlot);
  });
}
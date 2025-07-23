#include <stdio.h>
#include <fstream>
#include "llm-engine/llm-engine.hpp"

int main(int argc, char *argv[])
{
  // Creating LLM Engine object
  llmEngine::LLMEngine llmEngine;

  // Counter for incoming requests
  size_t requestCount  = 0;
  size_t totalRequests = 1;

  // Listen request function -- it expects an outside input and creates a request
  std::string requestOutput;
  llmEngine.registerFunction("Listen Request", [&](llmEngine::Task *task) {
    printf("Executing Listen Request\n");

    // Finish the LLM service if all requests have been processed
    // printf("Request Count: %lu / %lu\n", requestCount, totalRequests);
    if (requestCount >= totalRequests)
    {
      llmEngine.terminate();
      return;
    }

    // Simulate a delay between requests
    // if (requestCount > 0) sleep(1);

    // Create and register request as output
    requestOutput = std::string("This is request ") + std::to_string(requestCount);
    task->setOutput("Result", requestOutput.data(), requestOutput.size() + 1);

    // Advance request counter
    requestCount++;
  });

  llmEngine.registerFunction("Return Result", [&](llmEngine::Task *task) {
    // Getting incoming decoded request 1
    const auto &resultMsg = task->getInput("Result");
    const auto  result    = std::string((const char *)resultMsg.buffer);
    printf("Final Result: '%s'\n", result.c_str());
  });

  // Initializing LLM engine
  auto isRoot = llmEngine.initialize(&argc, &argv);

  // Let only the root instance deploy the LLM engine
  if (isRoot)
  {
    // Checking arguments
    if (argc != 2)
    {
      fprintf(stderr, "Error: Must provide the request file as argument.\n");
      llmEngine.abort();
      return -1;
    }

    // Getting config file name from arguments
    std::string configFilePath = std::string(argv[1]);

    // Parsing request file contents to a JSON object
    std::ifstream ifs(configFilePath);
    auto          configJs = nlohmann::json::parse(ifs);

    // Running LLM Engine
    llmEngine.run(configJs);
  }

  // Finalizing LLM engine
  llmEngine.finalize();
}
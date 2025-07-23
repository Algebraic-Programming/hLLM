#include <stdio.h>
#include <fstream>
#include "llm-engine/llm-engine.hpp"

int main(int argc, char *argv[])
{
  // Creating LLM Engine object
  llmEngine::LLMEngine llmEngine;

  // Listen request function -- it expects an outside input and creates a request
  std::string requestOutput;
  llmEngine.registerFunction("Hello World", [&](llmEngine::Task *task) {
    printf("Hello World\n");

    // This instance can terminate the llmEngine
    llmEngine.terminate();
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
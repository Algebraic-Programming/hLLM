#include <stdio.h>
#include <fstream>
#include "llm-engine/llm-engine.hpp"

int main(int argc, char *argv[])
{
  printf("Starting\n");

  llmEngine::LLMEngine llmEngine;
  llmEngine.initialize(&argc, &argv);

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

  printf("Finished Successfully\n");
}
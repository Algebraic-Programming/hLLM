#include <stdio.h>
#include <fstream>
#include "llm-engine/llm-engine.hpp"

int main(int argc, char *argv[])
{
  printf("Starting\n");

  llmEngine::LLMEngine llmEngine;

  // Registering functions
  llmEngine.registerFunction("Transform Request 1", []() { printf("Executing Transform Request 1\n"); });
  llmEngine.registerFunction("Transform Request 2", []() { printf("Executing Transform Request 2\n"); });
  llmEngine.registerFunction("Decode Request", []() { printf("Executing Decode Request\n"); });
  llmEngine.registerFunction("Listen Request", []() { printf("Executing Listen Request\n"); });
  llmEngine.registerFunction("Joiner", []() { printf("Executing Joiner\n"); });
  llmEngine.registerFunction("Pre-Transform Request", []() { printf("Executing Pre-Transform Request\n"); });
  llmEngine.registerFunction("Transform Request", []() { printf("Executing Transform Request\n"); });
  llmEngine.registerFunction("Return Result", []() { printf("Executing Return Result\n"); });

  // Initializing LLM engine
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
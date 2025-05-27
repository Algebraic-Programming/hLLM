#include <stdio.h>
#include "llm-engine/llm-engine.hpp"

int main(int argc, char *argv[])
{
  printf("Starting\n");

  llmEngine::LLMEngine llmEngine;
  llmEngine.initialize(&argc, &argv);

  printf("Finished Successfully\n");
}
#include <stdio.h>
#include <fstream>
#include "llm-engine/llm-engine.hpp"

int main(int argc, char *argv[])
{
  printf("Starting\n");

  llmEngine::LLMEngine llmEngine;
  
  // Counter for incoming requests
  size_t requestCount = 0;

  // Listen request function -- it expects an outside input and creates a request
  std::string requestOutput;
  llmEngine.registerFunction("Listen Request", [&](llmEngine::Task* task)
  { 
    printf("Executing Listen Request\n");
    
    // Simulate a delay between requests
    if (requestCount > 0) sleep(1);

    // Create and register request as output
    requestOutput = std::string("This is a request");
    task->setOutput("Request", requestOutput.data(), requestOutput.size() + 1);

    // Advance request counter
    requestCount++;
  });

  // Listen request function -- it expects an outside input and creates a request
  std::string decodedRequest1Output;
  std::string decodedRequest2Output;
  llmEngine.registerFunction("Decode Request", [&](llmEngine::Task* task)
  { 
    printf("Executing Decode Request\n"); 
    
    // Getting incoming request
    const auto& requestMsg = task->getInput("Request");

    // Getting request
    const auto request = std::string((const char*)requestMsg.buffer);
    printf("Decoding request: '%s'\n", request.c_str());

    // Create and register decoded requests
    decodedRequest1Output = request + std::string(" [Decoded 1]");
    task->setOutput("Decoded Request 1", decodedRequest1Output.data(), decodedRequest1Output.size() + 1);

    decodedRequest2Output = request + std::string(" [Decoded 2]");
    task->setOutput("Decoded Request 2", decodedRequest2Output.data(), decodedRequest2Output.size() + 1);
  });

  // Request transformation functions
  std::string transformedRequest1Output;
  llmEngine.registerFunction("Transform Request 1", [&](llmEngine::Task* task)
  { 
    // Getting incoming decoded request 1
    const auto& decodedRequest1Msg = task->getInput("Decoded Request 1");
    const auto decodedRequest1 = std::string((const char*)decodedRequest1Msg.buffer);
    printf("Transforming decoded request 1: '%s'\n", decodedRequest1.c_str());

    // Create and register decoded requests
    transformedRequest1Output = decodedRequest1 + std::string(" [Transformed]");
    task->setOutput("Transformed Request 1", transformedRequest1Output.data(), transformedRequest1Output.size() + 1);
  });

  std::string preTransformedRequest;
  llmEngine.registerFunction("Pre-Transform Request", [&](llmEngine::Task* task)
  { 
    // Getting incoming decoded request 1
    const auto& decodedRequest2Msg = task->getInput("Decoded Request 2");
    const auto decodedRequest2 = std::string((const char*)decodedRequest2Msg.buffer);
    printf("Pre-Transforming decoded request 2: '%s'\n", decodedRequest2.c_str());

    // Create and register decoded requests
    preTransformedRequest = decodedRequest2 + std::string(" [Pre-Transformed]");
  });

  std::string transformedRequest2Output;
  llmEngine.registerFunction("Transform Request 2", [&](llmEngine::Task* task)
  { 
    // Create and register decoded requests
    printf("Transforming pre-transformed request 2: '%s'\n", preTransformedRequest.c_str());
    transformedRequest2Output = preTransformedRequest + std::string(" [Transformed]");
    task->setOutput("Transformed Request 2", transformedRequest2Output.data(), transformedRequest2Output.size() + 1);
  });

  std::string resultOutput;
  llmEngine.registerFunction("Joiner", [&](llmEngine::Task* task) 
  {
    // Getting incoming decoded request 1
    const auto& transformedRequest1Msg = task->getInput("Transformed Request 1");
    const auto transformedRequest1 = std::string((const char*)transformedRequest1Msg.buffer);

    const auto& transformedRequest2Msg = task->getInput("Transformed Request 2");
    const auto transformedRequest2 = std::string((const char*)transformedRequest2Msg.buffer);

    resultOutput = transformedRequest1 + std::string(" + ") + transformedRequest2;
    printf("Joining '%s' + '%s' = '%s'\n", transformedRequest1.c_str(), transformedRequest2.c_str(), resultOutput.c_str());
    task->setOutput("Result", resultOutput.data(), resultOutput.size() + 1);
  });

  llmEngine.registerFunction("Return Result", [&](llmEngine::Task* task) 
  {
    // Getting incoming decoded request 1
    const auto& resultMsg = task->getInput("Result");
    const auto result = std::string((const char*)resultMsg.buffer);
    printf("Final Result: '%s'\n", result.c_str());
  });

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
import sys
import time
import json
from mpi4py import MPI

import llmEngine

comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

def main():
  # Creating LLM Engine object
  llmEngine_ = llmEngine.LLMEngine()
  
  # Counter for incoming requests
  requestCount = 0
  totalRequests = 32

  # Listen request function -- it expects an outside input and creates a request
  requestOutput = ""  # TODO: Why making this nonlocal?
  def fc(task):
    nonlocal requestCount
    nonlocal totalRequests

    nonlocal requestOutput

    print("Executing Listen Request")

    # Finish the LLM service if all requests have been processed
    print(f"Request Count: {requestCount} / {totalRequests}")
    if requestCount >= totalRequests:
      llmEngine_.terminate()
      return

    # Simulate a delay between requests
    if requestCount > 0: time.sleep(1)

    # Create and register request as output
    requestOutput = f"This is request {requestCount}"

    task.setOutput("Request", requestOutput)

    # Advance request counter
    requestCount += 1

  llmEngine_.registerFunction("Listen Request", fc)

  # Listen request function -- it expects an outside input and creates a request
  decodedRequest1Output = ""
  decodedRequest2Output = ""
  def fc(task):
    nonlocal decodedRequest1Output
    nonlocal decodedRequest2Output

    print("Executing Decode Request") 
    
    # Getting incoming request
    requestMsg = task.getInput("Request")

    # Getting request
    request = requestMsg.buffer
    print(f"Decoding request: '{request}'")

    # Create and register decoded requests
    decodedRequest1Output = request + " [Decoded 1]"
    task.setOutput("Decoded Request 1", decodedRequest1Output)

    decodedRequest2Output = request + " [Decoded 2]"
    task.setOutput("Decoded Request 2", decodedRequest2Output)

  llmEngine_.registerFunction("Decode Request", fc)

  # Request transformation functions
  transformedRequest1Output = ""
  def fc(task):
    nonlocal transformedRequest1Output

    # Getting incoming decoded request 1
    decodedRequest1Msg = task.getInput("Decoded Request 1")
    decodedRequest1 = decodedRequest1Msg.buffer
    print(f"Transforming decoded request 1: '{decodedRequest1}'")

    # Create and register decoded requests
    transformedRequest1Output = decodedRequest1 + " [Transformed]"

    task.setOutput("Transformed Request 1", transformedRequest1Output)

  llmEngine_.registerFunction("Transform Request 1", fc)

  preTransformedRequest = ""
  def fc(task):
    nonlocal preTransformedRequest

    # Getting incoming decoded request 1
    decodedRequest2Msg = task.getInput("Decoded Request 2")
    decodedRequest2 = decodedRequest2Msg.buffer
    print(f"Pre-Transforming decoded request 2: '{decodedRequest2}'")

    # Create and register decoded requests
    preTransformedRequest = decodedRequest2 + " [Pre-Transformed]"
  
  llmEngine_.registerFunction("Pre-Transform Request", fc)

  transformedRequest2Output = ""
  def fc(task):
    # Create and register decoded requests
    print(f"Transforming pre-transformed request 2: '{preTransformedRequest}'")
    transformedRequest2Output = preTransformedRequest + " [Transformed]"

    task.setOutput("Transformed Request 2", transformedRequest2Output)

  llmEngine_.registerFunction("Transform Request 2", fc)

  resultOutput = ""
  def fc(task):
    nonlocal resultOutput

    # Getting incoming decoded request 1
    transformedRequest1Msg = task.getInput("Transformed Request 1")
    transformedRequest1 = transformedRequest1Msg.buffer

    transformedRequest2Msg = task.getInput("Transformed Request 2")
    transformedRequest2 = transformedRequest2Msg.buffer

    resultOutput = transformedRequest1 + " + " + transformedRequest2
    print(f"Joining '{transformedRequest1}' + '{transformedRequest2}' = '{resultOutput}'")

    task.setOutput("Result", resultOutput)
  
  llmEngine_.registerFunction("Joiner", fc)

  def fc(task):
    # Getting incoming decoded request 1
    resultMsg = task.getInput("Result")
    result = resultMsg.buffer
    print(f"Final Result: '{result}'")
  
  llmEngine_.registerFunction("Return Result", fc)

  # Initializing LLM engine
  isRoot = llmEngine.initialize_engine(llmEngine_, sys.argv)

  # Let only the root instance deploy the LLM engine
  if isRoot:

    # Checking arguments
    if len(sys.argv) != 2:
      print("Error: Must provide the request file as argument.", file=sys.stderr)
      llmEngine_.abort()
      sys.exit(1)

    configFilePath = str(sys.argv[1])

    # Parsing request file contents to a JSON object
    with open(configFilePath, 'r') as file:
      configJs = json.load(file)

    # Running LLM Engine
    llmEngine_.run(configJs)

  # Finalizing LLM engine
  llmEngine_.finalize()


if __name__ == "__main__":
  main()
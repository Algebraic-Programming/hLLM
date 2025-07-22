import sys
import time
import json
from mpi4py import MPI

import llmEngine

def main():
  comm = MPI.COMM_WORLD
  rank = comm.Get_rank()
  size = comm.Get_size()

  # Creating LLM Engine object
  llmEngine_ = llmEngine.LLMEngine()

  def fc(task):
    # Getting incoming decoded requested result
    print("Before getInput", flush=True)
    resultMsg = task.getInput("Result")
    print("After getInput", flush=True)
    result = resultMsg.buffer
    print(f"Final Result: '{result}'")

  llmEngine_.registerFunction("Return Result", fc)
  
  # Counter for incoming requests
  requestCount = 0
  totalRequests = 2

  # Listen request function -- it expects an outside input and creates a request
  global requestOutput
  def fc(task):
    nonlocal requestCount
    nonlocal totalRequests

    print("Executing Listen Request")
    
    # Finish the LLM service if all requests have been processed
    print(f"Request Count: {requestCount} / {totalRequests}")
    if requestCount >= totalRequests:
      llmEngine_.terminate()
      return

    # Simulate a delay between requests
    # if (requestCount > 0) time.sleep(1)

    # Create and register request as output
    requestOutput = f"This is request {requestCount}"
    print("Before setOutput", flush=True)
    task.setOutput("Result", requestOutput)
    print("After setOutput", flush=True)

    # Advance request counter
    requestCount += 1

  llmEngine_.registerFunction("Listen Request", fc)


  # Initializing LLM engine
  isRoot = llmEngine_.initialize(sys.argv)
  
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
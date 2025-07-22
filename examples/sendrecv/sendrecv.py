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

  # Counter for incoming requests
  requestCount = 0
  totalRequests = 1

  # Listen request function -- it expects an outside input and creates a request
  requestOutput = ""
  def fc(task):
    nonlocal requestCount
    nonlocal totalRequests
    print("Executing Listen Request")
    
    # Finish the LLM service if all requests have been processed
    # print(f"Request Count: {requestCount} / {totalRequests}")
    if requestCount >= totalRequests:
      llmEngine_.terminate()
      return

    # Simulate a delay between requests
    # if (requestCount > 0) time.sleep(1)

    # Create and register request as output
    requestOutput = f"This is request {requestCount}"
    task.setOutput("Result", requestOutput)

    # Advance request counter
    requestCount += 1

  llmEngine_.registerFunction("Listen Request", fc)

  def fc(task):
    # Getting incoming decoded requested result
    resultMsg = task.getInput("Result")
    result = resultMsg.buffer
    print(f"Final Result: '{result}'")

  llmEngine_.registerFunction("Return Result", fc)

  # Initializing LLM engine
  isRoot = llmEngine_.initialize(sys.argv)

  print(f"[Rank {rank}/{size}] Hello from MPI process and is Root: {isRoot}.")
  
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

    # Now `configJs` is a Python dictionary (or list, depending on the file content)
    print(configJs)

    # Running LLM Engine
    llmEngine_.run(configJs)

  # Finalizing LLM engine
  llmEngine_.finalize()


if __name__ == "__main__":
  main()
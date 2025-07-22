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
    print("Hello World")

    # This instance can terminate the llmEngine
    llmEngine_.terminate()

  llmEngine_.registerFunction("Hello World", fc)

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
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

  llmEngine_.registerFunction("Hello World", lambda task : print(f"hello I am: {task}"))

  # Initializing LLM engine
  isRoot = llmEngine.initialize_engine(llmEngine_, sys.argv)

  print(f"[Rank {rank}/{size}] Hello from MPI process and is Root: {isRoot}.")
  
  # # Let only the root instance deploy the LLM engine
  # if isRoot:

  #   # Checking arguments
  #   if len(sys.argv) != 2:
  #     print("Error: Must provide the request file as argument.", file=sys.stderr)
  #     llmEngine_.abort()
  #     sys.exit(1)

  #   configFilePath = str(sys.argv[1])

  #   # Parsing request file contents to a JSON object
  #   with open(configFilePath, 'r') as file:
  #     configJs = json.load(file)

  #   # Now `configJs` is a Python dictionary (or list, depending on the file content)
  #   print(configJs)

  #   # Running LLM Engine
  #   llmEngine_.run(configJs)

  # # Finalizing LLM engine
  # llmEngine_.finalize()


if __name__ == "__main__":
  main()
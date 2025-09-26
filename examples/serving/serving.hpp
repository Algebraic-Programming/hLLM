#pragma once

#include <string>
#include <torch/torch.h>
#include <iostream>

#include <hicr/core/memoryManager.hpp>
#include <hicr/core/memorySpace.hpp>
#include <hllm/engine.hpp>

#include "requester.hpp"
#include "attention.hpp"

size_t B; // batch size
constexpr size_t n        = 8;             // max sequence length
constexpr size_t d        = 4;             // hidden state size (or number of possible tokens)
constexpr size_t p_size   = 3;             // prompt size (this one will later be flexible)

// Sequences tensor and KV cache tensor the given prompts
torch::Tensor o_Output;
torch::Tensor KV_cache_Output;

// query, key, value matrices filled with random floats (Atm global)
torch::Tensor Wq;
torch::Tensor Wk;
torch::Tensor Wv;


void createTasks(hLLM::Engine &engine, HiCR::MemoryManager *const memoryManager, std::shared_ptr<HiCR::MemorySpace> memorySpace)
{
  B = _requestCount; // Atm batch size is the same size as the total number of requests

  // Some assertion
  assert(B == _requestCount && "Atm, the total number of request should match with the batch size");
  // assert(B == 1 && "For simplicity, the batch size is fixed to one. Batching may come later");

  // initializing the LLM weights (globally)
  torch::manual_seed(42);

  Wq = torch::rand({d,d});
  Wk = torch::rand({d,d});
  Wv = torch::rand({d,d});

  // Listen request function -- it expects an outside input and creates a request
  engine.registerFunction("Listen Request", [=](hLLM::Task *task) {

    // Listening to incoming requests (emulates an http service)
    printf("Listening to incoming requests...\n");
    requestId_t requestId = 0;
    for(size_t b = 0; b < B; ++b)
    {
      task->waitFor([&]() { return listenRequest(requestId); });
      auto prompt = torch::rand({p_size, d});
      std::cout << "Request: " << requestId << " is\n" << prompt << "\n";

    }
    
    // Allocating memory for the requested prompts
    KV_cache_Output = torch::zeros({int64_t(B), 2, n, d});
    o_Output        = torch::zeros({int64_t(B), n, d});
    
    o_Output[0].slice(0, 0, p_size) = torch::rand({p_size, d});
    std::cout << "All prompts: \n" << o_Output << "\n";

    // Register prompts and KV_cache as output
    const auto promptsMemSlot = memoryManager->registerLocalMemorySlot(memorySpace, o_Output.data_ptr(), o_Output.nbytes() + 1);
    task->setOutput("Prompts1", promptsMemSlot);

    const auto KVcacheMemSlot = memoryManager->registerLocalMemorySlot(memorySpace, KV_cache_Output.data_ptr(), KV_cache_Output.nbytes() + 1);
    task->setOutput("KV_Cache1", KVcacheMemSlot);
  });

  // Prompt Phase: computing the KV_cache for the given prompts
  engine.registerFunction("Prompt Phase", [=](hLLM::Task *task) {
    printf("Prompt Phase\n"); fflush(stdout);

    // Getting incoming prompts and their respective KV_cache
    const auto &promptsMemSlot = task->getInput("Prompts1");
    const auto  o        = *(torch::Tensor *)promptsMemSlot->getPointer();

    const auto &KVcacheMemSlot = task->getInput("KV_Cache1");
    auto  KV_cache       = *(torch::Tensor *)KVcacheMemSlot->getPointer();

    std::cout << "Computing KV Cache for prompt: " << o << "\n";

    // Computing the KV cache
    for(size_t b = 0; b < B; ++b){
        for(size_t i = 0; i < p_size; ++i){
            KV_cache[b][0][i] = torch::matmul(Wk, o[b][i]);
            KV_cache[b][1][i] = torch::matmul(Wv, o[b][i]);
        }
    }

    // Register prompts and KV_cache as output
    const auto promptsMemSlot2 = memoryManager->registerLocalMemorySlot(memorySpace, o_Output.data_ptr(), o_Output.nbytes() + 1);
    task->setOutput("Prompts2", promptsMemSlot2);

    const auto KVcacheMemSlot2 = memoryManager->registerLocalMemorySlot(memorySpace, KV_cache_Output.data_ptr(), KV_cache_Output.nbytes() + 1);
    task->setOutput("KV_Cache2", KVcacheMemSlot2);
  });

  engine.registerFunction("Autoregressive Phase", [=](hLLM::Task *task) {
    printf("Autoregressive Phase\n"); fflush(stdout);

    // Getting incoming prompts and their respective KV_cache
    const auto &promptsMemSlot = task->getInput("Prompts2");
    auto  o        = *(torch::Tensor *)promptsMemSlot->getPointer();

    const auto &KVcacheMemSlot = task->getInput("KV_Cache2");
    auto  KV_cache = *(torch::Tensor *)KVcacheMemSlot->getPointer();

    // autoregressive phase (in our case until max sequence length (i.e. not iteration-level scheduling))
    for(size_t b = 0; b < B; ++b){
        for(size_t i = p_size; i < n; ++i){
            auto q            = torch::matmul(Wq, o[b][i]);
            KV_cache[b][0][i] = torch::matmul(Wk, o[b][i]);
            KV_cache[b][1][i] = torch::matmul(Wv, o[b][i]);

            single_attention(q, KV_cache, o, i, b);

            std::cout << "b: " << b << ", i: " << i << ", o: " << o << "\n";
        }
    }

    // Register prompts and KV_cache as output
    const auto promptsMemSlot3 = memoryManager->registerLocalMemorySlot(memorySpace, o_Output.data_ptr(), o_Output.nbytes());
    task->setOutput("Prompts3", promptsMemSlot3);
  });

  engine.registerFunction("Respond Request", [=](hLLM::Task *task) {
    printf("Respond Request\n"); fflush(stdout);

    // Getting incoming prompts and their respective KV_cache
    const auto &promptsMemSlot = task->getInput("Prompts3");
    const auto  o              = *(torch::Tensor *)promptsMemSlot->getPointer();

    // Producing response and sending it
    std::cout << "Resulting Sequence:\n" << o << "\n";
    respondRequest();
  });
}
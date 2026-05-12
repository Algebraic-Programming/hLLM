#pragma once

#include <vector>
#include <memory>
#include <thread>

#include <hicr/core/instance.hpp>

#include <system/channels/message.hpp>
#include <system/channels/input.hpp>
#include <system/channels/output.hpp>

#define __HLLM_TELEPHONE_GAME_MESSAGE_TYPE 42

__INLINE__ void telephoneGame(std::vector<std::shared_ptr<hLLM::system::channels::Input>>  &inputs,
                              std::vector<std::shared_ptr<hLLM::system::channels::Output>> &outputs,
                              const HiCR::Instance::instanceId_t                            instanceId,
                              const bool                                                    isRoot)
{
  if (inputs.size() > 1 || outputs.size() > 1) HICR_THROW_LOGIC("This telephone game example only supports one input and one output channel per instance.");
  if (inputs.empty() && outputs.empty()) HICR_THROW_LOGIC("This telephone game example requires at least an input or an output channel.");

  const auto &inputChannel  = inputs[0];
  const auto &outputChannel = outputs[0];

  if (isRoot)
  {
    // Root instance starts the game by sending a message to the next instance
    const std::string text = "Hello from root instance!";
    printf("[Instance %lu][TelephoneGame] Sending message: %s\n", instanceId, text.c_str());
    auto input = hLLM::system::channels::Message(reinterpret_cast<const uint8_t *>(text.data()), text.size(), hLLM::system::channels::Message::metadata_t{});
    outputChannel->pushMessageLocking(input);

    // wait for the return message
    while (inputChannel->hasMessage() == false) { std::this_thread::sleep_for(std::chrono::milliseconds(500)); }
    auto output = inputChannel->getMessage();

    printf("[Instance %lu][TelephoneGame] Received message: %s\n", instanceId, std::string(reinterpret_cast<const char *>(output.getData()), output.getSize()).c_str());
  }
  else
  {
    while (inputChannel->hasMessage() == false) { std::this_thread::sleep_for(std::chrono::milliseconds(500)); }
    auto input = inputChannel->getMessage();

    printf("[Instance %lu][TelephoneGame] Received message: %s\n", instanceId, std::string(reinterpret_cast<const char *>(input.getData()), input.getSize()).c_str());

    auto text = std::string(reinterpret_cast<const char *>(input.getData()), input.getSize());
    printf("[Instance %lu][TelephoneGame] Sending message: %s\n", instanceId, text.c_str());
    auto output = hLLM::system::channels::Message(reinterpret_cast<const uint8_t *>(text.data()), text.size(), hLLM::system::channels::Message::metadata_t{});
    outputChannel->pushMessageLocking(output);
  }
}
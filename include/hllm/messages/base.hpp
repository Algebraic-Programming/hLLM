#pragma once

#include "../edge/message.hpp"
#include <hicr/core/exceptions.hpp>
#include <hicr/core/definitions.hpp>

namespace hLLM::messages
{

// Do not use zero, as it might be confused with a blank (erroneous) message, producing a false positive
#define __HLLM__BASE_MESSAGE_ID__ 128
enum messageTypes : edge::Message::messageType_t
{
  heartbeat = __HLLM__BASE_MESSAGE_ID__ + 0,
  prompt = __HLLM__BASE_MESSAGE_ID__ + 1,
  data = __HLLM__BASE_MESSAGE_ID__ + 2,
  replicaReady = __HLLM__BASE_MESSAGE_ID__ + 3
};

class Base
{
  public:

  virtual void decode(const edge::Message& rawMessage) = 0;
  virtual edge::Message encode() const = 0;
  virtual edge::Message::messageType_t getType() const = 0;

  protected:

  Base() = default;
  virtual ~Base() = default;
}; // class Base

} // namespace hLLM
#pragma once

#include "../edge/message.hpp"
#include <hicr/core/exceptions.hpp>
#include <hicr/core/definitions.hpp>

namespace hLLM::messages
{

enum messageTypes : edge::Message::messageType_t
{
  heartbeat = 0,
  prompt = 1
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
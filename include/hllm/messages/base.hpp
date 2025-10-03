#pragma once

#include "../edge/message.hpp"
#include <hicr/core/exceptions.hpp>
#include <hicr/core/definitions.hpp>

namespace hLLM::messages
{

enum messageTypes : edge::Message::messageType_t
{
  heartbeatPing = 0,
  heartbeatPong = 0
};

class Base
{
  public:

  protected:

  Base() = default;
  virtual ~Base() = default;
  virtual void decode(const edge::Message& rawMessage) = 0;
  virtual edge::Message encode() const = 0;
  virtual edge::Message::messageType_t getType() const = 0;

}; // class Base

} // namespace hLLM
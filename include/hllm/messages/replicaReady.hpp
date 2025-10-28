#pragma once

#include "base.hpp"

namespace hLLM::messages
{

class ReplicaReady final : public Base
{
  private:

  const edge::Message::messageType_t _type = messageTypes::replicaReady;

  public:

  ReplicaReady(const edge::Message& rawMessage) : Base()
  {
    decode(rawMessage);
  }
  ReplicaReady() : Base() { }
  ~ReplicaReady() = default;

  __INLINE__ edge::Message::messageType_t getType() const override { return _type; }

  __INLINE__ void decode(const edge::Message& rawMessage) override
  {
  }

  __INLINE__ edge::Message encode() const override
  { 
    edge::Message rawMessage(nullptr, 0, edge::Message::metadata_t( { .type = getType(), .sessionId = 0, .messageId = 0 }));

    return rawMessage;
  }

  private:

}; // class ReplicaReady

} // namespace hLLM::messages
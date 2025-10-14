#pragma once

#include "base.hpp"

namespace hLLM::messages
{

class Heartbeat final : public Base
{
  private:

  const edge::Message::messageType_t _type = messageTypes::heartbeat;
  const std::string _signature = "Heartbeat";

  public:

  Heartbeat() = default;
  Heartbeat(const edge::Message& rawMessage) : Base()
  {
    decode(rawMessage);
  }
  ~Heartbeat() = default;

  __INLINE__ edge::Message::messageType_t getType() const override { return _type; }

  __INLINE__ void decode(const edge::Message& rawMessage) override
  {
    const auto messageType = rawMessage.getMetadata().type;
    if (messageType != getType()) HICR_THROW_RUNTIME("Message type %lu being decoded by class of type %lu. This is a bug in hLLM", messageType, getType());

    const auto signatureSize = rawMessage.getSize();
    const std::string signature = std::string((const char*)rawMessage.getData());
    if (signatureSize != _signature.size() + 1) HICR_THROW_RUNTIME("Heartbeat message has signature incorrect size (%lu != %lu, Recevied: '%s'). This is a bug in hLLM", signatureSize, _signature.size() + 1, signature.c_str());
    if (signature != _signature) HICR_THROW_RUNTIME("Heartbeat message has incorrect signature ('%s' != '%s'). This is a bug in hLLM", signature.c_str(), _signature.c_str());
  }

  __INLINE__ edge::Message encode() const override
  { 
    edge::Message rawMessage((uint8_t*)_signature.data(), _signature.size()+1, edge::Message::metadata_t( { .type = getType(), .messageId = 0, .sessionId = 0 }));

    return rawMessage;
  }

  private:
  
}; // class Heartbeat

} // namespace hLLM::messages
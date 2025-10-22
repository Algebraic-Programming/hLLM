#pragma once

#include "base.hpp"
#include "../prompt.hpp"

namespace hLLM::messages
{

class Data final : public Base
{
  private:

  const edge::Message::messageType_t _type = messageTypes::data;

  public:

  Data() = default;
  Data(const edge::Message& rawMessage) : Base()
  {
    decode(rawMessage);
  }
  Data(const uint8_t* data, const size_t size, Prompt::promptId_t promptId) : 
    Base(),
    _data(data),
    _size(size),
    _promptId(promptId) { }
  ~Data() = default;

  __INLINE__ edge::Message::messageType_t getType() const override { return _type; }

  __INLINE__ void decode(const edge::Message& rawMessage) override
  {
    const auto messageType = rawMessage.getMetadata().type;
    if (messageType != getType()) HICR_THROW_RUNTIME("Message type %lu being decoded by class of type %lu. This is a bug in hLLM", messageType, getType());

    _data = rawMessage.getData();
    _size = rawMessage.getSize();
    _promptId = { rawMessage.getMetadata().sessionId, rawMessage.getMetadata().messageId };
  }

  __INLINE__ edge::Message encode() const override
  { 
    edge::Message rawMessage(_data, _size, edge::Message::metadata_t( { .type = getType(), .sessionId = _promptId.first, .messageId = _promptId.second }));

    return rawMessage;
  }

  __INLINE__ const uint8_t* getData() const { return _data; }
  __INLINE__ const size_t getSize() const { return _size; }
  __INLINE__ const Prompt::promptId_t getPromptId() const { return _promptId; }

  private:
  
  const uint8_t* _data;
  size_t _size;
  Prompt::promptId_t _promptId;

}; // class Prompt

} // namespace hLLM::messages
#pragma once

#include "base.hpp"

namespace hLLM::messages
{

class UserInput final : public Base
{
  private:

  const edge::Message::messageType_t _type = messageTypes::userInput;

  public:

  UserInput() = default;
  UserInput(const edge::Message& rawMessage) : Base()
  {
    decode(rawMessage);
  }
  UserInput(const std::string& input, sessionId_t sessionId, requestId_t requestId) : 
    Base(),
    _input(input),
    _sessionId(sessionId),
    _requestId(requestId) { }
  ~UserInput() = default;

  __INLINE__ edge::Message::messageType_t getType() const override { return _type; }

  __INLINE__ void decode(const edge::Message& rawMessage) override
  {
    const auto messageType = rawMessage.getMetadata().type;
    if (messageType != getType()) HICR_THROW_RUNTIME("Message type %lu being decoded by class of type %lu. This is a bug in hLLM", messageType, getType());
  }

  __INLINE__ edge::Message encode() const override
  { 
    edge::Message rawMessage((uint8_t*)_input.data(), _input.size()+1, edge::Message::metadata_t( { .type = getType(), .requestId = _requestId, .sessionId = _sessionId }));

    return rawMessage;
  }

  __INLINE__ const std::string& getInput() const { return _input; }
  __INLINE__ const sessionId_t getSessionId() const { return _sessionId; }
  __INLINE__ const requestId_t getRequestId() const { return _requestId; }

  private:
  
  std::string _input;
  sessionId_t _sessionId;
  requestId_t _requestId;

}; // class UserInput

} // namespace hLLM::messages
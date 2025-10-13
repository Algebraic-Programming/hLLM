#pragma once

#include <cstdint>
#include <mutex>
#include <queue>
#include "messages/prompt.hpp"

namespace hLLM
{

class Engine;

class Session
{
  public:

  friend class Engine;

  Session() = delete;
  ~Session() = default;

  Session(const sessionId_t sessionId) :
  _sessionId(sessionId)
  {
    _currentMessageId = 0;
  }

  __INLINE__ const sessionId_t getSessionId() const { return _sessionId; }
  
  __INLINE__ messageId_t sendPrompt(const std::string& prompt)
  {
    // Getting and increasing message id
    const auto messageId = _currentMessageId++;

    auto message = std::make_shared<nlohmann::json>();
    message.operator*()["Type"] = "Prompt";
    message.operator*()["Message Id"] = messageId;
    message.operator*()["Prompt"] = prompt;

    // Emulates online connection
    _rawMessageQueueLock.lock();
    _rawMessageQueue.push(message);
    _rawMessageQueueLock.unlock();

    return messageId;
  }

  __INLINE__ bool isConnected() const { return _isConnected; }

  private:

  __INLINE__ void connect() { _isConnected = true; }
  __INLINE__ void disconnect() { _isConnected = false; }

  __INLINE__ const std::shared_ptr<messages::Base> getMessage()
  {
    // Getting a raw message from the queue.
    std::shared_ptr<nlohmann::json> rawMessage = nullptr;

    _rawMessageQueueLock.lock();
    if (_rawMessageQueue.empty() == false)
    {
      rawMessage = _rawMessageQueue.front();
      _rawMessageQueue.pop();
    }
    _rawMessageQueueLock.unlock();

    // Returning nullptr, if the queue was empty
    if (rawMessage == nullptr) return nullptr;

    // Getting message type
    const auto type = hicr::json::getString(*rawMessage, "Type");
    const auto messageId = hicr::json::getNumber<messageId_t>(*rawMessage, "Message Id");

    // Holder for the new message to return
    std::shared_ptr<messages::Base> message = nullptr;

    // Decoding message according to type
    bool isRecognized = false;
    if (type == "Prompt")
    {
      const auto prompt = hicr::json::getString(*rawMessage, "Prompt");
      message = std::make_shared<messages::Prompt>(prompt, _sessionId, messageId);
      isRecognized = true;
    }

    // Check if the message type was recognized
    if (isRecognized == false) HICR_THROW_LOGIC("Unrecognized request type '%s' for session %lu. This must be a bug in hLLM", type.c_str(), _sessionId);

    // Return message
    return message;
  }

  bool _isConnected = false;
  const sessionId_t _sessionId;
  std::mutex _rawMessageQueueLock;
  std::queue<std::shared_ptr<nlohmann::json>> _rawMessageQueue;
  messageId_t _currentMessageId;

}; // class Session

} // namespace hLLM
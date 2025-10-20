#pragma once

#include <cstdint>
#include <mutex>
#include <queue>
#include "prompt.hpp"

namespace hLLM
{

namespace roles 
{
  class RequestManager;
}

class Session
{
  public:

  friend class roles::RequestManager;

  Session() = delete;
  ~Session() = default;

  Session(const sessionId_t sessionId) :
  _sessionId(sessionId)
  {
    _currentMessageId = 0;
  }

  __INLINE__ const sessionId_t getSessionId() const { return _sessionId; }
  
  __INLINE__ std::shared_ptr<Prompt> pushPrompt(const std::string& prompt)
  {
    // Getting and increasing message id
    const auto messageId = _currentMessageId++;

    // Creating prompt object
    const auto promptId = Prompt::promptId_t({_sessionId, messageId});
    const auto promptObject = std::make_shared<Prompt>(promptId, prompt);
    _promptMutex.lock();
    _promptMap.insert({promptId, promptObject});
    _newPromptQueue.push(promptObject);
    _promptMutex.unlock();

    return promptObject;
  }

  __INLINE__ bool isConnected() const { return _isConnected; }

  private:

  __INLINE__ void connect() { _isConnected = true; }
  __INLINE__ void disconnect() { _isConnected = false; }

  __INLINE__ const std::shared_ptr<Prompt> getPrompt()
  {
    std::shared_ptr<Prompt> prompt = nullptr;

    // Getting prompt from the queue, if there's any
    _promptMutex.lock();
    if (_newPromptQueue.empty() == false) { prompt = _newPromptQueue.front(); _newPromptQueue.pop(); }
    _promptMutex.unlock();

    return prompt;
  }

  bool _isConnected = false;
  const sessionId_t _sessionId;
  messageId_t _currentMessageId;

  // Mutual exclusion for managing prompts
  std::mutex _promptMutex;

  // This map connects a prompt ids with their prompt objects for prompt/response assignment
  std::map<Prompt::promptId_t, std::shared_ptr<Prompt>> _promptMap;

  // This queue hold the prompt until they are started by the request manager
  std::queue<std::shared_ptr<Prompt>> _newPromptQueue;

}; // class Session

} // namespace hLLM
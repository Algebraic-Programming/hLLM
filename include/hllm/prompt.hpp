#pragma once

#include <cstdint>
#include <hicr/core/definitions.hpp>

namespace hLLM
{

namespace roles 
{
  class RequestManager;
}

class Prompt
{
  public:

  friend class roles::RequestManager;

  typedef std::pair<sessionId_t, messageId_t>  promptId_t;

  Prompt() = delete;
  ~Prompt() = default;

  Prompt(const promptId_t promptId, const std::string& prompt) :
          _promptId(promptId),
          _prompt(prompt)
  {
  }

  [[nodiscard]] __INLINE__ auto hasResponse() const { return _hasResponse; }
  [[nodiscard]] __INLINE__ const std::string& getResponse() const { return _response; }
  [[nodiscard]] __INLINE__ promptId_t getPromptId() const { return _promptId; }
  [[nodiscard]] __INLINE__ const std::string& getPrompt() const { return _prompt; }

  private:

  __INLINE__ void setResponse(const std::string& response) { _response = response; _hasResponse = true; }

  const promptId_t _promptId;
  const std::string _prompt;
  std::string _response;
  volatile bool _hasResponse = false;

}; // class Prompt

} // namespace hLLM
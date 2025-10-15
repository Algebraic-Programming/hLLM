#pragma once

#include <cstdint>

namespace hLLM
{

class Prompt
{
  public:

  typedef std::pair<sessionId_t, messageId_t>  promptId_t;

  Prompt() = delete;
  ~Prompt() = default;

  Prompt(const promptId_t promptId, const std::string& prompt) :
          _promptId(promptId),
          _prompt(prompt)
  {
  }

  __INLINE__ void setResponse(const std::string& response) { _response = response; }
  __INLINE__ promptId_t getPromptId() const { return _promptId; }
  __INLINE__ const std::string& getPrompt() const { return _prompt; }

  private:

  const promptId_t _promptId;
  const std::string _prompt;
  std::string _response;

}; // class Prompt

} // namespace hLLM
#pragma once

#include <cstdint>

namespace hLLM
{

class Prompt
{
  public:

  #pragma pack(push, 1)
  struct promptId_t
  {
    sessionId_t sessionId;
    messageId_t messageId;
  };
  #pragma pack(pop)

  Prompt() = delete;
  ~Prompt() = default;

  Prompt(const promptId_t promptId, const std::string& prompt) :
          _promptId(promptId),
          _prompt(prompt)
  {
  }

  __INLINE__ void setResponse(const std::string& response) { _response = response; }

  private:

  const promptId_t _promptId;
  const std::string _prompt;
  std::string _response;

}; // class Prompt

} // namespace hLLM
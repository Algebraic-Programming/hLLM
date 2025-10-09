#pragma once

#include <cstdint>

namespace hLLM
{

class Session
{
  public:

  typedef uint64_t sessionId_t;

  Session() = delete;
  ~Session() = default;

  Session(
    const sessionId_t sessionId
  ) 
  {
  }

  private:

}; // class Session

} // namespace hLLM
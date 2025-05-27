
#pragma once

#include <hicr/core/exceptions.hpp>
#include <hicr/core/definitions.hpp>
#include <deployr/deployr.hpp>

namespace llmEngine
{

class LLMEngine final
{
  public:

  LLMEngine()
  {
  }

  __INLINE__ void initialize(int *pargc, char ***pargv)
  {
    // Initializing DeployR
    _deployr.initialize(pargc, pargv);

  }

  ~LLMEngine() = default;

  private:

  // DeployR instance
  deployr::DeployR _deployr;

}; // class LLMEngine

} // namespace llmEngine
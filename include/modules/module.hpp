#pragma once

#include <hicr/core/definitions.hpp>
#include <taskr/service.hpp>

#include <system/channels/dispatcher.hpp>

namespace hLLM::modules
{
class Module
{
  public:

  Module() = default;

  Module(const size_t interval)
  {
    _service = std::make_unique<taskr::Service>([&]() { run(); }, interval);
  }

  virtual ~Module() = default;

  __INLINE__ bool hasService() const { return _service != nullptr; }
  __INLINE__ taskr::Service *getService() const { return _service.get(); }

  virtual void initialize() = 0;
  virtual void finalize()   = 0;

  protected:

  std::unique_ptr<taskr::Service> _service = nullptr;

  virtual void run() = 0;
};

} // namespace hLLM::modules
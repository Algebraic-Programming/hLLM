#pragma once

#include <cstddef>

namespace hLLM::channel
{
enum dependencyType
{
  buffered,
  unbuffered
};

struct unbufferedData_t
{
  void             *ptr;
  const std::size_t size;
};
} // namespace hLLM::channel
#pragma once

#include <cstdint>
#include <cstdlib>

#include <hicr/core/definitions.hpp>

namespace hLLM::system::channels
{

class Message final
{
  public:

  typedef uint64_t messageType_t;
  typedef uint64_t groupId_t;
  typedef uint64_t messageId_t;

#pragma pack(push, 1)
  struct metadata_t
  {
    messageType_t type = 0;
    groupId_t     groupId = 0;
    messageId_t   messageId = 0;
  };
#pragma pack(pop)

  Message() = delete;

  Message(const uint8_t *const data, const size_t size, const metadata_t metadata)
    : _data(data),
      _size(size),
      _metadata(metadata)
  {}

  ~Message() = default;

  [[nodiscard]] __INLINE__ const uint8_t    *getData() const { return _data; }
  [[nodiscard]] __INLINE__ size_t            getSize() const { return _size; }
  [[nodiscard]] __INLINE__ const metadata_t &getMetadata() const { return _metadata; }

  private:

  const uint8_t *const _data;
  const size_t         _size;
  const metadata_t     _metadata;
}; // class Message
} // namespace hLLM::system::channels
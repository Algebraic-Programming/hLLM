#pragma once

#include <cstdint>
#include <cstdlib>

#include <hicr/core/definitions.hpp>

namespace hLLM::system::channels
{

class Message final
{
  public:

  // 10 bits for type (1024))
  using messageType_t = uint16_t;

  // 20 bits for type (1M))
  using groupId_t = uint32_t;
  // 34 bits for type (17B))
  using sequenceId_t = uint64_t;

  // Id type
  using messageId_t = uint64_t;

  static constexpr messageId_t TYPE_BITS     = 10; // 10 bits for type (1024))
  static constexpr messageId_t GROUP_BITS    = 20; // 20 bits for type (1M))
  static constexpr messageId_t SEQUENCE_BITS = 34; // 34 bits for type (17B))

  static constexpr messageId_t MAX_TYPE     = (messageId_t{1} << TYPE_BITS) - 1;
  static constexpr messageId_t MAX_GROUP    = (messageId_t{1} << GROUP_BITS) - 1;
  static constexpr messageId_t MAX_SEQUENCE = (messageId_t{1} << SEQUENCE_BITS) - 1;

#pragma pack(push, 1)
  struct metadata_t
  {
    messageType_t type       = 0;
    groupId_t     groupId    = 0;
    sequenceId_t  sequenceId = 0;

    bool isValid() const
    {
      return static_cast<messageId_t>(type) <= MAX_TYPE && static_cast<messageId_t>(groupId) <= MAX_GROUP && static_cast<messageId_t>(sequenceId) <= MAX_SEQUENCE;
    }

    messageId_t getId() const
    {
      return (static_cast<messageId_t>(type) << (GROUP_BITS + SEQUENCE_BITS)) | (static_cast<messageId_t>(groupId) << SEQUENCE_BITS) | static_cast<messageId_t>(sequenceId);
    }
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
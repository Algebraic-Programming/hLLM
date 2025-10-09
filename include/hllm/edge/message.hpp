#pragma once

#include <cstdint>
#include <cstdlib>
#include "../request.hpp"
#include "../session.hpp"

namespace hLLM::edge
{

class Message final
{
  public:

  typedef uint8_t messageType_t;

  #pragma pack(push, 1)
  struct metadata_t
  {
    messageType_t type;
    hLLM::Request::requestId_t requestId;
    hLLM::Session::sessionId_t sessionId;
  };
  #pragma pack(pop)

  Message() = delete;
  Message(const uint8_t* const data, const size_t size, const metadata_t metadata) :
   _data(data),
   _size(size),
   _metadata(metadata)
  { }

  virtual ~Message() = default;

  const uint8_t* getData() const { return _data; }
  size_t getSize() const { return _size; }
  const metadata_t& getMetadata() const { return _metadata; }

  private:

  const uint8_t* const _data;
  const size_t _size;
  const metadata_t _metadata;
  
}; // class Message

} // namespace hLLM::edge
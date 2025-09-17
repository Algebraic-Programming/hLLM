#pragma once

#include <cstdint>
#include <string>
#include <hicr/core/definitions.hpp>
#include <nlohmann_json/parser.hpp>

namespace hLLM::configuration
{

class Channel
{
  public:

  Channel(const nlohmann::json& js) { deserialize(js); } 
  Channel(const std::string& name, const std::string& mode, const size_t bufferCapacity, const size_t bufferSize = 0)
   :  _name(name),
      _mode(mode),
     _bufferCapacity(bufferCapacity),
     _bufferSize(bufferSize)
  {  validate(); }
  virtual ~Channel() = default;

  __INLINE__ void setName(const std::string& name) { _name = name; }
  [[nodiscard]] __INLINE__ std::string getName() const { return _name; }
  [[nodiscard]] __INLINE__ size_t getBufferCapacity() const { return _bufferCapacity; }
  [[nodiscard]] __INLINE__ size_t getBufferSize() const { return _bufferSize; }
  [[nodiscard]] __INLINE__ std::string getMode() const { return _mode; }
  
  [[nodiscard]] __INLINE__ nlohmann::json serialize() const
  {
    nlohmann::json js;

    js["Name"] = _name;
    js["Mode"] = _mode;
    js["Buffer Capacity"] = _bufferCapacity;
    js["Buffer Size"] = _bufferSize;

    return js;
  }

  __INLINE__ void deserialize (const nlohmann::json& js)
  {
    _name = hicr::json::getString(js, "Name");
    _mode = hicr::json::getString(js, "Mode");
    _bufferCapacity = hicr::json::getNumber<size_t>(js, "Buffer Capacity");
    _bufferSize = hicr::json::getNumber<size_t>(js, "Buffer Size");

    validate();
  }

  private:

  static size_t getReferenceBufferSize(const size_t bufferCapacity) { return bufferCapacity * sizeof(void*); }

  void validate()
  {
    if (_mode == "Copy" && _bufferSize == 0) HICR_THROW_LOGIC("A zero buffer size provided, although operation mode 'Copy' was indicated");
    if (_mode == "Reference" && _bufferSize == 0) _bufferSize = getReferenceBufferSize(_bufferCapacity);
    if (_mode == "Reference" && _bufferSize != getReferenceBufferSize(_bufferCapacity)) HICR_THROW_LOGIC("A non-zero buffer size provided, although operation mode 'Reference' was indicated");
  }

  std::string _name;
  std::string _mode;
  size_t _bufferCapacity;
  size_t _bufferSize;

}; // class Base

} // namespace hLLM::configuration
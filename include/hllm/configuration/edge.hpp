#pragma once

#include <cstdint>
#include <string>
#include <hicr/core/definitions.hpp>
#include <nlohmann_json/parser.hpp>

namespace hLLM::configuration
{

class Edge
{
  public:

  typedef uint64_t edgeIndex_t;

  Edge(const nlohmann::json& js) { deserialize(js); } 
  Edge(const std::string& name, const std::string& producer, const std::string& consumer, const std::string& mode, const size_t bufferCapacity, const size_t bufferSize = 0)
   :  _name(name),
      _producer(producer),
      _consumer(consumer),
      _mode(mode),
     _bufferCapacity(bufferCapacity),
     _bufferSize(bufferSize)
  {  validate(); }
  virtual ~Edge() = default;

  __INLINE__ void setName(const std::string& name) { _name = name; }
  [[nodiscard]] __INLINE__ std::string getName() const { return _name; }
  [[nodiscard]] __INLINE__ std::string getProducer() const { return _producer; }
  [[nodiscard]] __INLINE__ std::string getConsumer() const { return _consumer; }
  [[nodiscard]] __INLINE__ size_t getBufferCapacity() const { return _bufferCapacity; }
  [[nodiscard]] __INLINE__ size_t getBufferSize() const { return _bufferSize; }
  [[nodiscard]] __INLINE__ std::string getMode() const { return _mode; }
  
  [[nodiscard]] __INLINE__ nlohmann::json serialize() const
  {
    nlohmann::json js;

    js["Name"] = _name;
    js["Mode"] = _mode;
    js["Producer"] = _producer;
    js["Consumer"] = _consumer;
    js["Buffer Capacity"] = _bufferCapacity;
    js["Buffer Size"] = _bufferSize;

    return js;
  }

  __INLINE__ void deserialize (const nlohmann::json& js)
  {
    _name = hicr::json::getString(js, "Name");
    _mode = hicr::json::getString(js, "Mode");
    _producer = hicr::json::getString(js, "Producer");
    _consumer = hicr::json::getString(js, "Consumer");
    _bufferCapacity = hicr::json::getNumber<size_t>(js, "Buffer Capacity");
    _bufferSize = hicr::json::getNumber<size_t>(js, "Buffer Size");

    validate();
  }

  // Functions to set the HiCR elements required for the creation of edge channels
  __INLINE__ void setPayloadCommunicationManager(HiCR::CommunicationManager* const communicationManager) { _payloadCommunicationManager = communicationManager; }
  __INLINE__ void setPayloadMemoryManager(HiCR::MemoryManager* const memoryManager) { _payloadMemoryManager = memoryManager; }
  __INLINE__ void setPayloadMemorySpace(const std::shared_ptr<HiCR::MemorySpace> memorySpace) { _payloadMemorySpace = memorySpace; }
  __INLINE__ void setCoordinationCommunicationManager(HiCR::CommunicationManager* const communicationManager) { _coordinationCommunicationManager = communicationManager; }
  __INLINE__ void setCoordinationMemoryManager(HiCR::MemoryManager* const memoryManager) { _coordinationMemoryManager = memoryManager; }
  __INLINE__ void setCoordinationMemorySpace(const std::shared_ptr<HiCR::MemorySpace> memorySpace) { _coordinationMemorySpace = memorySpace; }

    // Functions to set the HiCR elements required for the creation of edge channels
  [[nodiscard]] __INLINE__ HiCR::CommunicationManager* getPayloadCommunicationManager     () const { return _payloadCommunicationManager; }
  [[nodiscard]] __INLINE__ HiCR::MemoryManager*        getPayloadMemoryManager            () const { return _payloadMemoryManager; }
  [[nodiscard]] __INLINE__ std::shared_ptr<HiCR::MemorySpace>          getPayloadMemorySpace              () const { return _payloadMemorySpace; }
  [[nodiscard]] __INLINE__ HiCR::CommunicationManager* getCoordinationCommunicationManager() const { return _coordinationCommunicationManager; }
  [[nodiscard]] __INLINE__ HiCR::MemoryManager*        getCoordinationMemoryManager       () const { return _coordinationMemoryManager; }
  [[nodiscard]] __INLINE__ std::shared_ptr<HiCR::MemorySpace>          getCoordinationMemorySpace         () const { return _coordinationMemorySpace; }

  private:

  static size_t getReferenceBufferSize(const size_t bufferCapacity) { return bufferCapacity * sizeof(void*); }

  void validate()
  {
    bool modeRecognized = false;
    if (_mode == "Copy" || _mode == "Reference") modeRecognized = true;
    if (modeRecognized == false) HICR_THROW_LOGIC("Edge mode '%s' is unrecognized", _mode.c_str());

    if (_mode == "Copy" && _bufferSize == 0) HICR_THROW_LOGIC("A zero buffer size provided, although operation mode 'Copy' was indicated");
    if (_mode == "Reference" && _bufferSize == 0) _bufferSize = getReferenceBufferSize(_bufferCapacity);
    if (_mode == "Reference" && _bufferSize != getReferenceBufferSize(_bufferCapacity)) HICR_THROW_LOGIC("A non-zero buffer size provided, although operation mode 'Reference' was indicated");
  }

  std::string _name;
  std::string _producer;
  std::string _consumer;
  std::string _mode;
  size_t _bufferCapacity;
  size_t _bufferSize;

  // HiCR-specific objects to create the payload buffers. These are to be set at runtime
  HiCR::CommunicationManager* _payloadCommunicationManager = nullptr;
  HiCR::MemoryManager* _payloadMemoryManager  = nullptr;
  std::shared_ptr<HiCR::MemorySpace> _payloadMemorySpace  = nullptr;

  // HiCR-specific objects to create the coordination buffers. These are to be set at runtime
  HiCR::CommunicationManager* _coordinationCommunicationManager  = nullptr;
  HiCR::MemoryManager* _coordinationMemoryManager  = nullptr;
  std::shared_ptr<HiCR::MemorySpace> _coordinationMemorySpace  = nullptr;

}; // class Base

} // namespace hLLM::configuration
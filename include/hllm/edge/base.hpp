#pragma once

#include <map>
#include <hicr/core/definitions.hpp>
#include <hicr/core/exceptions.hpp>
#include <hicr/core/localMemorySlot.hpp>
#include <hicr/core/globalMemorySlot.hpp>
#include "../configuration/edge.hpp"
#include "../configuration/replica.hpp"

namespace hLLM::edge
{

struct memorySlotExchangeInfo_t
{
  /// Pointer to the communication manager required to exchange this memory slot
  HiCR::CommunicationManager* communicationManager;

  /// Global key to use for the exchange
  HiCR::GlobalMemorySlot::globalKey_t globalKey;
  
  /// Local memory slot to exchange
  std::shared_ptr<HiCR::LocalMemorySlot> memorySlot;
};

class Base 
{
  public:

  static constexpr size_t maxEdgeIndexBits = 32;
  static constexpr size_t maxReplicaIndexBits = 24;
  static constexpr size_t maxChannelSpecificKeyBits = 8;
  static constexpr configuration::Edge::edgeIndex_t maxEdgeIndex = 1ul << maxEdgeIndexBits;
  static constexpr configuration::Replica::replicaIndex_t maxReplicaIndex = 1ul << maxReplicaIndexBits;
  static constexpr HiCR::GlobalMemorySlot::globalKey_t maxChannelSpecificKey = 1ul << maxChannelSpecificKeyBits;
  static constexpr configuration::Replica::replicaIndex_t coordinatorReplicaIndex = maxReplicaIndex - 1;
  
  Base(const configuration::Edge edgeConfig,
       const configuration::Edge::edgeIndex_t edgeIndex,
       const configuration::Replica::replicaIndex_t replicaIndex) : 
    _edgeConfig(edgeConfig),
    _edgeIndex(edgeIndex),
    _replicaIndex(replicaIndex)
  {
    // Verifying all the required HiCR object have been passed
    if (_edgeConfig.getPayloadCommunicationManager     () == nullptr) HICR_THROW_LOGIC("Required HiCR object 'PayloadCommunicationManager' not provided at deployment time for edge '%s'", _edgeConfig.getName().c_str());
    if (_edgeConfig.getPayloadMemoryManager            () == nullptr) HICR_THROW_LOGIC("Required HiCR object 'PayloadMemoryManager' not provided at deployment time for edge '%s'", _edgeConfig.getName().c_str());
    if (_edgeConfig.getPayloadMemorySpace              () == nullptr) HICR_THROW_LOGIC("Required HiCR object 'PayloadMemorySpace' not provided at deployment time for edge '%s'", _edgeConfig.getName().c_str());
    if (_edgeConfig.getCoordinationCommunicationManager() == nullptr) HICR_THROW_LOGIC("Required HiCR object 'CoordinationCommunicationManager' not provided at deployment time for edge '%s'", _edgeConfig.getName().c_str());
    if (_edgeConfig.getCoordinationMemoryManager       () == nullptr) HICR_THROW_LOGIC("Required HiCR object 'CoordinationMemoryManager not provided at deployment time for edge '%s'", _edgeConfig.getName().c_str());
    if (_edgeConfig.getCoordinationMemorySpace         () == nullptr) HICR_THROW_LOGIC("Required HiCR object 'CoordinationMemorySpace' not provided at deployment time for edge '%s'", _edgeConfig.getName().c_str());

    // Reserving memory for the local coordination buffers
    const auto coordinationBufferSize = HiCR::channel::Base::getCoordinationBufferSize();
    _localCoordinationBufferForSizes = _edgeConfig.getCoordinationMemoryManager()->allocateLocalMemorySlot(_edgeConfig.getCoordinationMemorySpace(), coordinationBufferSize);
    _localCoordinationBufferForPayloads = _edgeConfig.getCoordinationMemoryManager()->allocateLocalMemorySlot(_edgeConfig.getCoordinationMemorySpace(), coordinationBufferSize);
  }

  virtual ~Base()
  {
    // Freeing up buffers
    _edgeConfig.getCoordinationMemoryManager()->freeLocalMemorySlot(_localCoordinationBufferForSizes);
    _edgeConfig.getCoordinationMemoryManager()->freeLocalMemorySlot(_localCoordinationBufferForPayloads);
  }

  virtual void getMemorySlotsToExchange(std::vector<memorySlotExchangeInfo_t>& memorySlots) const = 0;
  
  // Function to initialize the channels. It must be called only all the memory slots have been exchanged
  __INLINE__ void initialize(const HiCR::GlobalMemorySlot::tag_t tag)
  {
    // Obtaining the globally exchanged memory slots
    _consumerSizesBuffer                   = _edgeConfig.getCoordinationCommunicationManager()->getGlobalMemorySlot(tag, encodeGlobalKey(_edgeIndex, _replicaIndex, _consumerSizesBufferKey));
    _consumerPayloadBuffer                 = _edgeConfig.getPayloadCommunicationManager()->getGlobalMemorySlot(     tag, encodeGlobalKey(_edgeIndex, _replicaIndex, _consumerPayloadBufferKey));
    _consumerCoordinationBufferForSizes    = _edgeConfig.getCoordinationCommunicationManager()->getGlobalMemorySlot(tag, encodeGlobalKey(_edgeIndex, _replicaIndex, _consumerCoordinationBufferforSizesKey));
    _consumerCoordinationBufferForPayloads = _edgeConfig.getCoordinationCommunicationManager()->getGlobalMemorySlot(tag, encodeGlobalKey(_edgeIndex, _replicaIndex, _consumerCoordinationBufferforPayloadKey));
    _producerCoordinationBufferForSizes    = _edgeConfig.getCoordinationCommunicationManager()->getGlobalMemorySlot(tag, encodeGlobalKey(_edgeIndex, _replicaIndex, _producerCoordinationBufferforSizesKey));
    _producerCoordinationBufferForPayloads = _edgeConfig.getCoordinationCommunicationManager()->getGlobalMemorySlot(tag, encodeGlobalKey(_edgeIndex, _replicaIndex, _producerCoordinationBufferforPayloadKey));

    // Creating channels now
    createChannels();
  }

  protected:

  virtual void createChannels() = 0;

  __INLINE__ static HiCR::GlobalMemorySlot::globalKey_t encodeGlobalKey(
    const configuration::Edge::edgeIndex_t edgeIndex,
    const configuration::Replica::replicaIndex_t replicaIndex,
    const HiCR::GlobalMemorySlot::globalKey_t channelKey)
  {
    // Encoding reserves:
    // + 32 bits for edgeIndex (max: 4294967296)
    // + 24 bits for replicaIndex (max: 16777216) minus one -- the last one -- which is reserved for the coordinator index
    // + 8 bits for channel-specific keys (max: 256)

    // Sanity checks
    if (edgeIndex >= maxEdgeIndex) HICR_THROW_LOGIC("Base index %lu exceeds maximum: %lu\n", edgeIndex, maxEdgeIndex);
    if (replicaIndex >= maxReplicaIndex) HICR_THROW_LOGIC("Replica index %lu exceeds maximum: %lu\n", replicaIndex, maxEdgeIndex);
    if (channelKey >= maxChannelSpecificKey) HICR_THROW_LOGIC("Channel-specific key %lu exceeds maximum: %lu\n", edgeIndex, maxEdgeIndex);

    return HiCR::GlobalMemorySlot::globalKey_t(
        edgeIndex    << (maxReplicaIndexBits + maxChannelSpecificKeyBits + 0) |
        replicaIndex << (maxChannelSpecificKeyBits + 0) |
        channelKey   << (0)
    );
  }

  // Assigning keys to the global slots to exchange between consumer and producer sides of this edge
  static constexpr HiCR::GlobalMemorySlot::globalKey_t _consumerSizesBufferKey = 0;
  static constexpr HiCR::GlobalMemorySlot::globalKey_t _consumerPayloadBufferKey = 1;
  static constexpr HiCR::GlobalMemorySlot::globalKey_t _consumerCoordinationBufferforSizesKey = 2;
  static constexpr HiCR::GlobalMemorySlot::globalKey_t _consumerCoordinationBufferforPayloadKey = 3;
  static constexpr HiCR::GlobalMemorySlot::globalKey_t _producerCoordinationBufferforSizesKey = 4;
  static constexpr HiCR::GlobalMemorySlot::globalKey_t _producerCoordinationBufferforPayloadKey = 5;

  const configuration::Edge _edgeConfig;
  const configuration::Edge::edgeIndex_t _edgeIndex;
  const configuration::Replica::replicaIndex_t _replicaIndex;

  // Here we declare the local coordination buffer memory slots we need to allocate
  std::shared_ptr<HiCR::LocalMemorySlot> _localCoordinationBufferForSizes;
  std::shared_ptr<HiCR::LocalMemorySlot> _localCoordinationBufferForPayloads;

  // Here we declare the global memory slots we need to exchange to build a variable sized SPSC channel in HICR
  std::shared_ptr<HiCR::GlobalMemorySlot> _consumerSizesBuffer;
  std::shared_ptr<HiCR::GlobalMemorySlot> _consumerPayloadBuffer;
  std::shared_ptr<HiCR::GlobalMemorySlot> _consumerCoordinationBufferForSizes;
  std::shared_ptr<HiCR::GlobalMemorySlot> _consumerCoordinationBufferForPayloads;
  std::shared_ptr<HiCR::GlobalMemorySlot> _producerCoordinationBufferForSizes;
  std::shared_ptr<HiCR::GlobalMemorySlot> _producerCoordinationBufferForPayloads;

}; // class Base

} // namespace hLLM::edge
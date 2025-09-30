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

typedef uint8_t edgeTypeDatatype_t;
enum edgeType_t : edgeTypeDatatype_t
{
  coordinatorToCoordinatorInput = 0,
  coordinatorToCoordinatorOutput = 1,
  coordinatorToReplica = 2,
  replicaToCoordinator = 3
};


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

  static constexpr size_t maxEdgeIndexBits = 24;
  static constexpr size_t maxPartitionIndexBits = 16;
  static constexpr size_t maxReplicaIndexBits = 16;
  static constexpr size_t maxEdgeTypeBits = 2;
  static constexpr size_t maxChannelSpecificKeyBits = 6;
  static constexpr configuration::Edge::edgeIndex_t maxEdgeIndex = 1ul << maxEdgeIndexBits;
  static constexpr configuration::Partition::partitionIndex_t maxPartitionIndex = 1ul << maxPartitionIndexBits;
  static constexpr configuration::Replica::replicaIndex_t maxReplicaIndex = 1ul << maxReplicaIndexBits;
  static constexpr edgeTypeDatatype_t maxEdgeType = 1u << maxEdgeTypeBits;
  static constexpr HiCR::GlobalMemorySlot::globalKey_t maxChannelSpecificKey = 1ul << maxChannelSpecificKeyBits;

  // When it comes to communication between coordinators, the replica index is indicated as the maximum value possible
  static constexpr configuration::Replica::replicaIndex_t coordinatorReplicaIndex = maxReplicaIndex - 1;
  
  // When it comes to control messages, the edge index is indicated as the maximum value possible
  static constexpr configuration::Edge::edgeIndex_t controlEdgeIndex = maxEdgeIndex - 1;
  
  Base(const configuration::Edge edgeConfig,
       const edgeType_t edgeType,
       const configuration::Edge::edgeIndex_t edgeIndex,
       const configuration::Partition::partitionIndex_t partitionIndex,
       const configuration::Replica::replicaIndex_t replicaIndex) : 
    _edgeConfig(edgeConfig),
    _edgeType(edgeType),
    _edgeIndex(edgeIndex),
    _partitionIndex(partitionIndex),
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
    _dataChannelLocalCoordinationBufferForSizes    = _edgeConfig.getCoordinationMemoryManager()->allocateLocalMemorySlot(_edgeConfig.getCoordinationMemorySpace(), coordinationBufferSize);
    _dataChannelLocalCoordinationBufferForPayloads = _edgeConfig.getCoordinationMemoryManager()->allocateLocalMemorySlot(_edgeConfig.getCoordinationMemorySpace(), coordinationBufferSize);
    _metadataChannelLocalCoordinationBuffer        = _edgeConfig.getCoordinationMemoryManager()->allocateLocalMemorySlot(_edgeConfig.getCoordinationMemorySpace(), coordinationBufferSize);
    HiCR::channel::Base::initializeCoordinationBuffer(_dataChannelLocalCoordinationBufferForSizes);
    HiCR::channel::Base::initializeCoordinationBuffer(_dataChannelLocalCoordinationBufferForPayloads);
    HiCR::channel::Base::initializeCoordinationBuffer(_metadataChannelLocalCoordinationBuffer);
  }

  virtual ~Base()
  {
    // Freeing up buffers
    _edgeConfig.getCoordinationMemoryManager()->freeLocalMemorySlot(_dataChannelLocalCoordinationBufferForSizes);
    _edgeConfig.getCoordinationMemoryManager()->freeLocalMemorySlot(_dataChannelLocalCoordinationBufferForPayloads);
    _edgeConfig.getCoordinationMemoryManager()->freeLocalMemorySlot(_metadataChannelLocalCoordinationBuffer);
  }

  virtual void getMemorySlotsToExchange(std::vector<memorySlotExchangeInfo_t>& memorySlots) const = 0;
  
  // Function to initialize the channels. It must be called only all the memory slots have been exchanged
  __INLINE__ void initialize(const HiCR::GlobalMemorySlot::tag_t tag)
  {
    ///// Data Channel common (producer and consumer) global memory slots
    _dataChannelConsumerSizesBuffer                   = _edgeConfig.getCoordinationCommunicationManager()->getGlobalMemorySlot(tag, encodeGlobalKey(_edgeIndex, _partitionIndex, _replicaIndex, _edgeType, _dataChannelConsumerSizesBufferKey));
    _dataChannelConsumerPayloadBuffer                 = _edgeConfig.getPayloadCommunicationManager()->getGlobalMemorySlot(     tag, encodeGlobalKey(_edgeIndex, _partitionIndex, _replicaIndex, _edgeType, _dataChannelConsumerPayloadBufferKey));
    _dataChannelConsumerCoordinationBufferForSizes    = _edgeConfig.getCoordinationCommunicationManager()->getGlobalMemorySlot(tag, encodeGlobalKey(_edgeIndex, _partitionIndex, _replicaIndex, _edgeType, _dataChannelConsumerCoordinationBufferforSizesKey));
    _dataChannelConsumerCoordinationBufferForPayloads = _edgeConfig.getCoordinationCommunicationManager()->getGlobalMemorySlot(tag, encodeGlobalKey(_edgeIndex, _partitionIndex, _replicaIndex, _edgeType, _dataChannelConsumerCoordinationBufferforPayloadKey));
    _dataChannelProducerCoordinationBufferForSizes    = _edgeConfig.getCoordinationCommunicationManager()->getGlobalMemorySlot(tag, encodeGlobalKey(_edgeIndex, _partitionIndex, _replicaIndex, _edgeType, _dataChannelProducerCoordinationBufferforSizesKey));
    _dataChannelProducerCoordinationBufferForPayloads = _edgeConfig.getCoordinationCommunicationManager()->getGlobalMemorySlot(tag, encodeGlobalKey(_edgeIndex, _partitionIndex, _replicaIndex, _edgeType, _dataChannelProducerCoordinationBufferforPayloadKey));

    ///// Metadata Channel common (producer and consumer) global memory slots
    _metadataChannelConsumerPayloadBuffer                 = _edgeConfig.getCoordinationCommunicationManager()->getGlobalMemorySlot(tag, encodeGlobalKey(_edgeIndex, _partitionIndex, _replicaIndex, _edgeType, _metadataChannelConsumerPayloadBufferKey));
    _metadataChannelConsumerCoordinationBuffer            = _edgeConfig.getCoordinationCommunicationManager()->getGlobalMemorySlot(tag, encodeGlobalKey(_edgeIndex, _partitionIndex, _replicaIndex, _edgeType, _metadataChannelConsumerCoordinationBufferKey));
    _metadataChannelProducerCoordinationBuffer            = _edgeConfig.getCoordinationCommunicationManager()->getGlobalMemorySlot(tag, encodeGlobalKey(_edgeIndex, _partitionIndex, _replicaIndex, _edgeType, _metadataChannelProducerCoordinationBufferKey));

    // Creating channels now
    createChannels();
  }

  protected:

  virtual void createChannels() = 0;

  __INLINE__ static HiCR::GlobalMemorySlot::globalKey_t encodeGlobalKey(
    const configuration::Edge::edgeIndex_t edgeIndex,
    const configuration::Partition::partitionIndex_t partitionIndex,
    const configuration::Replica::replicaIndex_t replicaIndex,
    const edgeType_t edgeType,
    const HiCR::GlobalMemorySlot::globalKey_t channelKey)
  {
    // Encoding reserves:
    // + 24 bits for edgeIndex (max: 16777216)
    // + 16 bits for partitionIndex (max: 65536) minus one -- the last one -- which is reserved
    // + 16 bits for replicaIndex (max: 65536) minus one -- the last one -- which is reserved
    // + 2 bits for edge type (max: 4)
    // + 6 bits for channel-specific keys (max: 64)

    // Sanity checks
    if (edgeIndex >= maxEdgeIndex) HICR_THROW_LOGIC("Base index %lu exceeds maximum: %lu\n", edgeIndex, maxEdgeIndex);
    if (partitionIndex >= maxPartitionIndex) HICR_THROW_LOGIC("Partition index %lu exceeds maximum: %lu\n", partitionIndex, maxPartitionIndex);
    if (replicaIndex >= maxReplicaIndex) HICR_THROW_LOGIC("Replica index %lu exceeds maximum: %lu\n", replicaIndex, maxReplicaIndex);
    if (edgeType >= maxEdgeType) HICR_THROW_LOGIC("Edge type value %lu exceeds maximum: %lu (this must be a bug in hLLM)\n", edgeType, maxEdgeType);
    if (channelKey >= maxChannelSpecificKey) HICR_THROW_LOGIC("Channel-specific key %lu exceeds maximum: %lu\n", edgeIndex, maxEdgeIndex);

    // Initial bit for encoding
    constexpr uint8_t initialBit = 0;

    // Creating global key
    const auto globalKey = HiCR::GlobalMemorySlot::globalKey_t(
        edgeIndex      << (maxPartitionIndexBits + maxReplicaIndexBits + maxChannelSpecificKeyBits + maxEdgeTypeBits + initialBit) |
        partitionIndex << (maxReplicaIndexBits + maxChannelSpecificKeyBits + maxEdgeTypeBits + initialBit) |
        replicaIndex   << (maxChannelSpecificKeyBits + maxEdgeTypeBits + initialBit) |
        edgeType       << (maxChannelSpecificKeyBits + initialBit) |
        channelKey     << (initialBit)
    );

    // printf("Key: %lu = f(%lu, %lu, %u, %lu)\n", globalKey, edgeIndex, replicaIndex, edgeType, channelKey);

    return globalKey;
  }

  // Assigning keys to the global slots to exchange between consumer and producer sides of this edge

  // For the data channel
  static constexpr HiCR::GlobalMemorySlot::globalKey_t _dataChannelConsumerSizesBufferKey = 0;
  static constexpr HiCR::GlobalMemorySlot::globalKey_t _dataChannelConsumerPayloadBufferKey = 1;
  static constexpr HiCR::GlobalMemorySlot::globalKey_t _dataChannelConsumerCoordinationBufferforSizesKey = 2;
  static constexpr HiCR::GlobalMemorySlot::globalKey_t _dataChannelConsumerCoordinationBufferforPayloadKey = 3;
  static constexpr HiCR::GlobalMemorySlot::globalKey_t _dataChannelProducerCoordinationBufferforSizesKey = 4;
  static constexpr HiCR::GlobalMemorySlot::globalKey_t _dataChannelProducerCoordinationBufferforPayloadKey = 5;

  // For the metadata channel
  static constexpr HiCR::GlobalMemorySlot::globalKey_t _metadataChannelConsumerPayloadBufferKey = 6;
  static constexpr HiCR::GlobalMemorySlot::globalKey_t _metadataChannelConsumerCoordinationBufferKey = 7;
  static constexpr HiCR::GlobalMemorySlot::globalKey_t _metadataChannelProducerCoordinationBufferKey = 8;

  ////// Data Channel Memory Slots

  // Here we declare the local coordination buffer memory slots we need to allocate
  std::shared_ptr<HiCR::LocalMemorySlot> _dataChannelLocalCoordinationBufferForSizes;
  std::shared_ptr<HiCR::LocalMemorySlot> _dataChannelLocalCoordinationBufferForPayloads;

  // Here we declare the global memory slots we need to exchange to build a variable sized SPSC channel in HICR
  std::shared_ptr<HiCR::GlobalMemorySlot> _dataChannelConsumerSizesBuffer;
  std::shared_ptr<HiCR::GlobalMemorySlot> _dataChannelConsumerPayloadBuffer;
  std::shared_ptr<HiCR::GlobalMemorySlot> _dataChannelConsumerCoordinationBufferForSizes;
  std::shared_ptr<HiCR::GlobalMemorySlot> _dataChannelConsumerCoordinationBufferForPayloads;
  std::shared_ptr<HiCR::GlobalMemorySlot> _dataChannelProducerCoordinationBufferForSizes;
  std::shared_ptr<HiCR::GlobalMemorySlot> _dataChannelProducerCoordinationBufferForPayloads;

  ////// Metadata Channel Memory Slots

  // Here we declare the local coordination buffer memory slots we need to allocate
  std::shared_ptr<HiCR::LocalMemorySlot> _metadataChannelLocalCoordinationBuffer;

  // Here we declare the global memory slots we need to exchange to build a fixed sized SPSC channel in HICR
  std::shared_ptr<HiCR::GlobalMemorySlot> _metadataChannelConsumerPayloadBuffer;
  std::shared_ptr<HiCR::GlobalMemorySlot> _metadataChannelConsumerCoordinationBuffer;
  std::shared_ptr<HiCR::GlobalMemorySlot> _metadataChannelProducerCoordinationBuffer;

  // Edge configuration and identification variables
  const configuration::Edge _edgeConfig;
  const edgeType_t _edgeType;
  const configuration::Edge::edgeIndex_t _edgeIndex;
  const configuration::Partition::partitionIndex_t _partitionIndex;
  const configuration::Replica::replicaIndex_t _replicaIndex;

}; // class Base

} // namespace hLLM::edge
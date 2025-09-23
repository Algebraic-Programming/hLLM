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
  }

  virtual ~Base() = default;

  virtual HiCR::CommunicationManager::globalKeyToMemorySlotMap_t getMemorySlotsToExchange() const = 0;

  private:

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
    if (replicaIndex >= maxReplicaIndex - 1) HICR_THROW_LOGIC("Replica index %lu exceeds maximum: %lu\n", edgeIndex, maxEdgeIndex);
    if (channelKey >= maxChannelSpecificKey) HICR_THROW_LOGIC("Channel-specific key %lu exceeds maximum: %lu\n", edgeIndex, maxEdgeIndex);

    return HiCR::GlobalMemorySlot::globalKey_t(
        edgeIndex    << (maxReplicaIndexBits + maxChannelSpecificKeyBits + 0) |
        replicaIndex << (maxChannelSpecificKeyBits + 0) |
        channelKey   << (0)
    );
  }

  const configuration::Edge _edgeConfig;
  const configuration::Edge::edgeIndex_t _edgeIndex;
  const configuration::Replica::replicaIndex_t _replicaIndex;
}; // class Base

} // namespace hLLM::edge
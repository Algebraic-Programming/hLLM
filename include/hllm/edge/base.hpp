#pragma once

#include <map>
#include <hicr/core/definitions.hpp>
#include <hicr/core/exceptions.hpp>
#include <hicr/core/localMemorySlot.hpp>
#include <hicr/core/globalMemorySlot.hpp>
#include "configuration/edge.hpp"
#include "configuration/replica.hpp"

namespace hLLM
{

class Edge final
{
  public:

  static constexpr size_t maxEdgeIndexBits = 32;
  static constexpr size_t maxReplicaIndexBits = 24;
  static constexpr size_t maxChannelSpecificKeyBits = 8;
  static constexpr configuration::Edge::edgeIndex_t maxEdgeIndex = 1ul << maxEdgeIndexBits;
  static constexpr configuration::Replica::replicaIndex_t maxReplicaIndex = 1ul << maxReplicaIndexBits;
  static constexpr HiCR::GlobalMemorySlot::globalKey_t maxChannelSpecificKey = 1ul << maxChannelSpecificKeyBits;
  static constexpr configuration::Replica::replicaIndex_t coordinatorReplicaIndex = maxReplicaIndex - 1;
  
  Edge(const configuration::Edge edgeConfig,
       const configuration::Edge::edgeIndex_t edgeIndex,
       const configuration::Replica::replicaIndex_t replicaIndex) : 
    _edgeIndex(edgeIndex),
    _replicaIndex(replicaIndex),
    _edgeConfig(edgeConfig)
  {

  }

  ~Edge() = default;

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
    if (edgeIndex >= maxEdgeIndex) HICR_THROW_LOGIC("Edge index %lu exceeds maximum: %lu\n", edgeIndex, maxEdgeIndex);
    if (replicaIndex >= maxReplicaIndex - 1) HICR_THROW_LOGIC("Replica index %lu exceeds maximum: %lu\n", edgeIndex, maxEdgeIndex);
    if (channelKey >= maxChannelSpecificKey) HICR_THROW_LOGIC("Channel-specific key %lu exceeds maximum: %lu\n", edgeIndex, maxEdgeIndex);

    return HiCR::GlobalMemorySlot::globalKey_t(
        edgeIndex    << (maxReplicaIndexBits + maxChannelSpecificKeyBits + 0) |
        replicaIndex << (maxChannelSpecificKeyBits + 0) |
        channelKey   << (0)
    );
  }

  const configuration::Edge::edgeIndex_t _edgeIndex;
  const configuration::Replica::replicaIndex_t _replicaIndex;
  const configuration::Edge _edgeConfig;

}; // class Edge

} // namespace hLLM
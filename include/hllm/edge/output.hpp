#pragma once

#include "base.hpp"
#include "hicr/core/communicationManager.hpp"
#include "hicr/core/memoryManager.hpp"
#include "hicr/frontends/channel/variableSize/spsc/producer.hpp"

namespace hLLM::edge
{

class Output final : public Base
{
  public:

  Output(const configuration::Edge edgeConfig,
        const configuration::Edge::edgeIndex_t edgeIndex,
        const configuration::Replica::replicaIndex_t replicaIndex) :
         Base(edgeConfig, edgeIndex, replicaIndex)
  {
    
  }

  ~Output() = default;

  public:
  
  __INLINE__ void getMemorySlotsToExchange(std::vector<memorySlotExchangeInfo_t>& memorySlots) const override
  {
    // Getting key / memory slot pairs
    memorySlots.push_back( memorySlotExchangeInfo_t { .communicationManager = _edgeConfig.getCoordinationCommunicationManager(), .globalKey = encodeGlobalKey(_edgeIndex, _replicaIndex, _producerCoordinationBufferforSizesKey),   .memorySlot = _localCoordinationBufferForSizes } );
    memorySlots.push_back( memorySlotExchangeInfo_t { .communicationManager = _edgeConfig.getCoordinationCommunicationManager(), .globalKey = encodeGlobalKey(_edgeIndex, _replicaIndex, _producerCoordinationBufferforPayloadKey), .memorySlot = _localCoordinationBufferForPayloads } );
  }

  private:

  //// We use a variable size SPSC channel to communicate, where as an output edge we take the producer interface
  
  // The HiCR channels we use to communicate
  std::shared_ptr<HiCR::channel::variableSize::SPSC::Producer> _channel;
  
}; // class Output

} // namespace hLLM::edge
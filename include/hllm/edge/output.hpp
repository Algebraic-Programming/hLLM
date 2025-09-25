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
    _producerSizeInfoBuffer = _edgeConfig.getCoordinationMemoryManager()->allocateLocalMemorySlot(_edgeConfig.getCoordinationMemorySpace(), sizeof(size_t)); 
  }

  ~Output()
  {
    // Freeing up buffers
    _edgeConfig.getCoordinationMemoryManager()->freeLocalMemorySlot(_producerSizeInfoBuffer);
  }

  public:
  
  __INLINE__ void getMemorySlotsToExchange(std::vector<memorySlotExchangeInfo_t>& memorySlots) const override
  {
    // Getting key / memory slot pairs
    memorySlots.push_back( memorySlotExchangeInfo_t { .communicationManager = _edgeConfig.getCoordinationCommunicationManager(), .globalKey = encodeGlobalKey(_edgeIndex, _replicaIndex, _producerCoordinationBufferforSizesKey),   .memorySlot = _localCoordinationBufferForSizes } );
    memorySlots.push_back( memorySlotExchangeInfo_t { .communicationManager = _edgeConfig.getCoordinationCommunicationManager(), .globalKey = encodeGlobalKey(_edgeIndex, _replicaIndex, _producerCoordinationBufferforPayloadKey), .memorySlot = _localCoordinationBufferForPayloads } );
  }

  private:

  //// We use a variable size SPSC channel to communicate, where as an output edge we take the producer interface

  __INLINE__ void createChannels() override
  {
    // Creating producer channel
    _channel = std::make_shared<HiCR::channel::variableSize::SPSC::Producer>(
      *_edgeConfig.getCoordinationCommunicationManager(),
      _producerSizeInfoBuffer,
      _consumerPayloadBuffer,
      _consumerSizesBuffer,
      _producerCoordinationBufferForSizes->getSourceLocalMemorySlot(),
      _producerCoordinationBufferForPayloads->getSourceLocalMemorySlot(),
      _consumerCoordinationBufferForSizes,
      _consumerCoordinationBufferForPayloads,
      _edgeConfig.getBufferSize(),
      sizeof(uint8_t),
      _edgeConfig.getBufferCapacity()
    );
  }

  
  // Internal memory slot for producer coordination
  std::shared_ptr<HiCR::LocalMemorySlot> _producerSizeInfoBuffer;

  // The HiCR channels we use to communicate
  std::shared_ptr<HiCR::channel::variableSize::SPSC::Producer> _channel;
  
}; // class Output

} // namespace hLLM::edge
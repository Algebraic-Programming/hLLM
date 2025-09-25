#pragma once

#include "base.hpp"
#include "hicr/frontends/channel/variableSize/spsc/consumer.hpp"

namespace hLLM::edge
{

class Input final : public Base
{
  public:

  Input(const configuration::Edge edgeConfig,
        const edgeType_t edgeType,
        const configuration::Edge::edgeIndex_t edgeIndex,
        const configuration::Replica::replicaIndex_t replicaIndex) :
         Base(edgeConfig, edgeType, edgeIndex, replicaIndex)
  {
    // Allocating additional local buffers required for the consumer 

    // Getting required buffer size
    auto sizesBufferSize = HiCR::channel::variableSize::Base::getTokenBufferSize(sizeof(size_t), _edgeConfig.getBufferCapacity());

    // Allocating sizes buffer as a local memory slot
    _sizesBuffer = _edgeConfig.getCoordinationMemoryManager()->allocateLocalMemorySlot(_edgeConfig.getCoordinationMemorySpace(), sizesBufferSize);

    // Allocating payload buffer as a local memory slot
    _payloadBuffer = _edgeConfig.getPayloadMemoryManager()->allocateLocalMemorySlot(_edgeConfig.getPayloadMemorySpace(), _edgeConfig.getBufferSize());
  }

  ~Input()
  {
    // Freeing up buffers
    _edgeConfig.getCoordinationMemoryManager()->freeLocalMemorySlot(_sizesBuffer);
    _edgeConfig.getPayloadMemoryManager()->freeLocalMemorySlot(_payloadBuffer);
  }

  __INLINE__ void getMemorySlotsToExchange(std::vector<memorySlotExchangeInfo_t>& memorySlots) const override
  {
    // Getting key / memory slot pairs
    memorySlots.push_back( memorySlotExchangeInfo_t { .communicationManager = _edgeConfig.getCoordinationCommunicationManager(), .globalKey = encodeGlobalKey(_edgeIndex, _replicaIndex, _edgeType, _consumerCoordinationBufferforSizesKey),   .memorySlot = _localCoordinationBufferForSizes } );
    memorySlots.push_back( memorySlotExchangeInfo_t { .communicationManager = _edgeConfig.getCoordinationCommunicationManager(), .globalKey = encodeGlobalKey(_edgeIndex, _replicaIndex, _edgeType, _consumerCoordinationBufferforPayloadKey), .memorySlot = _localCoordinationBufferForPayloads } );
    memorySlots.push_back( memorySlotExchangeInfo_t { .communicationManager = _edgeConfig.getCoordinationCommunicationManager(), .globalKey = encodeGlobalKey(_edgeIndex, _replicaIndex, _edgeType, _consumerSizesBufferKey),                  .memorySlot = _sizesBuffer } );
    memorySlots.push_back( memorySlotExchangeInfo_t { .communicationManager = _edgeConfig.getPayloadCommunicationManager(),      .globalKey = encodeGlobalKey(_edgeIndex, _replicaIndex, _edgeType, _consumerPayloadBufferKey),                .memorySlot = _payloadBuffer } );
  }

  private:

  __INLINE__ void createChannels() override
  {
    // Creating consumer channel
    _channel = std::make_shared<HiCR::channel::variableSize::SPSC::Consumer>(
        *_edgeConfig.getCoordinationCommunicationManager(),
        _consumerPayloadBuffer /*payload buffer */,
        _consumerSizesBuffer,
        _consumerCoordinationBufferForSizes->getSourceLocalMemorySlot(),
        _consumerCoordinationBufferForPayloads->getSourceLocalMemorySlot(),
        _producerCoordinationBufferForSizes,
        _producerCoordinationBufferForPayloads,
        _edgeConfig.getBufferSize(),
        _edgeConfig.getBufferCapacity()
      );
  }

  // Buffers associated with this channel
  std::shared_ptr<HiCR::LocalMemorySlot> _sizesBuffer;
  std::shared_ptr<HiCR::LocalMemorySlot> _payloadBuffer;

  // The HiCR channels we use to communicate
  std::shared_ptr<HiCR::channel::variableSize::SPSC::Consumer> _channel;

}; // class Input

} // namespace hLLM::edge
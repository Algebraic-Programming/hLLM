#pragma once

#include "base.hpp"
#include "message.hpp"
#include "hicr/frontends/channel/variableSize/spsc/consumer.hpp"
#include "hicr/frontends/channel/fixedSize/spsc/consumer.hpp"

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
    ///// Allocating additional local buffers required for the consumer data channel

    // Getting required buffer size
    auto sizesBufferSize = HiCR::channel::variableSize::Base::getTokenBufferSize(sizeof(size_t), _edgeConfig.getBufferCapacity());

    // Allocating sizes buffer as a local memory slot
    _dataChannelSizesBuffer   = _edgeConfig.getCoordinationMemoryManager()->allocateLocalMemorySlot(_edgeConfig.getCoordinationMemorySpace(), sizesBufferSize);

    // Allocating payload buffer as a local memory slot
    _dataChannelPayloadBuffer = _edgeConfig.getPayloadMemoryManager()->allocateLocalMemorySlot(_edgeConfig.getPayloadMemorySpace(), _edgeConfig.getBufferSize());

    ///// Allocating additional local buffers required for the consumer medata channel

    // Getting required buffer size
    auto bufferSize = HiCR::channel::fixedSize::Base::getTokenBufferSize(sizeof(Message::metadata_t), _edgeConfig.getBufferCapacity());

    // Allocating payload buffer as a local memory slot
    _metadataChannelPayloadBuffer = _edgeConfig.getCoordinationMemoryManager()->allocateLocalMemorySlot(_edgeConfig.getCoordinationMemorySpace(), bufferSize);
  }

  ~Input()
  {
    // Freeing up buffers
    _edgeConfig.getCoordinationMemoryManager()->freeLocalMemorySlot(_dataChannelSizesBuffer);
    _edgeConfig.getPayloadMemoryManager()->freeLocalMemorySlot(_dataChannelPayloadBuffer);
    _edgeConfig.getCoordinationMemoryManager()->freeLocalMemorySlot(_metadataChannelPayloadBuffer);
  }

  __INLINE__ void getMemorySlotsToExchange(std::vector<memorySlotExchangeInfo_t>& memorySlots) const override
  {
    // Getting key / memory slot pairs for the data channel
    memorySlots.push_back( memorySlotExchangeInfo_t { .communicationManager = _edgeConfig.getCoordinationCommunicationManager(), .globalKey = encodeGlobalKey(_edgeIndex, _replicaIndex, _edgeType, _dataChannelConsumerCoordinationBufferforSizesKey),    .memorySlot = _dataChannelLocalCoordinationBufferForSizes } );
    memorySlots.push_back( memorySlotExchangeInfo_t { .communicationManager = _edgeConfig.getCoordinationCommunicationManager(), .globalKey = encodeGlobalKey(_edgeIndex, _replicaIndex, _edgeType, _dataChannelConsumerCoordinationBufferforPayloadKey),  .memorySlot = _dataChannelLocalCoordinationBufferForPayloads } );
    memorySlots.push_back( memorySlotExchangeInfo_t { .communicationManager = _edgeConfig.getCoordinationCommunicationManager(), .globalKey = encodeGlobalKey(_edgeIndex, _replicaIndex, _edgeType, _dataChannelConsumerSizesBufferKey),                   .memorySlot = _dataChannelSizesBuffer } );
    memorySlots.push_back( memorySlotExchangeInfo_t { .communicationManager = _edgeConfig.getPayloadCommunicationManager(),      .globalKey = encodeGlobalKey(_edgeIndex, _replicaIndex, _edgeType, _dataChannelConsumerPayloadBufferKey),                 .memorySlot = _dataChannelPayloadBuffer } );

    // Getting key / memory slot pairs for the meta data channel
    memorySlots.push_back( memorySlotExchangeInfo_t { .communicationManager = _edgeConfig.getCoordinationCommunicationManager(), .globalKey = encodeGlobalKey(_edgeIndex, _replicaIndex, _edgeType, _metadataChannelConsumerCoordinationBufferKey),        .memorySlot = _metadataChannelLocalCoordinationBuffer } );
    memorySlots.push_back( memorySlotExchangeInfo_t { .communicationManager = _edgeConfig.getCoordinationCommunicationManager(), .globalKey = encodeGlobalKey(_edgeIndex, _replicaIndex, _edgeType, _metadataChannelConsumerPayloadBufferKey),             .memorySlot = _metadataChannelPayloadBuffer } );
  }

  private:

  __INLINE__ void createChannels() override
  {
    // Creating consumer data channel
    _dataChannel = std::make_shared<HiCR::channel::variableSize::SPSC::Consumer>(
        *_edgeConfig.getCoordinationCommunicationManager(),
        _dataChannelConsumerPayloadBuffer /*payload buffer */,
        _dataChannelConsumerSizesBuffer,
        _dataChannelconsumerCoordinationBufferForSizes->getSourceLocalMemorySlot(),
        _dataChannelConsumerCoordinationBufferForPayloads->getSourceLocalMemorySlot(),
        _dataChannelProducerCoordinationBufferForSizes,
        _dataChannelProducerCoordinationBufferForPayloads,
        _edgeConfig.getBufferSize(),
        _edgeConfig.getBufferCapacity()
      );

  // Creating consumer data channel
  _metadataChannel = std::make_shared<HiCR::channel::fixedSize::SPSC::Consumer>(
    *_edgeConfig.getCoordinationCommunicationManager(),
    _metadataChannelConsumerPayloadBuffer,
    _metadataChannelConsumerCoordinationBuffer->getSourceLocalMemorySlot(),
    _metadataChannelProducerCoordinationBuffer,
    sizeof(Message::metadata_t),
    _edgeConfig.getBufferCapacity()
  );
  }

  // Buffers associated with the data channel
  std::shared_ptr<HiCR::LocalMemorySlot> _dataChannelSizesBuffer;
  std::shared_ptr<HiCR::LocalMemorySlot> _dataChannelPayloadBuffer;

  // Buffers associated with the metadata channel
  std::shared_ptr<HiCR::LocalMemorySlot> _metadataChannelPayloadBuffer;

  // The HiCR channels we use to communicate
  std::shared_ptr<HiCR::channel::variableSize::SPSC::Consumer> _dataChannel;
  std::shared_ptr<HiCR::channel::fixedSize::SPSC::Consumer> _metadataChannel;

}; // class Input

} // namespace hLLM::edge
#pragma once

#include "base.hpp"
#include "hicr/core/communicationManager.hpp"
#include "hicr/core/memoryManager.hpp"
#include "hicr/frontends/channel/variableSize/spsc/producer.hpp"
#include "hicr/frontends/channel/fixedSize/spsc/producer.hpp"

namespace hLLM::edge
{

class Output final : public Base
{
  public:

  Output(const configuration::Edge edgeConfig,
        const edgeType_t edgeType,
        const configuration::Edge::edgeIndex_t edgeIndex,
        const configuration::Replica::replicaIndex_t replicaIndex) :
         Base(edgeConfig, edgeType, edgeIndex, replicaIndex)
  {
    _dataChannelProducerSizeInfoBuffer = _edgeConfig.getCoordinationMemoryManager()->allocateLocalMemorySlot(_edgeConfig.getCoordinationMemorySpace(), sizeof(size_t)); 
  }

  ~Output()
  {
    // Freeing up buffers
    _edgeConfig.getCoordinationMemoryManager()->freeLocalMemorySlot(_dataChannelProducerSizeInfoBuffer);
  }

  public:
  
  __INLINE__ void getMemorySlotsToExchange(std::vector<memorySlotExchangeInfo_t>& memorySlots) const override
  {
    // Getting key / memory slot pairs
    memorySlots.push_back( memorySlotExchangeInfo_t { .communicationManager = _edgeConfig.getCoordinationCommunicationManager(), .globalKey = encodeGlobalKey(_edgeIndex, _replicaIndex, _edgeType, _dataChannelProducerCoordinationBufferforSizesKey),   .memorySlot = _dataChannelLocalCoordinationBufferForSizes } );
    memorySlots.push_back( memorySlotExchangeInfo_t { .communicationManager = _edgeConfig.getCoordinationCommunicationManager(), .globalKey = encodeGlobalKey(_edgeIndex, _replicaIndex, _edgeType, _dataChannelProducerCoordinationBufferforPayloadKey), .memorySlot = _dataChannelLocalCoordinationBufferForPayloads } );
    memorySlots.push_back( memorySlotExchangeInfo_t { .communicationManager = _edgeConfig.getCoordinationCommunicationManager(), .globalKey = encodeGlobalKey(_edgeIndex, _replicaIndex, _edgeType, _metadataChannelProducerCoordinationBufferKey),       .memorySlot = _metadataChannelLocalCoordinationBuffer } );
  }

    // Function to check whether the output channels are full
  __INLINE__ bool isFull() const
  { 
    // Requesting the re-check of the channel's usage

    _metadataChannel->updateDepth();
    if (_metadataChannel->isFull() == true) return true;

    _dataChannel->updateDepth();
    if (_dataChannel->isFull() == true) return true;

    // Check if both are not empty
    return false;
  } 

  __INLINE__ void pushMessage(const Message message) const
  {
    if (isFull() == true) HICR_THROW_RUNTIME("Trying to push a message when channel is full. This is a bug in hLLM.");

    auto messagePayloadMemorySlot = _edgeConfig.getPayloadMemoryManager()->registerLocalMemorySlot(_edgeConfig.getPayloadMemorySpace(), (void*)message.getData(), message.getSize());
    _dataChannel->push(messagePayloadMemorySlot);

    auto messageMetadataMemorySlot = _edgeConfig.getCoordinationMemoryManager()->registerLocalMemorySlot(_edgeConfig.getCoordinationMemorySpace(), (void*)&message.getMetadata(), sizeof(Message::metadata_t));
    _metadataChannel->push(messageMetadataMemorySlot);

    _edgeConfig.getPayloadMemoryManager()->freeLocalMemorySlot(messagePayloadMemorySlot);
    _edgeConfig.getCoordinationMemoryManager()->freeLocalMemorySlot(messageMetadataMemorySlot);
  }

  private:

  //// We use a variable size SPSC channel to communicate, where as an output edge we take the producer interface

  __INLINE__ void createChannels() override
  {
    // Creating producer data channel
    _dataChannel = std::make_shared<HiCR::channel::variableSize::SPSC::Producer>(
      *_edgeConfig.getCoordinationCommunicationManager(),
      _dataChannelProducerSizeInfoBuffer,
      _dataChannelConsumerPayloadBuffer,
      _dataChannelConsumerSizesBuffer,
      _dataChannelProducerCoordinationBufferForSizes->getSourceLocalMemorySlot(),
      _dataChannelProducerCoordinationBufferForPayloads->getSourceLocalMemorySlot(),
      _dataChannelconsumerCoordinationBufferForSizes,
      _dataChannelConsumerCoordinationBufferForPayloads,
      _edgeConfig.getBufferSize(),
      sizeof(uint8_t),
      _edgeConfig.getBufferCapacity()
    );

    // Creating producer metadata channel
    _metadataChannel = std::make_shared<HiCR::channel::fixedSize::SPSC::Producer>(
      *_edgeConfig.getCoordinationCommunicationManager(),
      _metadataChannelConsumerCoordinationBuffer,
      _metadataChannelProducerCoordinationBuffer->getSourceLocalMemorySlot(),
      _metadataChannelConsumerCoordinationBuffer,
      sizeof(Message::metadata_t),
      _edgeConfig.getBufferCapacity()
    );
  }

  // Internal memory slot for data channel producer coordination buffer
  std::shared_ptr<HiCR::LocalMemorySlot> _dataChannelProducerSizeInfoBuffer;

  // The HiCR channels we use to communicate
  std::shared_ptr<HiCR::channel::variableSize::SPSC::Producer> _dataChannel;
  std::shared_ptr<HiCR::channel::fixedSize::SPSC::Producer> _metadataChannel;
  
}; // class Output

} // namespace hLLM::edge
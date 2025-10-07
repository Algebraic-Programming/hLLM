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
        const configuration::Partition::partitionIndex_t producerPartitionIndex,
        const configuration::Partition::partitionIndex_t consumerPartitionIndex,
        const configuration::Replica::replicaIndex_t replicaIndex) :
         Base(edgeConfig, edgeType, edgeIndex, producerPartitionIndex, consumerPartitionIndex, replicaIndex)
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
    // printf("Exchanging %lu\n", encodeGlobalKey(_edgeIndex, _producerPartitionIndex, _consumerPartitionIndex, _replicaIndex, _edgeType, _dataChannelConsumerCoordinationBufferforSizesKey) );
    // printf("Exchanging %lu\n", encodeGlobalKey(_edgeIndex, _producerPartitionIndex, _consumerPartitionIndex, _replicaIndex, _edgeType, _dataChannelConsumerCoordinationBufferforPayloadKey) );
    // printf("Exchanging %lu\n", encodeGlobalKey(_edgeIndex, _producerPartitionIndex, _consumerPartitionIndex, _replicaIndex, _edgeType, _dataChannelConsumerSizesBufferKey) );
    // printf("Exchanging %lu\n", encodeGlobalKey(_edgeIndex, _producerPartitionIndex, _consumerPartitionIndex, _replicaIndex, _edgeType, _dataChannelConsumerPayloadBufferKey) );
    // printf("Exchanging %lu\n", encodeGlobalKey(_edgeIndex, _producerPartitionIndex, _consumerPartitionIndex, _replicaIndex, _edgeType, _metadataChannelConsumerCoordinationBufferKey) );
    // printf("Exchanging %lu\n", encodeGlobalKey(_edgeIndex, _producerPartitionIndex, _consumerPartitionIndex, _replicaIndex, _edgeType, _metadataChannelConsumerPayloadBufferKey));

    // Getting key / memory slot pairs for the data channel
    memorySlots.push_back( memorySlotExchangeInfo_t { .communicationManager = _edgeConfig.getCoordinationCommunicationManager(), .globalKey = encodeGlobalKey(_edgeIndex, _producerPartitionIndex, _consumerPartitionIndex, _replicaIndex, _edgeType, _dataChannelConsumerCoordinationBufferforSizesKey),    .memorySlot = _dataChannelLocalCoordinationBufferForSizes } );
    memorySlots.push_back( memorySlotExchangeInfo_t { .communicationManager = _edgeConfig.getCoordinationCommunicationManager(), .globalKey = encodeGlobalKey(_edgeIndex, _producerPartitionIndex, _consumerPartitionIndex, _replicaIndex, _edgeType, _dataChannelConsumerCoordinationBufferforPayloadKey),  .memorySlot = _dataChannelLocalCoordinationBufferForPayloads } );
    memorySlots.push_back( memorySlotExchangeInfo_t { .communicationManager = _edgeConfig.getCoordinationCommunicationManager(), .globalKey = encodeGlobalKey(_edgeIndex, _producerPartitionIndex, _consumerPartitionIndex, _replicaIndex, _edgeType, _dataChannelConsumerSizesBufferKey),                   .memorySlot = _dataChannelSizesBuffer } );
    memorySlots.push_back( memorySlotExchangeInfo_t { .communicationManager = _edgeConfig.getPayloadCommunicationManager(),      .globalKey = encodeGlobalKey(_edgeIndex, _producerPartitionIndex, _consumerPartitionIndex, _replicaIndex, _edgeType, _dataChannelConsumerPayloadBufferKey),                 .memorySlot = _dataChannelPayloadBuffer } );

    // Getting key / memory slot pairs for the meta data channel
    memorySlots.push_back( memorySlotExchangeInfo_t { .communicationManager = _edgeConfig.getCoordinationCommunicationManager(), .globalKey = encodeGlobalKey(_edgeIndex, _producerPartitionIndex, _consumerPartitionIndex, _replicaIndex, _edgeType, _metadataChannelConsumerCoordinationBufferKey),        .memorySlot = _metadataChannelLocalCoordinationBuffer } );
    memorySlots.push_back( memorySlotExchangeInfo_t { .communicationManager = _edgeConfig.getCoordinationCommunicationManager(), .globalKey = encodeGlobalKey(_edgeIndex, _producerPartitionIndex, _consumerPartitionIndex, _replicaIndex, _edgeType, _metadataChannelConsumerPayloadBufferKey),             .memorySlot = _metadataChannelPayloadBuffer } );
  }

  // Function to check for incoming messages in the edge
  __INLINE__ bool hasMessage() const
  { 
    _metadataChannel->updateDepth();
    // printf("Input Metadata Depth: %lu\n", _metadataChannel->getDepth());
    if (_metadataChannel->isEmpty() == true) return false;

    // Requesting the re-check of the channel's usage
    _dataChannel->updateDepth();
    // printf("Input Data Depth: %lu\n", _metadataChannel->getDepth());
    if (_dataChannel->isEmpty() == true) return false;

    return true;
  } 

  __INLINE__ Message getMessage() const
  {
    if (hasMessage() == false) HICR_THROW_RUNTIME("Trying to get message when there is none available. This is a bug in hLLM.");

    // Receiving message
    const auto dataBufferPtr = (uint8_t*) _dataChannel->getPayloadBufferMemorySlot()->getSourceLocalMemorySlot()->getPointer();
    const auto dataToken     = _dataChannel->peek();
    const auto dataMessagePos = dataToken[0];
    const auto dataMessagePtr = &dataBufferPtr[dataMessagePos];
    const auto dataMessageSize = dataToken[1];

    const auto metadataBufferPtr  = (uint8_t*) _metadataChannel->getTokenBuffer()->getSourceLocalMemorySlot()->getPointer(); 
    const auto metadataToken      = _metadataChannel->peek();
    const auto metadataMessagePos = metadataToken;
    const auto metadataMessagePtr = (Message::metadata_t*)&metadataBufferPtr[metadataMessagePos];
    const Message::metadata_t metadata = *metadataMessagePtr;

    return Message(dataMessagePtr, dataMessageSize, metadata);
  }

  __INLINE__ void popMessage()
  {
    if (hasMessage() == false) HICR_THROW_RUNTIME("Trying to pop message when there is none available. This is a bug in hLLM.");

    _dataChannel->pop();
    _metadataChannel->pop();
  }

  private:

  __INLINE__ void createChannels() override
  {
    // Creating consumer data channel
    _dataChannel = std::make_shared<HiCR::channel::variableSize::SPSC::Consumer>(
        *_edgeConfig.getCoordinationCommunicationManager(),
        *_edgeConfig.getPayloadCommunicationManager(),
        _dataChannelConsumerPayloadBuffer /*payload buffer */,
        _dataChannelConsumerSizesBuffer,
        _dataChannelConsumerCoordinationBufferForSizes->getSourceLocalMemorySlot(),
        _dataChannelConsumerCoordinationBufferForPayloads->getSourceLocalMemorySlot(),
        _dataChannelProducerCoordinationBufferForSizes,
        _dataChannelProducerCoordinationBufferForPayloads,
        _edgeConfig.getBufferSize(),
        _edgeConfig.getBufferCapacity()
      );

    // Creating consumer data channel
    _metadataChannel = std::make_shared<HiCR::channel::fixedSize::SPSC::Consumer>(
      *_edgeConfig.getCoordinationCommunicationManager(),
      *_edgeConfig.getPayloadCommunicationManager(),
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
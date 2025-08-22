#pragma once

#include "engine.hpp"

namespace hLLM
{

/**
   * Retrieves one of the producer channels creates during deployment.
   * 
   * If the provided name is not registered for this instance, the function will produce an exception.
   * 
   * @param[in] name The channel name
   * @return The requested producer
   */
__INLINE__ std::unique_ptr<channel::channelProducerInterface_t> Engine::moveProducer(const std::string &name)
{
  if (_producers.contains(name) == false)
    HICR_THROW_LOGIC("Requested producer ('%s') is not defined for this instance ('%s')\n", name.c_str(), _deployr.getLocalInstance().getName().c_str());

  auto producer = std::move(_producers[name]);
  _producers.erase(name);

  return producer;
}

/**
   * Retrieves one of the consumer channels creates during deployment.
   * 
   * If the provided name is not registered for this instance, the function will produce an exception.
   * 
   * @param[in] name The channel name
   * @return The requested consumer
   */
__INLINE__ std::unique_ptr<channel::channelConsumerInterface_t> Engine::moveConsumer(const std::string &name)
{
  if (_consumers.contains(name) == false)
    HICR_THROW_LOGIC("Requested consumer ('%s') is not defined for this instance ('%s')\n", name.c_str(), _deployr.getLocalInstance().getName().c_str());

  auto consumer = std::move(_consumers[name]);
  _consumers.erase(name);
  return consumer;
}

void Engine::createChannels()
{
  // Create the channels
  auto channelId = 0;

  for (const auto &channelInfo : hicr::json::getArray<nlohmann::json>(_config, "Channels"))
  {
    // Getting channel's name
    const auto &name = hicr::json::getString(channelInfo, "Name");

    // Getting channel's producer
    const auto &producerName = hicr::json::getString(channelInfo, "Producer");

    // Getting channel's consumer
    const auto &consumerName = hicr::json::getString(channelInfo, "Consumer");

    // Getting buffer capacity (max token count)
    const auto &bufferCapacity = hicr::json::getNumber<uint64_t>(channelInfo, "Buffer Capacity (Tokens)");

    // Getting buffer size (bytes)
    const auto &bufferSize = hicr::json::getNumber<uint64_t>(channelInfo, "Buffer Size (Bytes)");

    // Get producer HiCR instance id
    const auto producerId         = _deployr.getDeployment().getPairings().at(producerName);
    const auto producerInstanceId = _deployr.getDeployment().getHosts()[producerId].getInstanceId();

    // Getting consumer instance id
    const auto consumerId         = _deployr.getDeployment().getPairings().at(consumerName);
    const auto consumerInstanceId = _deployr.getDeployment().getHosts()[consumerId].getInstanceId();

    // Creating channel object and increase channelId
    //TODO: for malleability check the channel type to be used
    auto [producer, consumer] = createChannel(channelId++, name, producerInstanceId, consumerInstanceId, bufferCapacity, bufferSize);

    // Adding channel to map, only if defined
    if (producer != nullptr) _producers.emplace(name, std::move(producer));
    if (consumer != nullptr) _consumers.emplace(name, std::move(consumer));
  }
}

#define SIZES_BUFFER_KEY 0
#define CONSUMER_COORDINATION_BUFFER_FOR_SIZES_KEY 3
#define CONSUMER_COORDINATION_BUFFER_FOR_PAYLOADS_KEY 4
#define CONSUMER_PAYLOAD_KEY 5
#define _CHANNEL_CREATION_ERROR 1

__INLINE__ std::pair<std::unique_ptr<channel::channelProducerInterface_t>, std::unique_ptr<channel::channelConsumerInterface_t>> Engine::createChannel(const size_t channelTag,
                                                                                                                                                       const std::string channelName,
                                                                                                                                                       const size_t producerId,
                                                                                                                                                       const size_t consumerId,
                                                                                                                                                       const size_t bufferCapacity,
                                                                                                                                                       const size_t bufferSize)
{
  // Interfaces for the channel
  std::unique_ptr<channel::channelConsumerInterface_t> consumerInterface = nullptr;
  std::unique_ptr<channel::channelProducerInterface_t> producerInterface = nullptr;

  // Getting my local instance index
  auto localInstanceId = _instanceManager->getCurrentInstance()->getId();

  // Checking if I am consumer
  bool isConsumer = consumerId == localInstanceId;

  // Checking if I am producer
  bool isProducer = producerId == localInstanceId;

  // Finding the first memory space to create our channels
  auto &bufferMemorySpace = _channelMemSpace;

  // Collection of memory slots to exchange and their keys
  std::vector<HiCR::CommunicationManager::globalKeyMemorySlotPair_t> memorySlotsToExchange;

  // Temporary storage for coordination buffer pointers
  std::shared_ptr<HiCR::LocalMemorySlot> localCoordinationBufferForSizes    = nullptr;
  std::shared_ptr<HiCR::LocalMemorySlot> localCoordinationBufferForPayloads = nullptr;

  ////// Pre-Exchange: Create local slots and register them for exchange

  // If I am consumer, create the consumer interface for the channel
  if (isConsumer == true)
  {
    // Getting required buffer sizes
    auto sizesBufferSize = HiCR::channel::variableSize::Base::getTokenBufferSize(sizeof(size_t), bufferCapacity);

    // Allocating sizes buffer as a local memory slot
    auto sizesBufferSlot = _rpcEngine->getMemoryManager()->allocateLocalMemorySlot(bufferMemorySpace, sizesBufferSize);

    // Allocating payload buffer as a local memory slot
    auto payloadBufferSlot = _rpcEngine->getMemoryManager()->allocateLocalMemorySlot(bufferMemorySpace, bufferSize);

    // Getting required buffer size
    auto coordinationBufferSize = HiCR::channel::variableSize::Base::getCoordinationBufferSize();

    // Allocating coordination buffer for internal message size metadata
    localCoordinationBufferForSizes = _rpcEngine->getMemoryManager()->allocateLocalMemorySlot(bufferMemorySpace, coordinationBufferSize);

    // Allocating coordination buffer for internal payload metadata
    localCoordinationBufferForPayloads = _rpcEngine->getMemoryManager()->allocateLocalMemorySlot(bufferMemorySpace, coordinationBufferSize);

    // Initializing coordination buffer (sets to zero the counters)
    HiCR::channel::variableSize::Base::initializeCoordinationBuffer(localCoordinationBufferForSizes);
    HiCR::channel::variableSize::Base::initializeCoordinationBuffer(localCoordinationBufferForPayloads);

    // Adding memory slots to exchange
    memorySlotsToExchange.push_back({SIZES_BUFFER_KEY, sizesBufferSlot});
    memorySlotsToExchange.push_back({CONSUMER_COORDINATION_BUFFER_FOR_SIZES_KEY, localCoordinationBufferForSizes});
    memorySlotsToExchange.push_back({CONSUMER_COORDINATION_BUFFER_FOR_PAYLOADS_KEY, localCoordinationBufferForPayloads});
    memorySlotsToExchange.push_back({CONSUMER_PAYLOAD_KEY, payloadBufferSlot});
  }

  // If I am producer, exchange relevant buffers
  if (isProducer == true)
  {
    // nothing to exchange for now, but this might change in the future if we support other types of channels
  }

  ////// Exchange: local memory slots to become global for them to be used by the remote end
  _rpcEngine->getCommunicationManager()->exchangeGlobalMemorySlots(channelTag, memorySlotsToExchange);

  // Synchronizing so that all actors have finished registering their global memory slots
  _rpcEngine->getCommunicationManager()->fence(channelTag);

  ///// Post exchange: create consumer/producer intefaces

  // If I am a consumer, create consumer interface now
  if (isConsumer == true)
  {
    // Obtaining the globally exchanged memory slots
    auto globalSizesBufferSlot                 = _rpcEngine->getCommunicationManager()->getGlobalMemorySlot(channelTag, SIZES_BUFFER_KEY);
    auto consumerCoordinationBufferForSizes    = _rpcEngine->getCommunicationManager()->getGlobalMemorySlot(channelTag, CONSUMER_COORDINATION_BUFFER_FOR_SIZES_KEY);
    auto consumerCoordinationBufferForPayloads = _rpcEngine->getCommunicationManager()->getGlobalMemorySlot(channelTag, CONSUMER_COORDINATION_BUFFER_FOR_PAYLOADS_KEY);
    auto globalPayloadBuffer                   = _rpcEngine->getCommunicationManager()->getGlobalMemorySlot(channelTag, CONSUMER_PAYLOAD_KEY);

    // Creating producer and consumer channels
    consumerInterface = std::make_unique<channel::channelConsumerInterface_t>(*_rpcEngine->getCommunicationManager(),
                                                                              globalPayloadBuffer,   /* payloadBuffer */
                                                                              globalSizesBufferSlot, /* tokenSizeBuffer */
                                                                              localCoordinationBufferForSizes,
                                                                              localCoordinationBufferForPayloads,
                                                                              consumerCoordinationBufferForSizes,
                                                                              consumerCoordinationBufferForPayloads,
                                                                              bufferSize,
                                                                              bufferCapacity);
  }

  // If I am producer, create the producer interface for the channel
  if (isProducer == true)
  {
    // Getting required buffer size
    auto coordinationBufferSize = HiCR::channel::variableSize::Base::getCoordinationBufferSize();

    // Allocating sizes buffer as a local memory slot
    auto coordinationBufferForSizes = _rpcEngine->getMemoryManager()->allocateLocalMemorySlot(bufferMemorySpace, coordinationBufferSize);

    auto coordinationBufferForPayloads = _rpcEngine->getMemoryManager()->allocateLocalMemorySlot(bufferMemorySpace, coordinationBufferSize);

    auto sizeInfoBuffer = _rpcEngine->getMemoryManager()->allocateLocalMemorySlot(bufferMemorySpace, sizeof(size_t));

    // Initializing coordination buffers for message sizes and payloads (sets to zero the counters)
    HiCR::channel::variableSize::Base::initializeCoordinationBuffer(coordinationBufferForSizes);
    HiCR::channel::variableSize::Base::initializeCoordinationBuffer(coordinationBufferForPayloads);

    // Obtaining the globally exchanged memory slots
    auto sizesBuffer                           = _rpcEngine->getCommunicationManager()->getGlobalMemorySlot(channelTag, SIZES_BUFFER_KEY);
    auto consumerCoordinationBufferForSizes    = _rpcEngine->getCommunicationManager()->getGlobalMemorySlot(channelTag, CONSUMER_COORDINATION_BUFFER_FOR_SIZES_KEY);
    auto consumerCoordinationBufferForPayloads = _rpcEngine->getCommunicationManager()->getGlobalMemorySlot(channelTag, CONSUMER_COORDINATION_BUFFER_FOR_PAYLOADS_KEY);
    auto payloadBuffer                         = _rpcEngine->getCommunicationManager()->getGlobalMemorySlot(channelTag, CONSUMER_PAYLOAD_KEY);

    // Creating producer and consumer channels
    producerInterface = std::make_unique<channel::channelProducerInterface_t>(*_rpcEngine->getCommunicationManager(),
                                                                              sizeInfoBuffer,
                                                                              payloadBuffer,
                                                                              sizesBuffer,
                                                                              coordinationBufferForSizes,
                                                                              coordinationBufferForPayloads,
                                                                              consumerCoordinationBufferForSizes,
                                                                              consumerCoordinationBufferForPayloads,
                                                                              bufferSize,
                                                                              sizeof(char),
                                                                              bufferCapacity);
  }

  return {std::move(producerInterface), std::move(consumerInterface)};
}
} // namespace hLLM
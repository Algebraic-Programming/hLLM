#pragma once

#include <hicr/core/communicationManager.hpp>

#include "engine.hpp"

#define __HLLM_CREATE_PRODUCER_RPC "[hLLM] Create Producer"
#define __HLLM_CREATE_CONSUMER_RPC "[hLLM] Create Consumer"
#define __HLLM_TERMINATE_CHANNEL_CREATION "[hLLM] Terminate Channel Creation"
#define __HLLM_GET_CHANNEL_INFORMATION "[hLLM] Get Channel Information"

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
  if (_producers.contains(name) == false) HICR_THROW_LOGIC("Requested producer ('%s') is not defined for this instance ('%s')\n", name.c_str(), _partitionName.c_str());

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
  if (_consumers.contains(name) == false) HICR_THROW_LOGIC("Requested consumer ('%s') is not defined for this instance ('%s')\n", name.c_str(), _partitionName.c_str());

  auto consumer = std::move(_consumers[name]);
  _consumers.erase(name);
  return consumer;
}

// void newcreate() { std::set<HiCR::GlobalMemorySlot::tag_t> tags; }

void Engine::createChannels(HiCR::CommunicationManager         &bufferedCommunicationManager,
                            HiCR::MemoryManager                &bufferedMemoryManager,
                            std::shared_ptr<HiCR::MemorySpace> &bufferedMemorySpace,
                            HiCR::CommunicationManager         &unbufferedCommunicationManager,
                            HiCR::MemoryManager                &unbufferedMemoryManager,
                            std::shared_ptr<HiCR::MemorySpace> &unbufferedMemorySpace)
{
  // Create the channels
  auto channelId = 0;

  const auto &dependencies = _config["Dependencies"];

  std::set<std::string> bufferedDependencies;
  std::set<std::string> unbufferedDependencies;

  for (const auto &[name, dependency] : dependencies.items())
  {
    if (hicr::json::getString(dependency, "Type") == "Buffered") { bufferedDependencies.emplace(name); }
    else { unbufferedDependencies.emplace(name); }
  }

  // Create unbuffered dependencies channels
  for (const auto &name : unbufferedDependencies)
  {
    // Get dependency informations
    const auto &dependency = hicr::json::getObject(dependencies, name);

    // Getting channel's producer
    const auto &producerPartitionId = hicr::json::getNumber<partitionId_t>(dependency, "Producer");

    // Getting channel's consumer
    const auto &consumerPartitionId = hicr::json::getNumber<partitionId_t>(dependency, "Consumer");

    // Getting buffer capacity (max token count)
    const auto &bufferCapacity = hicr::json::getNumber<uint64_t>(dependency, "Buffer Capacity (Tokens)");

    // Getting buffer size (bytes)
    const auto &bufferSize = hicr::json::getNumber<uint64_t>(dependency, "Buffer Size (Bytes)");

    // Creating channel object and increase channelId
    //TODO: for malleability check the channel type to be used
    auto [producer, consumer] = createChannel(channelId++,
                                              name,
                                              producerPartitionId == _partitionId,
                                              consumerPartitionId == _partitionId,
                                              unbufferedCommunicationManager,
                                              unbufferedMemoryManager,
                                              unbufferedMemorySpace,
                                              bufferCapacity,
                                              bufferSize);

    // Adding channel to map, only if defined
    if (producer != nullptr) _producers.emplace(name, std::move(producer));
    if (consumer != nullptr) _consumers.emplace(name, std::move(consumer));
  }

  for (const auto &name : bufferedDependencies)
  {
    // Get dependency informations
    const auto &dependency = hicr::json::getObject(dependencies, name);

    // Getting channel's producer
    const auto &producerPartitionId = hicr::json::getNumber<partitionId_t>(dependency, "Producer");

    // Getting channel's consumer
    const auto &consumerPartitionId = hicr::json::getNumber<partitionId_t>(dependency, "Consumer");

    // Getting buffer capacity (max token count)
    const auto &bufferCapacity = hicr::json::getNumber<uint64_t>(dependency, "Buffer Capacity (Tokens)");

    // Getting buffer size (bytes)
    const auto &bufferSize = hicr::json::getNumber<uint64_t>(dependency, "Buffer Size (Bytes)");

    // Creating channel object and increase channelId
    //TODO: for malleability check the channel type to be used
    auto [producer, consumer] = createChannel(channelId++,
                                              name,
                                              producerPartitionId == _partitionId,
                                              consumerPartitionId == _partitionId,
                                              bufferedCommunicationManager,
                                              bufferedMemoryManager,
                                              bufferedMemorySpace,
                                              bufferCapacity,
                                              bufferSize);

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

__INLINE__ std::unique_ptr<channel::channelConsumerInterface_t> createConsumer(HiCR::CommunicationManager             &communicationManager,
                                                                               HiCR::GlobalMemorySlot::tag_t           channelTag,
                                                                               size_t                                  bufferSize,
                                                                               size_t                                  bufferCapacity,
                                                                               std::shared_ptr<HiCR::LocalMemorySlot> &localCoordinationBufferForSizes,
                                                                               std::shared_ptr<HiCR::LocalMemorySlot> &localCoordinationBufferForPayloads)
{
  // Obtaining the globally exchanged memory slots
  auto globalSizesBufferSlot                 = communicationManager.getGlobalMemorySlot(channelTag, SIZES_BUFFER_KEY);
  auto consumerCoordinationBufferForSizes    = communicationManager.getGlobalMemorySlot(channelTag, CONSUMER_COORDINATION_BUFFER_FOR_SIZES_KEY);
  auto consumerCoordinationBufferForPayloads = communicationManager.getGlobalMemorySlot(channelTag, CONSUMER_COORDINATION_BUFFER_FOR_PAYLOADS_KEY);
  auto globalPayloadBuffer                   = communicationManager.getGlobalMemorySlot(channelTag, CONSUMER_PAYLOAD_KEY);

  // Creating producer and consumer channels
  return std::make_unique<channel::channelConsumerInterface_t>(communicationManager,
                                                               globalPayloadBuffer,   /* payloadBuffer */
                                                               globalSizesBufferSlot, /* tokenSizeBuffer */
                                                               localCoordinationBufferForSizes,
                                                               localCoordinationBufferForPayloads,
                                                               consumerCoordinationBufferForSizes,
                                                               consumerCoordinationBufferForPayloads,
                                                               bufferSize,
                                                               bufferCapacity);
}

__INLINE__ std::unique_ptr<channel::channelProducerInterface_t> createProducer(HiCR::CommunicationManager         &communicationManager,
                                                                               HiCR::MemoryManager                &memoryManager,
                                                                               std::shared_ptr<HiCR::MemorySpace> &memorySpace,
                                                                               HiCR::GlobalMemorySlot::tag_t       channelTag,
                                                                               size_t                              bufferSize,
                                                                               size_t                              bufferCapacity)
{
  // Getting required buffer size
  auto coordinationBufferSize = HiCR::channel::variableSize::Base::getCoordinationBufferSize();

  // Allocating sizes buffer as a local memory slot
  auto coordinationBufferForSizes = memoryManager.allocateLocalMemorySlot(memorySpace, coordinationBufferSize);

  auto coordinationBufferForPayloads = memoryManager.allocateLocalMemorySlot(memorySpace, coordinationBufferSize);

  auto sizeInfoBuffer = memoryManager.allocateLocalMemorySlot(memorySpace, sizeof(size_t));

  // Initializing coordination buffers for message sizes and payloads (sets to zero the counters)
  HiCR::channel::variableSize::Base::initializeCoordinationBuffer(coordinationBufferForSizes);
  HiCR::channel::variableSize::Base::initializeCoordinationBuffer(coordinationBufferForPayloads);

  // Obtaining the globally exchanged memory slots
  auto sizesBuffer                           = communicationManager.getGlobalMemorySlot(channelTag, SIZES_BUFFER_KEY);
  auto consumerCoordinationBufferForSizes    = communicationManager.getGlobalMemorySlot(channelTag, CONSUMER_COORDINATION_BUFFER_FOR_SIZES_KEY);
  auto consumerCoordinationBufferForPayloads = communicationManager.getGlobalMemorySlot(channelTag, CONSUMER_COORDINATION_BUFFER_FOR_PAYLOADS_KEY);
  auto payloadBuffer                         = communicationManager.getGlobalMemorySlot(channelTag, CONSUMER_PAYLOAD_KEY);

  // Creating producer and consumer channels
  return std::make_unique<channel::channelProducerInterface_t>(communicationManager,
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

__INLINE__ std::vector<HiCR::CommunicationManager::globalKeyMemorySlotPair_t> exchangeGlobalMemorySlots(HiCR::MemoryManager                    &memoryManager,
                                                                                                        std::shared_ptr<HiCR::MemorySpace>     &memorySpace,
                                                                                                        HiCR::GlobalMemorySlot::tag_t           channelTag,
                                                                                                        size_t                                  bufferSize,
                                                                                                        size_t                                  bufferCapacity,
                                                                                                        std::shared_ptr<HiCR::LocalMemorySlot> &localCoordinationBufferForSizes,
                                                                                                        std::shared_ptr<HiCR::LocalMemorySlot> &localCoordinationBufferForPayloads)
{
  // Collection of memory slots to exchange and their keys
  std::vector<HiCR::CommunicationManager::globalKeyMemorySlotPair_t> memorySlotsToExchange;
  // Getting required buffer sizes
  auto sizesBufferSize = HiCR::channel::variableSize::Base::getTokenBufferSize(sizeof(size_t), bufferCapacity);

  // Allocating sizes buffer as a local memory slot
  auto sizesBufferSlot = memoryManager.allocateLocalMemorySlot(memorySpace, sizesBufferSize);

  // Allocating payload buffer as a local memory slot
  auto payloadBufferSlot = memoryManager.allocateLocalMemorySlot(memorySpace, bufferSize);

  // Getting required buffer size
  auto coordinationBufferSize = HiCR::channel::variableSize::Base::getCoordinationBufferSize();

  // Allocating coordination buffer for internal message size metadata
  localCoordinationBufferForSizes = memoryManager.allocateLocalMemorySlot(memorySpace, coordinationBufferSize);

  // Allocating coordination buffer for internal payload metadata
  localCoordinationBufferForPayloads = memoryManager.allocateLocalMemorySlot(memorySpace, coordinationBufferSize);

  // Initializing coordination buffer (sets to zero the counters)
  HiCR::channel::variableSize::Base::initializeCoordinationBuffer(localCoordinationBufferForSizes);
  HiCR::channel::variableSize::Base::initializeCoordinationBuffer(localCoordinationBufferForPayloads);

  // Adding memory slots to exchange
  memorySlotsToExchange.push_back({SIZES_BUFFER_KEY, sizesBufferSlot});
  memorySlotsToExchange.push_back({CONSUMER_COORDINATION_BUFFER_FOR_SIZES_KEY, localCoordinationBufferForSizes});
  memorySlotsToExchange.push_back({CONSUMER_COORDINATION_BUFFER_FOR_PAYLOADS_KEY, localCoordinationBufferForPayloads});
  memorySlotsToExchange.push_back({CONSUMER_PAYLOAD_KEY, payloadBufferSlot});

  return memorySlotsToExchange;
}


__INLINE__ std::pair<std::unique_ptr<channel::channelProducerInterface_t>, std::unique_ptr<channel::channelConsumerInterface_t>> Engine::createChannel(
  const size_t                        channelTag,
  const std::string                   channelName,
  const bool                          isProducer,
  const bool                          isConsumer,
  HiCR::CommunicationManager         &communicationManager,
  HiCR::MemoryManager                &memoryManager,
  std::shared_ptr<HiCR::MemorySpace> &memorySpace,
  const size_t                        bufferCapacity,
  const size_t                        bufferSize)
{
  // Interfaces for the channel
  std::unique_ptr<channel::channelConsumerInterface_t> consumerInterface = nullptr;
  std::unique_ptr<channel::channelProducerInterface_t> producerInterface = nullptr;

  // Collection of memory slots to exchange and their keys
  std::vector<HiCR::CommunicationManager::globalKeyMemorySlotPair_t> memorySlotsToExchange;

  // Temporary storage for coordination buffer pointers
  std::shared_ptr<HiCR::LocalMemorySlot> localCoordinationBufferForSizes    = nullptr;
  std::shared_ptr<HiCR::LocalMemorySlot> localCoordinationBufferForPayloads = nullptr;

  ////// Pre-Exchange: Create local slots and register them for exchange
  if (isConsumer == true)
  {
    memorySlotsToExchange =
      exchangeGlobalMemorySlots(memoryManager, memorySpace, channelTag, bufferSize, bufferCapacity, localCoordinationBufferForSizes, localCoordinationBufferForPayloads);
  }
  ////// Exchange: local memory slots to become global for them to be used by the remote end
  communicationManager.exchangeGlobalMemorySlots(channelTag, memorySlotsToExchange);
  // Synchronizing so that all actors have finished registering their global memory slots
  communicationManager.fence(channelTag);

  ///// Post exchange: create consumer/producer intefaces
  // If I am a consumer, create consumer interface now
  if (isConsumer == true)
  {
    consumerInterface = createConsumer(communicationManager, channelTag, bufferSize, bufferCapacity, localCoordinationBufferForSizes, localCoordinationBufferForPayloads);
  }

  // If I am producer, create the producer interface for the channel
  if (isProducer == true) { producerInterface = createProducer(communicationManager, memoryManager, memorySpace, channelTag, bufferSize, bufferCapacity); }

  return {std::move(producerInterface), std::move(consumerInterface)};
}
} // namespace hLLM
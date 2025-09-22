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

#define SIZES_BUFFER_KEY 0
#define CONSUMER_COORDINATION_BUFFER_FOR_SIZES_KEY 3
#define CONSUMER_COORDINATION_BUFFER_FOR_PAYLOADS_KEY 4
#define CONSUMER_PAYLOAD_KEY 5
#define _CHANNEL_CREATION_ERROR 1

__INLINE__ std::vector<HiCR::CommunicationManager::globalKeyMemorySlotPair_t> allocateChannelBuffers(
  HiCR::MemoryManager                                                             &memoryManager,
  std::shared_ptr<HiCR::MemorySpace>                                              &memorySpace,
  HiCR::GlobalMemorySlot::tag_t                                                    channelTag,
  size_t                                                                           bufferSize,
  size_t                                                                           bufferCapacity,
  std::map<HiCR::GlobalMemorySlot::tag_t, std::shared_ptr<HiCR::LocalMemorySlot>> &localCoordinationBufferForSizesMap,
  std::map<HiCR::GlobalMemorySlot::tag_t, std::shared_ptr<HiCR::LocalMemorySlot>> &localCoordinationBufferForPayloadsMap)
{
  // Vector to store slots to exchange
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
  auto localCoordinationBufferForSizes = memoryManager.allocateLocalMemorySlot(memorySpace, coordinationBufferSize);

  // Allocating coordination buffer for internal payload metadata
  auto localCoordinationBufferForPayloads = memoryManager.allocateLocalMemorySlot(memorySpace, coordinationBufferSize);

  // Initializing coordination buffer (sets to zero the counters)
  HiCR::channel::variableSize::Base::initializeCoordinationBuffer(localCoordinationBufferForSizes);
  HiCR::channel::variableSize::Base::initializeCoordinationBuffer(localCoordinationBufferForPayloads);

  // Adding memory slots to exchange
  memorySlotsToExchange.push_back({SIZES_BUFFER_KEY, sizesBufferSlot});
  memorySlotsToExchange.push_back({CONSUMER_COORDINATION_BUFFER_FOR_SIZES_KEY, localCoordinationBufferForSizes});
  memorySlotsToExchange.push_back({CONSUMER_COORDINATION_BUFFER_FOR_PAYLOADS_KEY, localCoordinationBufferForPayloads});
  memorySlotsToExchange.push_back({CONSUMER_PAYLOAD_KEY, payloadBufferSlot});

  // Store coordination buffers
  localCoordinationBufferForSizesMap[channelTag]    = std::move(localCoordinationBufferForSizes);
  localCoordinationBufferForPayloadsMap[channelTag] = std::move(localCoordinationBufferForPayloads);

  return memorySlotsToExchange;
}

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

__INLINE__ void createChannelBuffers(HiCR::MemoryManager                                                                                         &memoryManager,
                                     std::shared_ptr<HiCR::MemorySpace>                                                                          &memorySpace,
                                     std::map<HiCR::GlobalMemorySlot::tag_t, nlohmann::json>                                                     &consumers,
                                     std::map<HiCR::GlobalMemorySlot::tag_t, std::vector<HiCR::CommunicationManager::globalKeyMemorySlotPair_t>> &memorySlotsToExchange,
                                     std::map<HiCR::GlobalMemorySlot::tag_t, std::shared_ptr<HiCR::LocalMemorySlot>>                             &coordinationBufferSizesSlots,
                                     std::map<HiCR::GlobalMemorySlot::tag_t, std::shared_ptr<HiCR::LocalMemorySlot>>                             &coordinationBufferPayloadsSlots)
{
  // Exchange unbuffered memory slots
  for (const auto &[tag, dependency] : consumers)
  {
    // Getting buffer capacity (max token count)
    const auto &bufferCapacity = hicr::json::getNumber<uint64_t>(dependency, "Buffer Capacity (Tokens)");

    // Getting buffer size (bytes)
    const auto &bufferSize = hicr::json::getNumber<uint64_t>(dependency, "Buffer Size (Bytes)");

    // Create Global Memory Slots to exchange
    memorySlotsToExchange[tag] = allocateChannelBuffers(memoryManager, memorySpace, tag, bufferSize, bufferCapacity, coordinationBufferSizesSlots, coordinationBufferPayloadsSlots);
  }
}

void createChannels(HiCR::MemoryManager                                                                         &memoryManager,
                   HiCR::CommunicationManager                                                                  &communicationManager,
                   std::shared_ptr<HiCR::MemorySpace>                                                          &memorySpace,
                   std::map<HiCR::GlobalMemorySlot::tag_t, nlohmann::json>                                     &consumersData,
                   std::map<HiCR::GlobalMemorySlot::tag_t, nlohmann::json>                                     &producersData,
                   HiCR::GlobalMemorySlot::tag_t                                                                channelsIds,
                   std::map<HiCR::GlobalMemorySlot::tag_t, std::string>                                        &channelTagNameMap,
                   std::unordered_map<std::string, std::unique_ptr<hLLM::channel::channelConsumerInterface_t>> &consumers,
                   std::unordered_map<std::string, std::unique_ptr<hLLM::channel::channelProducerInterface_t>> &producers)
{
  std::map<HiCR::GlobalMemorySlot::tag_t, std::vector<HiCR::CommunicationManager::globalKeyMemorySlotPair_t>> memorySlotsToExchange;
  std::map<HiCR::GlobalMemorySlot::tag_t, std::shared_ptr<HiCR::LocalMemorySlot>>                             coordinationBufferSizesSlots;
  std::map<HiCR::GlobalMemorySlot::tag_t, std::shared_ptr<HiCR::LocalMemorySlot>>                             coordinationBufferPayloadsSlots;

  // Exchange unbuffered memory slots
  for (const auto &[tag, dependency] : consumersData)
  {
    // Getting buffer capacity (max token count)
    const auto &bufferCapacity = hicr::json::getNumber<uint64_t>(dependency, "Buffer Capacity (Tokens)");

    // Getting buffer size (bytes)
    const auto &bufferSize = hicr::json::getNumber<uint64_t>(dependency, "Buffer Size (Bytes)");

    // Create Global Memory Slots to exchange
    memorySlotsToExchange[tag] = allocateChannelBuffers(memoryManager, memorySpace, tag, bufferSize, bufferCapacity, coordinationBufferSizesSlots, coordinationBufferPayloadsSlots);
  }

  // Exchange the Global Memory Slots
  for (HiCR::GlobalMemorySlot::tag_t tag = 0; tag < channelsIds; ++tag)
  {
    if (memorySlotsToExchange.contains(tag))
      communicationManager.exchangeGlobalMemorySlots(tag, memorySlotsToExchange[tag]);
    else
      communicationManager.exchangeGlobalMemorySlots(tag, {});
  }

  // Fence
  for (HiCR::GlobalMemorySlot::tag_t tag = 0; tag < channelsIds; ++tag) communicationManager.fence(tag);

  for (const auto &[tag, dependency] : consumersData)
  {
    // Getting buffer capacity (max token count)
    const auto &bufferCapacity = hicr::json::getNumber<uint64_t>(dependency, "Buffer Capacity (Tokens)");

    // Getting buffer size (bytes)
    const auto &bufferSize = hicr::json::getNumber<uint64_t>(dependency, "Buffer Size (Bytes)");
    consumers.emplace(channelTagNameMap[tag],
                      createConsumer(communicationManager, tag, bufferSize, bufferCapacity, coordinationBufferSizesSlots[tag], coordinationBufferPayloadsSlots[tag]));
  }

  for (const auto &[tag, dependency] : producersData)
  {
    // Getting buffer capacity (max token count)
    const auto &bufferCapacity = hicr::json::getNumber<uint64_t>(dependency, "Buffer Capacity (Tokens)");

    // Getting buffer size (bytes)
    const auto &bufferSize = hicr::json::getNumber<uint64_t>(dependency, "Buffer Size (Bytes)");
    producers.emplace(channelTagNameMap[tag], createProducer(communicationManager, memoryManager, memorySpace, tag, bufferSize, bufferCapacity));
  }
}

void Engine::createDependencies()
{
  // Tags for channels
  HiCR::GlobalMemorySlot::tag_t bufferedChannelId   = 0;
  HiCR::GlobalMemorySlot::tag_t unbufferedChannelId = 0;

  // Get the application dependencies
  const auto &dependencies = _config["Dependencies"];

  // Extract dependency informations
  std::map<HiCR::GlobalMemorySlot::tag_t, nlohmann::json> bufferedConsumers;
  std::map<HiCR::GlobalMemorySlot::tag_t, nlohmann::json> bufferedProducers;
  std::map<HiCR::GlobalMemorySlot::tag_t, nlohmann::json> unbufferedConsumers;
  std::map<HiCR::GlobalMemorySlot::tag_t, nlohmann::json> unbufferedProducers;
  std::map<HiCR::GlobalMemorySlot::tag_t, std::string>    bufferedChannelTagNameMap;
  std::map<HiCR::GlobalMemorySlot::tag_t, std::string>    unbufferedChannelTagNameMap;

  for (const auto &[name, dependency] : dependencies.items())
  {
    // Getting channel's producer
    const auto &producerPartitionId = hicr::json::getNumber<partitionId_t>(dependency, "Producer");

    // Getting channel's consumer
    const auto &consumerPartitionId = hicr::json::getNumber<partitionId_t>(dependency, "Consumer");

    // Store the producer
    if (_partitionId == producerPartitionId)
    {
      if (hicr::json::getString(dependency, "Type") == "Buffered")
      {
        // Store the producer
        bufferedProducers[bufferedChannelId] = dependency;

        // Store tag name mapping
        bufferedChannelTagNameMap[bufferedChannelId] = name;
      }
      else
      {
        // Store the producer
        unbufferedProducers[unbufferedChannelId] = dependency;
      }
    }

    if (_partitionId == consumerPartitionId)
    {
      if (hicr::json::getString(dependency, "Type") == "Buffered")
      {
        // Store the consumer
        bufferedConsumers[bufferedChannelId] = dependency;

        // Store the tag name mapping
        bufferedChannelTagNameMap[bufferedChannelId] = name;
      }
      else
      {
        // Store the consumer
        unbufferedConsumers[unbufferedChannelId] = dependency;

        // Store tag name mapping
        unbufferedChannelTagNameMap[unbufferedChannelId] = name;

        // Increase unbuffered channel id locally. The unbuffered are instance local
        unbufferedChannelId++;
      }
    }

    // Increase buffered channel id for everyone. All the instances should participate
    bufferedChannelId++;
  }

  createChannels(*_unbufferedMemoryManager,
                *_unbufferedCommunicationManager,
                _unbufferedMemorySpace,
                unbufferedConsumers,
                unbufferedProducers,
                unbufferedChannelId,
                unbufferedChannelTagNameMap,
                _consumers,
                _producers);
  createChannels(*_bufferedMemoryManager,
                *_bufferedCommunicationManager,
                _bufferedMemorySpace,
                bufferedConsumers,
                bufferedProducers,
                bufferedChannelId,
                bufferedChannelTagNameMap,
                _consumers,
                _producers);
}
} // namespace hLLM
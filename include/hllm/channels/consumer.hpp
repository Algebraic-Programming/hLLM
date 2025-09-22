#pragma once

#include <hicr/core/definitions.hpp>
#include <hicr/core/memoryManager.hpp>
#include <hicr/frontends/channel/variableSize/mpsc/locking/consumer.hpp>

#include "definitions.hpp"

namespace hLLM::channel
{

using channelConsumerInterface_t = HiCR::channel::variableSize::MPSC::locking::Consumer;

/**
 * Channel consumer that allows hLLM instances to receive variable-sized tokens to each other.
 */
class Consumer final
{
  public:

  Consumer() = delete;

  /**
   * Constructor for the Channel object. It requires passing all the elements it needs to execute at construction time.
   * 
   * @param[in] channelName The name of the channel
   * @param[in] memoryManager The memory manager to use to reserve buffer memory
   * @param[in] memorySpace The memory space for registering received token
   * @param[in] dependencyType The type of dependency the channel serves (i.e., buffered or unbuffered) 
   * @param[in] consumerInterface The interface for the consumer side of the channel
   */
  Consumer(const std::string                           channelName,
           HiCR::MemoryManager *const                  memoryManager,
           const std::shared_ptr<HiCR::MemorySpace>    memorySpace,
           const dependencyType                        dependencyType,
           std::unique_ptr<channelConsumerInterface_t> consumerInterface)
    : _channelName(channelName),
      _memoryManager(memoryManager),
      _memorySpace(memorySpace),
      _dependencyType(dependencyType),
      _consumerInterface(std::move(consumerInterface))
  {
    if (_consumerInterface == nullptr) { HICR_THROW_RUNTIME("Invalid consumer interface: nullptr"); }
  }

  ~Consumer() = default;

  /**
   * Tries to retrieve the earliest received message in the channel and returns immediately.
   * 
   * @return A local memory slot object, nullptr if the consumer's buffer is empty
   * 
   * @note This function can only be used if the caller instance is the consumer in this channel
   */
  [[nodiscard]] __INLINE__ const std::shared_ptr<HiCR::LocalMemorySlot> peek()
  {
    // If the buffer is full, returning false
    if (_consumerInterface->isEmpty()) return nullptr;

    // Pushing buffer
    auto result = _consumerInterface->peek();

    // Getting absolute pointer to the token
    size_t tokenPos    = result[0];
    size_t tokenSize   = result[1];
    auto   tokenBuffer = (uint8_t *)_consumerInterface->getPayloadBufferMemorySlot()->getSourceLocalMemorySlot()->getPointer();
    void  *tokenPtr    = &tokenBuffer[tokenPos];

    auto tokenMemorySlot = _memoryManager->registerLocalMemorySlot(_memorySpace, tokenPtr, tokenSize);

    // Returning a full message if dependency type is buffered, as we succeeded
    if (_dependencyType == dependencyType::buffered) { return tokenMemorySlot; }

    // If unbuffered, read the pointer, the size
    unbufferedData_t *unbufferedData = static_cast<unbufferedData_t *>(tokenMemorySlot->getPointer());

    // Register and return the memory slot
    return _memoryManager->registerLocalMemorySlot(_memorySpace, unbufferedData->ptr, unbufferedData->size);
  }

  /**
   * Tries to erase (pop) the earliest message in the channel and returns immediately.
   * 
   * @return true, if the message was erased; false, if the buffer was empty.
   */
  __INLINE__ bool pop()
  {
    // If the buffer is full, returning false
    if (_consumerInterface->isEmpty()) return false;

    // Popping buffer
    _consumerInterface->pop();

    // Returning true, as we succeeded
    return true;
  }

  /**
   * Checks whether the channel is empty, or a message has arrived yet
   * 
   * @return true, if the channel is empty; false, otherwise.
   */
  __INLINE__ bool isEmpty() { return _consumerInterface->isEmpty(); }

  /**
   * Checks whether the channel is full
   * 
   * @return true, if the channel is full; false, otherwise.
   */
  __INLINE__ bool isFull() { return _consumerInterface->isFull(); }

  /**
   * Retrieves the number of tokens present in the buffer
   * 
   * @return the number of tokens in the buffer
   */
  __INLINE__ size_t getDepth() { return _consumerInterface->getDepth(); }

  private:

  /// Unique identifier of the channel
  const std::string _channelName;

  /// Memory manager to use for memory slot registration
  HiCR::MemoryManager *const _memoryManager;

  /// Memory space to use for memory slot registration
  const std::shared_ptr<HiCR::MemorySpace> _memorySpace;

  /// Dependency type
  const dependencyType _dependencyType;

  /// HiCR Consumer interface
  const std::unique_ptr<channelConsumerInterface_t> _consumerInterface;
}; // class Channel

} // namespace hLLM::channel
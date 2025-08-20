#pragma once

#include <hicr/core/definitions.hpp>
#include <hicr/core/memoryManager.hpp>
#include <hicr/frontends/channel/variableSize/mpsc/locking/consumer.hpp>
#include <hicr/frontends/channel/variableSize/mpsc/locking/producer.hpp>

namespace hLLM
{

/**
 * Represents an input or output channel that allows hLLM instances to send variable-sized tokens to each other.
 * This channel may contain more than one producer, but always a single consumer.
 */
class Channel final
{
  public:

  /// Represents a message exchanged between two instances through a channel
  struct token_t
  {
    /// Whether the exchange succeeded
    bool success;

    /// Pointer to the buffer, if the exchange succeeded
    void *buffer;

    /// Size of the token received
    size_t size;
  };

  Channel() = delete;

  /**
   * Constructor for the Channel object. It requires passing all the elements it needs to execute at construction time.
   * 
   * @param[in] channelName The name of the channel
   * @param[in] memoryManager The memory manager to use to reserve buffer memory (consumer)
   * @param[in] memorySpace The memory space to use to reserve buffer memory (consumer)
   * @param[in] consumerInterface The interface for the consumer side of the channel. It is nullptr, if this instance is not a consumer of this channel
   * @param[in] producerInterface The interface for the producer side of the channel. It is nullptr, if this instance is not a producer of this channel
   * 
   * @note If this channel is accessible by an instance, it means that instance is either a consumer xor a producer.
   */
  Channel(const std::string                                                     channelName,
          HiCR::MemoryManager *const                                            memoryManager,
          const std::shared_ptr<HiCR::MemorySpace>                              memorySpace,
          std::shared_ptr<HiCR::channel::variableSize::MPSC::locking::Consumer> consumerInterface,
          std::shared_ptr<HiCR::channel::variableSize::MPSC::locking::Producer> producerInterface)
    : _memoryManager(memoryManager),
      _memorySpace(memorySpace),
      _consumerInterface(consumerInterface),
      _producerInterface(producerInterface)
  {}
  ~Channel() = default;

  /**
   * Tries to push a message through the channel and returns immediately.
   * 
   * @param buffer The data buffer that contains the message to send
   * @param size The size of the message to send
   * @return true, if the message was sent successfully; false, otherwise (e.g., the consumer buffer is already full)
   * 
   * @note This function can only be used if the caller instance is a producer in this channel
   */
  __INLINE__ bool push(const void *buffer, const size_t size)
  {
    if (_producerInterface == nullptr) HICR_THROW_LOGIC("Attempting to push a message, but this instance has no producer role in channel '%s'\n", _channelName.c_str());

    // If the buffer is full, returning false
    if (_producerInterface->isFull()) return false;

    // Using memory manager to register the buffer
    auto bufferMemorySlot = _memoryManager->registerLocalMemorySlot(_memorySpace, (void *)buffer, size);

    // Pushing buffer
    _producerInterface->push(bufferMemorySlot, 1);

    // Deregistering memory slot
    _memoryManager->deregisterLocalMemorySlot(bufferMemorySlot);

    // Returning true, as we succeeded
    return true;
  }

  /**
   * Tries to retrieve the earliest received message in the channel and returns immediately.
   * 
   * @return A token object, containing whether the operation succeeded (false if the consumer's buffer is empty), a pointer to the location of the message, and the size of the message
   * 
   * @note This function can only be used if the caller instance is the consumer in this channel
   */
  [[nodiscard]] __INLINE__ const token_t peek()
  {
    if (_consumerInterface == nullptr) HICR_THROW_LOGIC("Attempting to peek a message, but this instance has no consumer role in channel '%s'\n", _channelName.c_str());

    // If the buffer is full, returning false
    if (_consumerInterface->isEmpty()) return token_t{.success = false, .buffer = nullptr, .size = 0};

    // Pushing buffer
    auto result = _consumerInterface->peek();

    // Getting absolute pointer to the token
    size_t tokenPos    = result[0];
    size_t tokenSize   = result[1];
    auto   tokenBuffer = (uint8_t *)_consumerInterface->getPayloadBufferMemorySlot()->getSourceLocalMemorySlot()->getPointer();
    void  *tokenPtr    = &tokenBuffer[tokenPos];

    // Returning a full message, as we succeeded
    return token_t{.success = true, .buffer = tokenPtr, .size = tokenSize};
  }

  /**
   * Tries to erase (pop) the earliest message in the channel and returns immediately.
   * 
   * @return true, if the message was erased; false, if the buffer was empty.
   * 
   * @note This function can only be used if the caller instance is the consumer in this channel
   */
  __INLINE__ bool pop()
  {
    if (_consumerInterface == nullptr) HICR_THROW_LOGIC("Attempting to pop a message, but this instance has no consumer role in channel '%s'\n", _channelName.c_str());

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
  __INLINE__ bool isEmpty()
  {
    // Returning whether the consumer buffer of the channel is empty
    if (_consumerInterface != nullptr) return _consumerInterface->isEmpty();
    if (_producerInterface != nullptr)
    {
      _producerInterface->updateDepth();
      return _producerInterface->isEmpty();
    }
    HICR_THROW_FATAL("This channel '%s' contains no interfaces\n", _channelName.c_str());
  }

  /**
   * Checks whether the channel is full
   * 
   * @return true, if the channel is full; false, otherwise.
   */
  __INLINE__ bool isFull()
  {
    // Returning whether the consumer buffer of the channel is empty
    if (_consumerInterface != nullptr) return _consumerInterface->isFull();
    if (_producerInterface != nullptr)
    {
      _producerInterface->updateDepth();
      return _producerInterface->isFull();
    }
    HICR_THROW_FATAL("This channel '%s' contains no interfaces\n", _channelName.c_str());
  }

  /**
   * Retrieves the number of tokens present in the buffer
   * 
   * @return the number of tokens in the buffer
   */
  __INLINE__ size_t getDepth()
  {
    // Returning whether the consumer buffer of the channel is empty
    if (_consumerInterface != nullptr) return _consumerInterface->getDepth();
    if (_producerInterface != nullptr)
    {
      _producerInterface->updateDepth();
      return _producerInterface->getDepth();
    }
    HICR_THROW_FATAL("This channel '%s' contains no interfaces\n", _channelName.c_str());
  }

  private:

  /// Unique identifier of the channel
  const std::string _channelName;

  /// Memory manager to use for memory slot registration
  HiCR::MemoryManager *const _memoryManager;

  /// Memory space to use for memory slot registration
  const std::shared_ptr<HiCR::MemorySpace> _memorySpace;

  /// HiCR Consumer interface
  const std::shared_ptr<HiCR::channel::variableSize::MPSC::locking::Consumer> _consumerInterface;

  /// HiCR Producer interface
  const std::shared_ptr<HiCR::channel::variableSize::MPSC::locking::Producer> _producerInterface;

}; // class Channel

} // namespace hLLM
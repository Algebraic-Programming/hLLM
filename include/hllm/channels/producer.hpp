#pragma once

#include <hicr/core/definitions.hpp>
#include <hicr/core/memoryManager.hpp>
#include <hicr/frontends/channel/variableSize/mpsc/locking/producer.hpp>

namespace hLLM::channel
{

using channelProducerInterface_t = HiCR::channel::variableSize::MPSC::locking::Producer;

/**
 * Channel producer that allows hLLM instances to receive variable-sized tokens to each other.
 */
class Producer final
{
  public:

  Producer() = delete;

  /**
   * Constructor for the Producer object. It requires passing all the elements it needs to execute at construction time.
   * 
   * @param[in] channelName The name of the channel
   * @param[in] producerInterface The interface for the producer side of the channel
   */
  Producer(const std::string &channelName, std::unique_ptr<channelProducerInterface_t> producerInterface)
    : _channelName(channelName),
      _producerInterface(std::move(producerInterface))
  {
    if (_producerInterface == nullptr) { HICR_THROW_RUNTIME("Invalid producer interface: nullptr"); }
  }

  ~Producer() = default;

  /**
   * Tries to push a message through the channel and returns immediately.
   * 
   * @param buffer The data buffer that contains the message to send
   * @param size The size of the message to send
   * @return true, if the message was sent successfully; false, otherwise (e.g., the consumer buffer is already full)
   */
  __INLINE__ bool push(std::shared_ptr<HiCR::LocalMemorySlot> memorySlot)
  {
    // If the buffer is full, returning false
    if (_producerInterface->isFull()) return false;

    // Pushing buffer
    return _producerInterface->push(memorySlot, 1);
  }

  /**
   * Checks whether the channel is empty, or a message has arrived yet
   * 
   * @return true, if the channel is empty; false, otherwise.
   */
  __INLINE__ bool isEmpty()
  {
    _producerInterface->updateDepth();
    return _producerInterface->isEmpty();
  }

  /**
   * Checks whether the channel is full
   * 
   * @return true, if the channel is full; false, otherwise.
   */
  __INLINE__ bool isFull()
  {
    _producerInterface->updateDepth();
    return _producerInterface->isFull();
  }

  /**
   * Retrieves the number of tokens present in the buffer
   * 
   * @return the number of tokens in the buffer
   */
  __INLINE__ size_t getDepth()
  {
    _producerInterface->updateDepth();
    return _producerInterface->getDepth();
  }

  private:

  /// Unique identifier of the channel
  const std::string _channelName;

  /// HiCR Producer interface
  const std::unique_ptr<channelProducerInterface_t> _producerInterface;
}; // class Channel

} // namespace hLLM::channel
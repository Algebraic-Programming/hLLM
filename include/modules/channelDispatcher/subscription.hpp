#pragma once

#include <functional>
#include <memory>

#include <hicr/core/definitions.hpp>
#include <hicr/core/exceptions.hpp>

#include <system/channels/input.hpp>
#include <system/channels/message.hpp>

using namespace hLLM::system;
namespace hLLM::modules::channelDispatcher
{

typedef std::function<void(const std::shared_ptr<channels::Input>, const channels::Message &)> messageHandler_t;

class Subscription final
{
  public:

  Subscription() = delete;

  Subscription(const channels::Message::messageType_t type, const std::shared_ptr<channels::Input> edge, const messageHandler_t handler)
    : _type(type),
      _edge(edge),
      _handler(handler)
  {
    if (_edge == nullptr) HICR_THROW_LOGIC("Trying to create a channel subscription with a null input edge.");
    if (_handler == nullptr) HICR_THROW_LOGIC("Trying to create a channel subscription with a null handler.");
  }

  ~Subscription() = default;

  [[nodiscard]] __INLINE__ channels::Message::messageType_t getType() const { return _type; }
  [[nodiscard]] __INLINE__ const std::shared_ptr<channels::Input> &getEdge() const { return _edge; }
  [[nodiscard]] __INLINE__ const messageHandler_t                 &getHandler() const { return _handler; }

  private:

  const channels::Message::messageType_t _type;
  const std::shared_ptr<channels::Input> _edge;
  const messageHandler_t                 _handler;
};
} // namespace hLLM::modules::channelDispatcher
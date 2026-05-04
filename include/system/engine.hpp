#pragma once

#include <hicr/core/exceptions.hpp>
#include <hicr/core/definitions.hpp>
#include <hicr/core/instanceManager.hpp>
#include <hicr/frontends/RPCEngine/RPCEngine.hpp>

#define __HLLM_SYSTEM_START_RPC_NAME "[hLLM/system] Start Instances RPC"
#define __HLLM_SYSTEM_STOP_RPC_NAME "[hLLM/system] Stop Instances RPC"
#define __HLLM_DEFAULT_EXCHANGE_TAG 0x0000A000

namespace hLLM::system
{

/**
 * Engin that bootstraps the instances
 */
class Engine final
{
  public:

  /**
   * Constructor
   * 
   * @param[in] instanceManager The instance manager to use for creating / relinquishing  replicas
   * @param[in] rpcEngine Pointer to the HiCR RPC Engine to use for RPC
   * @param[in] exchangeTag HiCR Tag to use for channel exchanges.
   */
  Engine(std::shared_ptr<HiCR::InstanceManager>     instanceManager,
         std::shared_ptr<HiCR::ComputeManager>      computeManager,
         std::shared_ptr<HiCR::frontend::RPCEngine> rpcEngine,
         const HiCR::GlobalMemorySlot::tag_t        exchangeTag = __HLLM_DEFAULT_EXCHANGE_TAG)
    : _instanceManager(instanceManager),
      _computeManager(computeManager),
      _rpcEngine(rpcEngine),
      _exchangeTag(exchangeTag)
  {
    // Register RPC for sending list of instances. This is used by non-root instances to obtain the list of instances in the system
    _rpcEngine->addRPCTarget(__HLLM_SYSTEM_START_RPC_NAME, _computeManager->createExecutionUnit([this](void *) { start(); }));
    _rpcEngine->addRPCTarget(__HLLM_SYSTEM_STOP_RPC_NAME, _computeManager->createExecutionUnit([this](void *) { stop(); }));
  }

  ~Engine() = default;

  /**
   * Create the missing instances
   */
  __INLINE__ void initialize(const size_t desiredNumInstances)
  {
    auto isRoot = _instanceManager->getCurrentInstance()->isRootInstance();

    if (isRoot)
    {
      // Check how many instances are detected and create missing ones.
      auto instances = _instanceManager->getInstances();
      if (instances.size() != desiredNumInstances) { createMissingInstances(instances, desiredNumInstances); }
    }
  }

  __INLINE__ void run()
  {
    auto isRoot = _instanceManager->getCurrentInstance()->isRootInstance();

    if (isRoot)
    {
      for (const auto &instance : _instanceManager->getInstances())
      {
        if (instance->getId() == _instanceManager->getCurrentInstance()->getId()) continue;
        _rpcEngine->requestRPC(instance->getId(), __HLLM_SYSTEM_START_RPC_NAME);
      }
    }
    else { _rpcEngine->listen(); }
  }
  __INLINE__ void finalize()
  {
    auto isRoot = _instanceManager->getCurrentInstance()->isRootInstance();

    if (isRoot)
    {
      for (const auto &instance : _instanceManager->getInstances())
      {
        if (instance->getId() == _instanceManager->getCurrentInstance()->getId()) continue;
        _rpcEngine->requestRPC(instance->getId(), __HLLM_SYSTEM_STOP_RPC_NAME);
      }
    }
    else { _rpcEngine->listen(); }
  }

  private:

  /**
   * Creates the missing instances. Aborts if the provided instance manager cannot create new instances.
   */
  __INLINE__ void createMissingInstances(HiCR::InstanceManager::instanceList_t &instances, const size_t desiredInstances)
  {
    // Create the missing instances. Abort if the provided instance manager cannot create new instances.
    for (size_t i = instances.size(); i < desiredInstances; i++)
    {
      try
      {
        auto newInstance = _instanceManager->createInstance();
      }
      catch (const HiCR::RuntimeException &e)
      {
        fprintf(stderr, "The provided instance manager can not create new instances\n");
        _instanceManager->abort(-1);
      }
    }
  }

  void start() { printf("Instance %lu starting...\n", _instanceManager->getCurrentInstance()->getId()); }
  void stop() { printf("Instance %lu stopping...\n", _instanceManager->getCurrentInstance()->getId()); }

  // The instance manager to use for creating / relinquishing  replicas
  std::shared_ptr<HiCR::InstanceManager> _instanceManager;

  // Pointer to the HiCR Compute Manager
  std::shared_ptr<HiCR::ComputeManager> _computeManager;

  // Pointer to the taskr Runtime
  std::shared_ptr<taskr::Runtime> _taskr;

  // Pointer to the HiCR RPC Engine
  std::shared_ptr<HiCR::frontend::RPCEngine> _rpcEngine;

  // HiCR Tag to use for channel exchanges
  const HiCR::GlobalMemorySlot::tag_t _exchangeTag;
}; // class Engine

} // namespace hLLM::system

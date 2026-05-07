#pragma once

#include <atomic>
#include <memory>

#include <hicr/core/definitions.hpp>
#include <hicr/core/exceptions.hpp>
#include <hicr/core/instanceManager.hpp>
#include <hicr/frontends/RPCEngine/RPCEngine.hpp>
#include <taskr/taskr.hpp>

namespace hLLM::system
{

#define __HLLM_SYSTEM_START_RPC_NAME "[hLLM/system] Start Instances RPC"
#define __HLLM_SYSTEM_STOP_RPC_NAME "[hLLM/system] Stop Instances RPC"

class Engine final
{
  public:

  Engine(std::shared_ptr<HiCR::InstanceManager>     instanceManager,
         std::shared_ptr<HiCR::ComputeManager>      computeManager,
         std::shared_ptr<HiCR::frontend::RPCEngine> rpcEngine,
         const HiCR::Instance::instanceId_t         deployerInstanceId,
         std::shared_ptr<taskr::Runtime>            taskr)
    : _instanceManager(instanceManager),
      _computeManager(computeManager),
      _taskr(taskr),
      _rpcEngine(rpcEngine),
      _instanceId(instanceManager->getCurrentInstance()->getId()),
      _deployerInstanceId(deployerInstanceId)
  {
    _rpcEngine->addRPCTarget(__HLLM_SYSTEM_START_RPC_NAME, _computeManager->createExecutionUnit([this](void *) { start(); }));
    _rpcEngine->addRPCTarget(__HLLM_SYSTEM_STOP_RPC_NAME, _computeManager->createExecutionUnit([this](void *) { stop(); }));
  }

  ~Engine() = default;

  __INLINE__ void initialize()
  {
    _isRunning.store(false);
    printf("[Instance %lu] Initializing system\n", _instanceId);
  }

  __INLINE__ void run()
  {
    if (_instanceId == _deployerInstanceId)
    {
      printf("[Instance %lu] Broadcasting start\n", _instanceId);
      for (const auto &instance : _instanceManager->getInstances())
      {
        if (instance->getId() == _instanceId) continue;
        printf("[Instance %lu] Sending start RPC to instance %lu\n", _instanceId, instance->getId());
        _rpcEngine->requestRPC(instance->getId(), __HLLM_SYSTEM_START_RPC_NAME);
      }
      // Start myself
      start();
      return;
    }
    // Worker: wait for start RPC
    _rpcEngine->listen();
  }

  __INLINE__ void await()
  {
    while (_isRunning.load() == true)
      if (_rpcEngine->tryListen()) _rpcEngine->parseAndExecuteRPC();
  }

  __INLINE__ void terminate()
  {
    if (_instanceId == _deployerInstanceId)
    {
      printf("[Instance %lu] Broadcasting stop\n", _instanceId);
      for (const auto &instance : _instanceManager->getInstances())
      {
        if (instance->getId() == _instanceId) continue;
        printf("[Instance %lu] Sending stop RPC to instance %lu\n", _instanceId, instance->getId());
        _rpcEngine->requestRPC(instance->getId(), __HLLM_SYSTEM_STOP_RPC_NAME);
      }
      // Stop myself
      stop();
      return;
    }
    printf("[Instance %lu] Terminating system\n", _instanceId);
  }

  __INLINE__ void createInstance() { _instanceManager->createInstance(); }

  private:

  __INLINE__ void start()
  {
    if (_isRunning.load() == true)
    {
      printf("[Instance %lu] System already running\n", _instanceId);
      return;
    }
    _isRunning.store(true);
    printf("[Instance %lu] Starting system\n", _instanceId);
  }

  __INLINE__ void stop()
  {
    if (_isRunning.load() == false)
    {
      printf("[Instance %lu] System already stopped\n", _instanceId);
      return;
    }
    _isRunning.store(false);
    printf("[Instance %lu] Stopping system\n", _instanceId);
  }

  std::shared_ptr<HiCR::InstanceManager>     _instanceManager;
  std::shared_ptr<HiCR::ComputeManager>      _computeManager;
  std::shared_ptr<taskr::Runtime>            _taskr;
  std::shared_ptr<HiCR::frontend::RPCEngine> _rpcEngine;

  const HiCR::Instance::instanceId_t _instanceId;
  const HiCR::Instance::instanceId_t _deployerInstanceId;
  std::atomic<bool>                  _isRunning = false;
};
} // namespace hLLM::system
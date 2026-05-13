#pragma once

#include <atomic>
#include <map>
#include <memory>
#include <string>

#include <hicr/core/definitions.hpp>
#include <hicr/core/exceptions.hpp>
#include <hicr/core/instanceManager.hpp>
#include <hicr/frontends/RPCEngine/RPCEngine.hpp>

#include <modules/module.hpp>

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
         const HiCR::Instance::instanceId_t         deployerInstanceId)
    : _instanceManager(instanceManager),
      _computeManager(computeManager),
      _rpcEngine(rpcEngine),
      _instanceId(instanceManager->getCurrentInstance()->getId()),
      _deployerInstanceId(deployerInstanceId)
  {
    _rpcEngine->addRPCTarget(__HLLM_SYSTEM_START_RPC_NAME, _computeManager->createExecutionUnit([this](void *) { start(); }));
    _rpcEngine->addRPCTarget(__HLLM_SYSTEM_STOP_RPC_NAME, _computeManager->createExecutionUnit([this](void *) { stop(); }));
  }

  ~Engine() = default;

  __INLINE__ void addModule(const std::string &name, std::shared_ptr<modules::Module> module)
  {
    if (module == nullptr) HICR_THROW_LOGIC("Trying to add a null module.");
    if (_modules.contains(name)) HICR_THROW_LOGIC("Trying to add a module with a name that already exists in the system.");
    _modules[name] = module;
  }

  __INLINE__ void initialize()
  {
    _isRunning.store(false);
    printf("[Instance %lu] Initializing system\n", _instanceId);

    for (const auto &[name, module] : _modules)
    {
      printf("[Instance %lu] Initializing module %s\n", _instanceId, name.c_str());
      module->initialize();
    }
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
    {
      if (_rpcEngine->tryListen()) { _rpcEngine->parseAndExecuteRPC(); }
    }

    for (const auto &[name, module] : _modules)
    {
      printf("[Instance %lu] Terminating module %s\n", _instanceId, name.c_str());
      module->terminate();
    }

    for (const auto &[name, module] : _modules)
    {
      printf("[Instance %lu] Awaiting module %s\n", _instanceId, name.c_str());
      module->await();
    }

    for (const auto &[name, module] : _modules)
    {
      printf("[Instance %lu] Finalizing module %s\n", _instanceId, name.c_str());
      module->finalize();
    }
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
    // Workers are stopped by stop RPC
    printf("[Instance %lu] Terminate called on worker; waiting for stop RPC in await()\n", _instanceId);
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

    for (const auto &[name, module] : _modules)
    {
      printf("[Instance %lu] Running module %s\n", _instanceId, name.c_str());
      module->run();
    }
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
  std::shared_ptr<HiCR::frontend::RPCEngine> _rpcEngine;

  const HiCR::Instance::instanceId_t _instanceId;
  const HiCR::Instance::instanceId_t _deployerInstanceId;

  std::atomic<bool> _isRunning = false;

  std::map<std::string, std::shared_ptr<modules::Module>> _modules;
};
} // namespace hLLM::system
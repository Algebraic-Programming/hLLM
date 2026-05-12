#pragma once

#include <map>
#include <memory>
#include <set>
#include <vector>

#include <hicr/core/communicationManager.hpp>
#include <hicr/core/definitions.hpp>
#include <hicr/core/exceptions.hpp>
#include <hicr/core/globalMemorySlot.hpp>

#include <modules/module.hpp>
#include <system/channels/input.hpp>
#include <system/channels/output.hpp>

namespace hLLM::modules::channelBootstrap
{

#define __HLLM_DEFAULT_EXCHANGE_TAG 0x0000A000

class Module final : public modules::Module
{
  public:

  Module(const std::vector<std::shared_ptr<hLLM::system::channels::Input>>  &inputs,
         const std::vector<std::shared_ptr<hLLM::system::channels::Output>> &outputs,
         const std::vector<HiCR::CommunicationManager *>                    &communicationManagersInOrder,
         const HiCR::GlobalMemorySlot::tag_t                                 exchangeTag = __HLLM_DEFAULT_EXCHANGE_TAG)
    : modules::Module(),
      _inputs(inputs),
      _outputs(outputs),
      _communicationManagersInOrder(communicationManagersInOrder),
      _exchangeTag(exchangeTag)
  {
    if (_communicationManagersInOrder.empty()) HICR_THROW_LOGIC("Channel bootstrap requires at least one communication manager.");
    for (const auto manager : _communicationManagersInOrder)
      if (manager == nullptr) HICR_THROW_LOGIC("Channel bootstrap received a null communication manager.");
  }

  ~Module() override = default;

  __INLINE__ void addInput(const std::shared_ptr<hLLM::system::channels::Input> input)
  {
    if (input == nullptr) HICR_THROW_LOGIC("Trying to add null input channel to channel bootstrap.");
    _inputs.push_back(input);
  }

  __INLINE__ void addOutput(const std::shared_ptr<hLLM::system::channels::Output> output)
  {
    if (output == nullptr) HICR_THROW_LOGIC("Trying to add null output channel to channel bootstrap.");
    _outputs.push_back(output);
  }

  void initialize() override
  {
    // Validate registered channels
    for (const auto &input : _inputs)
      if (input == nullptr) HICR_THROW_LOGIC("Channel bootstrap contains a null input channel.");

    for (const auto &output : _outputs)
      if (output == nullptr) HICR_THROW_LOGIC("Channel bootstrap contains a null output channel.");

    // Collect memory slots to exchange
    std::vector<hLLM::system::channels::memorySlotExchangeInfo_t> memorySlotsToExchange;
    for (const auto &input : _inputs) input->getMemorySlotsToExchange(memorySlotsToExchange);
    for (const auto &output : _outputs) output->getMemorySlotsToExchange(memorySlotsToExchange);

    // Allowed manager set (from provided deterministic order)
    std::set<HiCR::CommunicationManager *> managerSet;
    for (const auto manager : _communicationManagersInOrder) managerSet.insert(manager);

    // Group memory slots by communication manager
    std::map<HiCR::CommunicationManager *, std::vector<HiCR::CommunicationManager::globalKeyMemorySlotPair_t>> exchangeMap;
    for (const auto &entry : memorySlotsToExchange)
    {
      if (entry.communicationManager == nullptr) HICR_THROW_LOGIC("Channel bootstrap found null communication manager in memory slot exchange entry.");
      if (managerSet.contains(entry.communicationManager) == false)
        HICR_THROW_LOGIC("Memory slot exchange entry uses communication manager not present in bootstrap manager order.");

      exchangeMap[entry.communicationManager].push_back(HiCR::CommunicationManager::globalKeyMemorySlotPair_t(entry.globalKey, entry.memorySlot));
    }

    // Exchange in deterministic order
    for (const auto manager : _communicationManagersInOrder) manager->exchangeGlobalMemorySlots(_exchangeTag, exchangeMap[manager]);

    // Fence in deterministic order
    for (const auto manager : _communicationManagersInOrder) manager->fence(_exchangeTag);

    // Initialize local channels
    for (const auto &input : _inputs) input->initialize(_exchangeTag);
    for (const auto &output : _outputs) output->initialize(_exchangeTag);
  }

  void run() override {}
  void terminate() override {}
  void await() override {}
  void finalize() override
  {
    _inputs.clear();
    _outputs.clear();
  }

  protected:

  // Init-only module (no periodic work)
  void service() override {}

  private:

  std::vector<std::shared_ptr<hLLM::system::channels::Input>>  _inputs;
  std::vector<std::shared_ptr<hLLM::system::channels::Output>> _outputs;
  std::vector<HiCR::CommunicationManager *>                    _communicationManagersInOrder;
  const HiCR::GlobalMemorySlot::tag_t                          _exchangeTag;
};
} // namespace hLLM::modules::channelBootstrap
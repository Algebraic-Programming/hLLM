#pragma once

#include "base.hpp"

namespace hLLM::edge
{

class Input final : public Base
{
  public:

  Input(const configuration::Edge edgeConfig,
        const configuration::Edge::edgeIndex_t edgeIndex,
        const configuration::Replica::replicaIndex_t replicaIndex) :
         Base(edgeConfig, edgeIndex, replicaIndex)
  {
    
  }

  ~Input() = default;

  public:

  __INLINE__ HiCR::CommunicationManager::globalKeyToMemorySlotMap_t getMemorySlotsToExchange() const override
  {
    HiCR::CommunicationManager::globalKeyToMemorySlotMap_t memorySlotMap;

    return memorySlotMap;
  }

  private:


}; // class Input

} // namespace hLLM::edge
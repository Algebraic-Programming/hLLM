#pragma once

#include "base.hpp"

namespace hLLM::edge
{

class Output final : public Base
{
  public:

  Output(const configuration::Edge edgeConfig,
        const configuration::Edge::edgeIndex_t edgeIndex,
        const configuration::Replica::replicaIndex_t replicaIndex) :
         Base(edgeConfig, edgeIndex, replicaIndex)
  {
    
  }

  ~Output() = default;

  public:
  
  __INLINE__ HiCR::CommunicationManager::globalKeyToMemorySlotMap_t getMemorySlotsToExchange() const override
  {
    HiCR::CommunicationManager::globalKeyToMemorySlotMap_t memorySlotMap;

    return memorySlotMap;
  }

  private:


}; // class Output

} // namespace hLLM::edge
#pragma once

#include "../configuration/partition.hpp"
#include "../configuration/replica.hpp"
#include "../edge/input.hpp"
#include "../edge/output.hpp"

namespace hLLM::coordinator
{

/** 
 * This is the definition of a replica from the standpoint of the coordinator.
 * 
 * It holds all the necessary information to communicate and distribute work to the associated replica
 */
class Replica final
{
  public:

  Replica() = delete;
  Replica(
    const configuration::Partition::partitionIndex_t partitionIndex,
    const configuration::Replica::replicaIndex_t replicaIndex,
    const std::vector<hLLM::Partition::edgeInfo_t>& coordinatorInputs,
    const std::vector<hLLM::Partition::edgeInfo_t>& coordinatorOutputs,
    const std::shared_ptr<hLLM::configuration::Edge> controlEdgeConfig) :
    _partitionIndex(partitionIndex),
    _replicaIndex(replicaIndex)
    {
      // For every one of the coordinator inputs, we create an output edge that allows us to redirect such inputs to the replica
      for (const auto& edge : coordinatorInputs)
      {
        const auto edgeIdx = edge.index;
        const auto& edgeConfig = edge.config;
          
        // Create the output edges to pass this distribute the input to any of the replicas
        auto newOutput = std::make_shared<edge::Output>(*edgeConfig, edge::edgeType_t::coordinatorToReplica, edgeIdx, _partitionIndex, _partitionIndex, _replicaIndex);
        _dataOutputs.push_back(newOutput);
      }

      // For everyone of the coordinator outputs, we create an input edge that receives such data from the replica to be redirected to a peer coordinator
      for (const auto& edge : coordinatorOutputs)
      {
        const auto edgeIdx = edge.index;
        const auto& edgeConfig = edge.config;
          
        // Create the input edges to receive the replica 
        auto newInput = std::make_shared<edge::Input>(*edgeConfig, edge::edgeType_t::replicaToCoordinator, edgeIdx, _partitionIndex, _partitionIndex, _replicaIndex);
        _dataInputs.push_back(newInput);
      }

      // Creating control edges for exchanging control operations (e.g., heartbeat, etc)
      _controlInput = std::make_shared<edge::Input>(*controlEdgeConfig, edge::edgeType_t::replicaToCoordinator, edge::Base::controlEdgeIndex, _partitionIndex, _partitionIndex, _replicaIndex);
      _controlOutput = std::make_shared<edge::Output>(*controlEdgeConfig, edge::edgeType_t::coordinatorToReplica, edge::Base::controlEdgeIndex, _partitionIndex, _partitionIndex, _replicaIndex);
    }

  ~Replica() = default;

  __INLINE__ void addDataInputEdge(std::shared_ptr<edge::Input> edge) { _dataInputs.push_back(edge); }
  __INLINE__ void addDataOutputEdge(std::shared_ptr<edge::Output> edge) { _dataOutputs.push_back(edge); }
  __INLINE__ const auto& getDataInputs() const { return _dataInputs; }
  __INLINE__ const auto& getDataOutputs() const { return _dataOutputs; }
  __INLINE__ const auto& getControlInput() const { return _controlInput; }
  __INLINE__ const auto& getControlOutput() const { return _controlOutput; }

  __INLINE__ void getMemorySlotsToExchange(std::vector<hLLM::edge::memorySlotExchangeInfo_t>& memorySlots)
  {
    for (const auto& edge : _dataInputs)  edge->getMemorySlotsToExchange(memorySlots);
    for (const auto& edge : _dataOutputs) edge->getMemorySlotsToExchange(memorySlots);
    _controlInput->getMemorySlotsToExchange(memorySlots);
    _controlOutput->getMemorySlotsToExchange(memorySlots);
  }

  __INLINE__ void initializeEdges(const HiCR::GlobalMemorySlot::tag_t tag)
  {
    for (const auto& edge : _dataInputs)    edge->initialize(tag);
    for (const auto& edge : _dataOutputs)   edge->initialize(tag);
    _controlInput->initialize(tag);
    _controlOutput->initialize(tag);
  }

  private:

  const configuration::Partition::partitionIndex_t _partitionIndex; 
  const configuration::Replica::replicaIndex_t _replicaIndex;

  // Data Input / Output edges to/from this coordinator<->replica
  std::vector<std::shared_ptr<edge::Input>> _dataInputs;
  std::vector<std::shared_ptr<edge::Output>> _dataOutputs;

  // Control Input / Output edges to/from this coordinator<->replica
  std::shared_ptr<edge::Input> _controlInput;
  std::shared_ptr<edge::Output> _controlOutput;

}; // class Replica

} // namespace hLLM::coordinator
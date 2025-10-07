#pragma once

#include <memory>
#include <vector>
#include <hicr/core/exceptions.hpp>
#include <hicr/core/definitions.hpp>
#include <hicr/core/globalMemorySlot.hpp>
#include <taskr/taskr.hpp>
#include "edge/base.hpp"
#include "configuration/deployment.hpp"

namespace hLLM
{

class Partition
{
  public:

  struct edgeInfo_t
  {
    configuration::Edge::edgeIndex_t index;
    std::shared_ptr<hLLM::configuration::Edge> config;
    configuration::Partition::partitionIndex_t producerPartitionIndex;
    configuration::Partition::partitionIndex_t consumerPartitionIndex;
  };

  Partition() = delete;
  ~Partition() = default;

  Partition(
    const configuration::Deployment deployment,
    const configuration::Partition::partitionIndex_t partitionIdx,
    taskr::Runtime* const taskr
  ) :
    _deployment(deployment),
    _partitionIdx(partitionIdx),
    _taskr(taskr)
  {
    // Get my partition configuration
    const auto& partitionConfiguration = _deployment.getPartitions()[_partitionIdx];

    // Get my partition name
    const auto& partitionName = partitionConfiguration->getName();

    // Getting list of edges in the deployment
    const auto& edgeConfigs = _deployment.getEdges();

    // Iterating through edges by their index and creating them
    for (configuration::Edge::edgeIndex_t edgeIdx = 0; edgeIdx < edgeConfigs.size(); edgeIdx++)
    {
      // Getting edge object by index
      const auto edgeConfig = edgeConfigs[edgeIdx];

      // If I am a consumer in this edge
      if (edgeConfig->getConsumer() == partitionName)
      {
        // Looking for the index of the producer of the input
        const auto& producerPartitionName = edgeConfig->getProducer(); 
        configuration::Partition::partitionIndex_t producerPartitionIdx = 0;
        bool producerFound = false;
        for (const auto& partition : _deployment.getPartitions())
        {
          if (partition->getName() == producerPartitionName)
          {
            producerFound = true;
            break;
          } 
          producerPartitionIdx++;
        }

        // Sanity check
        if (producerFound == false) HICR_THROW_RUNTIME("Could not find index of producer '%s' for edge '%s'. This is a bug in hLLM.", producerPartitionName.c_str(), edgeConfig->getName().c_str());

        // Adding new entry
        _inputEdges.push_back( edgeInfo_t { .index = edgeIdx, .config = edgeConfig, .producerPartitionIndex = producerPartitionIdx, .consumerPartitionIndex = _partitionIdx });
      } 

      // If I am a producer in this edge
      if (edgeConfig->getProducer() == partitionName)
      {
        // Looking for the index of the consumer of the input
        const auto& consumerPartitionName = edgeConfig->getConsumer(); 
        configuration::Partition::partitionIndex_t consumerPartitionIdx = 0;
        bool consumerFound = false;
        for (const auto& partition : _deployment.getPartitions())
        {
          if (partition->getName() == consumerPartitionName)
          {
            consumerFound = true;
            break;
          } 
          consumerPartitionIdx++;
        }

        // Sanity check
        if (consumerFound == false) HICR_THROW_RUNTIME("Could not find index of consumer '%s' for edge '%s'. This is a bug in hLLM.", consumerPartitionName.c_str(), edgeConfig->getName().c_str());

        // Adding new entry
        _outputEdges.push_back( edgeInfo_t { .index = edgeIdx, .config = edgeConfig, .producerPartitionIndex = _partitionIdx, .consumerPartitionIndex = consumerPartitionIdx });
      } 
    }
  }

  void initialize()
  {
    // Set to continue running until the deployment is stopped
    _continueRunning = true;

    initializeImpl();
  }

  protected: 

  /// This function completes the initialization of the edges, after the memory slot exchanges are completed
  virtual void initializeEdges(const HiCR::GlobalMemorySlot::tag_t tag) = 0;
  virtual void getMemorySlotsToExchange(std::vector<hLLM::edge::memorySlotExchangeInfo_t>& memorySlots) = 0;
  virtual void initializeImpl() = 0;

  const configuration::Deployment _deployment;
  const configuration::Partition::partitionIndex_t _partitionIdx;
  taskr::Runtime* const _taskr;

  // Container for input edges for this partition
  std::vector<edgeInfo_t> _inputEdges;

  // Container for output edges for this partition
  std::vector<edgeInfo_t> _outputEdges;

  // Flag indicating whether the execution must keep running
  __volatile__ bool _continueRunning;

}; // class Coordinator

} // namespace hLLM::coordinator
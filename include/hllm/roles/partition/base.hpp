#pragma once

#include <memory>
#include <vector>
#include <hicr/core/exceptions.hpp>
#include <hicr/core/definitions.hpp>
#include <hicr/core/globalMemorySlot.hpp>
#include <taskr/taskr.hpp>
#include "../../role.hpp"
#include "../../edge/base.hpp"
#include "../../edge/output.hpp"
#include "../../configuration/deployment.hpp"

namespace hLLM::roles::partition
{

class Base : public hLLM::Role
{
  public:

  #define __HLLM_PARTITION_DEFAULT_DATA_BUFFER_CAPACITY 1

  Base() = delete;
  ~Base() = default;

  Base(
    const configuration::Deployment deployment,
    const configuration::Partition::partitionIndex_t partitionIdx,
    taskr::Runtime* const taskr
  ) : Role(deployment, taskr),
    _partitionIdx(partitionIdx)
  {
    // Get my partition configuration
    const auto& partitionConfiguration = _deployment.getPartitions()[_partitionIdx];

    // Get my partition name
    const auto& partitionName = partitionConfiguration->getName();

    // Getting list of edges in the deployment
    const auto& edgeConfigs = _deployment.getEdges();

    // Iterating through edges by their index and creating them
    size_t inputEdgeVectorPosition = 0;
    for (configuration::Edge::edgeIndex_t edgeIdx = 0; edgeIdx < edgeConfigs.size(); edgeIdx++)
    {
      // Getting edge object by index
      const auto edgeConfig = edgeConfigs[edgeIdx];

      // Getting edge name
      const auto& edgeName = edgeConfig->getName();

      //printf("Edge: '%s' - Producer: %s, Consumer: %s\n", edgeConfig->getName().c_str(), edgeConfig->getProducer().c_str(), edgeConfig->getConsumer().c_str());
      // If I am a consumer in this edge and it is not a request manager output
      if (edgeConfig->getConsumer() == partitionName && edgeConfig->isResultEdge() == false)
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
        _inputEdges.push_back( edge::edgeInfo_t { .index = edgeIdx, .config = edgeConfig, .producerPartitionIndex = producerPartitionIdx, .consumerPartitionIndex = _partitionIdx });

        // Adding map entry to link the edge index to its position
        _edgeIndexToVectorPositionMap[edgeIdx] = inputEdgeVectorPosition;

        // Increasing position
        inputEdgeVectorPosition++;
      } 

      // If I am a producer in this edge
      size_t outputEdgeVectorPosition = 0;
      if (edgeConfig->getProducer() == partitionName  && edgeConfig->isPromptEdge() == false)
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
        _outputEdges.push_back( edge::edgeInfo_t { .index = edgeIdx, .config = edgeConfig, .producerPartitionIndex = _partitionIdx, .consumerPartitionIndex = consumerPartitionIdx });

        // Adding map entry to link the edge index to its position
        _edgeIndexToVectorPositionMap[edgeIdx] = outputEdgeVectorPosition;

        // Increasing position
        outputEdgeVectorPosition++;
      } 
    }
  }

  protected: 

  const configuration::Partition::partitionIndex_t _partitionIdx;

  // Container for input edges for this partition
  std::vector<edge::edgeInfo_t> _inputEdges;

  // Container for output edges for this partition
  std::vector<edge::edgeInfo_t> _outputEdges;

  // This map links an edge index (as defined in the deployment configuration) to its position in the input/output vectors
  std::map<hLLM::configuration::Edge::edgeIndex_t, size_t> _edgeIndexToVectorPositionMap;
}; // class Base

} // namespace hLLM
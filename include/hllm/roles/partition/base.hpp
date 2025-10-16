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
  
  struct edgeInfo_t
  {
    configuration::Edge::edgeIndex_t index;
    std::shared_ptr<hLLM::configuration::Edge> config;
    configuration::Partition::partitionIndex_t producerPartitionIndex;
    configuration::Partition::partitionIndex_t consumerPartitionIndex;
  };

  #define __HLLM_PARTITION_DEFAULT_DATA_BUFFER_CAPACITY 1

  // A job represents the series of tasks and input/output data a given prompt requires to execute in this particular partition
  class Job
  {
    public:

    struct edgeData_t
    {
      edgeInfo_t edgeInfo;
      std::shared_ptr<HiCR::LocalMemorySlot> edgeSlot;
      bool isSatisfied;
    };

    Job() = delete;
    ~Job() = default;

    Job(const Prompt::promptId_t promptId,
            const std::vector<edgeInfo_t>& inputEdges,
            const std::vector<edgeInfo_t>& outputEdges) :
            _promptId(promptId)
    {
      // Store data and allocate buffers for input edges
      for (const auto& edgeInfo : inputEdges)  _inputs.push_back(  edgeData_t{.edgeInfo = edgeInfo, .edgeSlot = nullptr, .isSatisfied = false} );
      for (const auto& edgeInfo : outputEdges) _outputs.push_back( edgeData_t{.edgeInfo = edgeInfo, .edgeSlot = nullptr, .isSatisfied = false} );
    }

    // Satisfying an input means that the data for that input has arrived from a peer coordinator to another.
    // Now we make a copy of the data to store in this prompt object until is is needed for execution
    __INLINE__ void satisfyInput(const size_t edgeVectorPosition, const uint8_t* data, const size_t size) { satisfyEdge(_inputs[edgeVectorPosition], data, size); }

    // Satisfying an output means that the data for that output has arrived from a replica to a coordinator.
    // Now we make a copy of the data to store in this prompt object until is is needed for sending out
    __INLINE__ void satisfyOutput(const size_t edgeVectorPosition, const uint8_t* data, const size_t size) { satisfyEdge(_outputs[edgeVectorPosition], data, size); }

    // Indicates whether the prompt is ready to be sent to a replica
    // For this, we need to check that all inputs coming from external sources have been satisfied
    [[nodiscard]] __INLINE__ bool isReadyToBeSentToReplica()
    {
      for (const auto& input : _inputs)
      {
        if (input.edgeInfo.consumerPartitionIndex == input.edgeInfo.producerPartitionIndex && input.edgeInfo.config->isPromptEdge() == false) continue; // Ignore internal edges
        if (input.isSatisfied == false) return false;
      }
      return true;
    }

    // Indicates whether the prompt's outputs destined to other partitions are ready for forwarding to the next partition(s)
    [[nodiscard]] __INLINE__ bool isReadyToForward()
    {
      for (const auto& output : _outputs)
      {
        if (output.edgeInfo.consumerPartitionIndex == output.edgeInfo.producerPartitionIndex && output.edgeInfo.config->isResultEdge() == false) continue; // Ignore internal edges
        if (output.isSatisfied == false) return false;
      }
      return true;
    }

    [[nodiscard]] Prompt::promptId_t getPromptId() const { return _promptId; }
    [[nodiscard]] const std::vector<edgeData_t>& getInputEdges() const { return _inputs;  }

    private:

    __INLINE__ void satisfyEdge(edgeData_t& edge, const uint8_t* data, const size_t size)
    {
      // Sanity check
      if (edge.isSatisfied == true) HICR_THROW_RUNTIME("Trying to satisfy edge index %lu ('%s') that was already satisfied. This must be a bug in HiCR\n", edge.edgeInfo.config->getName().c_str(), edge.edgeInfo.index);

      // Getting relevant managers
      auto edgeMemoryManager = edge.edgeInfo.config->getPayloadMemoryManager();
      auto edgeMemorySpace = edge.edgeInfo.config->getPayloadMemorySpace();
      auto edgeCommunicationManager = edge.edgeInfo.config->getPayloadCommunicationManager();

      // Registering memory slot for the incoming data
      const auto srcSlot = edgeMemoryManager->registerLocalMemorySlot(edgeMemorySpace, (void*)data, size);

      // Creating new buffer for the edge's data
      edge.edgeSlot = edgeMemoryManager->allocateLocalMemorySlot(edgeMemorySpace, size);

      // Copying the data to the prompt buffer
      edgeCommunicationManager->memcpy(edge.edgeSlot, 0, srcSlot, 0, size);
      edgeCommunicationManager->fence(edge.edgeSlot, 0, 1);

      // Deregister memory slot for the incoming data
      edgeMemoryManager->deregisterLocalMemorySlot(srcSlot);

      // Setting input as satisfied
      edge.isSatisfied = true;
    }

    const Prompt::promptId_t _promptId;
    std::vector<edgeData_t> _inputs;
    std::vector<edgeData_t> _outputs;
  }; // class Job

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
        _inputEdges.push_back( edgeInfo_t { .index = edgeIdx, .config = edgeConfig, .producerPartitionIndex = producerPartitionIdx, .consumerPartitionIndex = _partitionIdx });

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
        _outputEdges.push_back( edgeInfo_t { .index = edgeIdx, .config = edgeConfig, .producerPartitionIndex = _partitionIdx, .consumerPartitionIndex = consumerPartitionIdx });

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
  std::vector<edgeInfo_t> _inputEdges;

  // Container for output edges for this partition
  std::vector<edgeInfo_t> _outputEdges;

  // This map links an edge index (as defined in the deployment configuration) to its position in the input/output vectors
  std::map<hLLM::configuration::Edge::edgeIndex_t, size_t> _edgeIndexToVectorPositionMap;
}; // class Base

} // namespace hLLM
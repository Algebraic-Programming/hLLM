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
  class Job final
  {
    public:

    class Edge final
    {
      public: 

      Edge(const edgeInfo_t edgeInfo) : _edgeInfo(edgeInfo) {}
      ~Edge() = default;

      [[nodiscard]] __INLINE__ const auto& getDataSlot() const { return _dataSlot; }
      [[nodiscard]] __INLINE__ const auto& getEdgeInfo() const { return _edgeInfo; }
      [[nodiscard]] __INLINE__ const bool isSatisfied() const { return _isSatisfied; }

      __INLINE__ void setSatisfied(const bool value = true)  { _isSatisfied = value; }
      __INLINE__ void setDataSlot(const std::shared_ptr<HiCR::LocalMemorySlot> dataSlot) { _dataSlot = dataSlot; }

      __INLINE__ void freeDataSlot()
      {
        // Getting relevant managers
        auto edgeMemoryManager = _edgeInfo.config->getPayloadMemoryManager();

        // Deregister memory slot for the incoming data
        edgeMemoryManager->freeLocalMemorySlot(_dataSlot);
      }

      // Make a copy of the data for the provided input edge
      __INLINE__ void storeDataByCopy(const uint8_t* data, const size_t size)
      {
        // Getting relevant managers
        auto edgeMemoryManager = _edgeInfo.config->getPayloadMemoryManager();
        auto edgeMemorySpace = _edgeInfo.config->getPayloadMemorySpace();
        auto edgeCommunicationManager = _edgeInfo.config->getPayloadCommunicationManager();

        // Registering memory slot for the incoming data
        const auto srcSlot = edgeMemoryManager->registerLocalMemorySlot(edgeMemorySpace, (void*)data, size);

        // Creating new buffer for the edge's data
        const auto dataSlot = edgeMemoryManager->allocateLocalMemorySlot(edgeMemorySpace, size);

        // Copying the data to the prompt buffer
        edgeCommunicationManager->memcpy(dataSlot, 0, srcSlot, 0, size);
        edgeCommunicationManager->fence(dataSlot, 0, 1);

        // Deregister memory slot for the incoming data
        edgeMemoryManager->deregisterLocalMemorySlot(srcSlot);

        // Setting new data slot
        setDataSlot(dataSlot);
      }

      // Store a reference/cpoint to the data for the provided input edge
      __INLINE__ void storeDataByReference(const uint8_t* data, const size_t size)
      {
        // Getting relevant managers
        auto edgeMemoryManager = _edgeInfo.config->getPayloadMemoryManager();
        auto edgeMemorySpace = _edgeInfo.config->getPayloadMemorySpace();

        // Creating new buffer for the edge's data
        const auto dataSlot = edgeMemoryManager->registerLocalMemorySlot(edgeMemorySpace, (void*)data, size);

        // Setting new data slot
        setDataSlot(dataSlot);
      }

      private: 

      const edgeInfo_t _edgeInfo;
      std::shared_ptr<HiCR::LocalMemorySlot> _dataSlot = nullptr;
      bool _isSatisfied = false;
    };

    Job() = delete;
    ~Job() = default;

    Job(const Prompt::promptId_t promptId,
            const std::vector<edgeInfo_t>& inputEdges,
            const std::vector<edgeInfo_t>& outputEdges) :
            _promptId(promptId)
    {
      // Store data and allocate buffers for input edges
      for (const auto& edgeInfo : inputEdges)  _inputs.push_back(Job::Edge(edgeInfo));
      for (const auto& edgeInfo : outputEdges) _outputs.push_back(Job::Edge(edgeInfo));
    }
    
    // Indicates whether the prompt is ready to be sent to a replica
    // For this, we need to check that all inputs coming from external sources have been satisfied
    [[nodiscard]] __INLINE__ bool isReadyToBeSentToReplica()
    {
      for (const auto& input : _inputs)
      {
        if (input.getEdgeInfo().consumerPartitionIndex == input.getEdgeInfo().producerPartitionIndex &&
            input.getEdgeInfo().config->isPromptEdge() == false) continue; // Ignore internal edges
        if (input.isSatisfied() == false) return false;
      }
      return true;
    }

    // Indicates whether the prompt's outputs destined to other partitions are ready for forwarding to the next partition(s)
    [[nodiscard]] __INLINE__ bool isReadyToForward()
    {
      for (const auto& output : _outputs)
      {
        if (output.getEdgeInfo().consumerPartitionIndex == output.getEdgeInfo().producerPartitionIndex &&
            output.getEdgeInfo().config->isResultEdge() == false) continue; // Ignore internal edges
        if (output.isSatisfied() == false) return false;
      }
      return true;
    }

    [[nodiscard]] Prompt::promptId_t getPromptId() const { return _promptId; }
    [[nodiscard]] std::vector<Edge>& getInputEdges() { return _inputs;  }
    [[nodiscard]] std::vector<Edge>& getOutputEdges() { return _outputs;  }

    private:

    const Prompt::promptId_t _promptId;
    std::vector<Job::Edge> _inputs;
    std::vector<Job::Edge> _outputs;
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
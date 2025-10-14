#pragma once

#include <cstdint>
#include "edge/base.hpp"

namespace hLLM
{

class Prompt
{
  public:

  #pragma pack(push, 1)
  struct promptId_t
  {
    sessionId_t sessionId;
    messageId_t messageId;
  };
  #pragma pack(pop)
  
  struct edgeData_t
  {
    hLLM::edge::edgeInfo_t edgeInfo;
    std::shared_ptr<HiCR::LocalMemorySlot> edgeSlot;
    bool isSatisfied;
  };

  Prompt() = delete;
  ~Prompt() = default;

  Prompt(const promptId_t promptId,
          const std::vector<hLLM::edge::edgeInfo_t>& inputEdges,
          const std::vector<hLLM::edge::edgeInfo_t>& outputEdges) :
          _promptId(promptId)
  {
    // Store data and allocate buffers for input edges
    for (const auto& edgeInfo : inputEdges)  _inputs.push_back(  edgeData_t{.edgeInfo = edgeInfo, .edgeSlot = nullptr, .isSatisfied = false} );
    for (const auto& edgeInfo : outputEdges) _outputs.push_back( edgeData_t{.edgeInfo = edgeInfo, .edgeSlot = nullptr, .isSatisfied = false} );
  }

  // Satisfying an input means that the data for that input has arrived from a peer coordinator to another.
  // Now we make a copy of the data to store in this prompt object until is is needed for execution
  __INLINE__ void satisfyInput(const size_t edgeVectorPosition, const std::shared_ptr<HiCR::LocalMemorySlot> data) { satisfyEdge(_inputs[edgeVectorPosition], data); }

  // Satisfying an output means that the data for that output has arrived from a replica to a coordinator.
  // Now we make a copy of the data to store in this prompt object until is is needed for sending out
  __INLINE__ void satisfyOutput(const size_t edgeVectorPosition, const std::shared_ptr<HiCR::LocalMemorySlot> data) { satisfyEdge(_outputs[edgeVectorPosition], data); }

  // Indicates whether the prompt is ready for local execution (or to be sent to a replica)
  // For this, we need to check that all inputs coming from external sources have been satisfied
  __INLINE__ bool isReadyToExecute()
  {
    for (const auto& input : _inputs) 
      if (input.edgeInfo.consumerPartitionIndex != input.edgeInfo.producerPartitionIndex)
        if (input.isSatisfied == false) return false;
    return true;
  }

  // Indicates whether the prompt's outputs destined to other partitions are ready for forwarding to the next partition
  __INLINE__ bool isReadyToForward()
  {
    for (const auto& output : _outputs)
     if (output.edgeInfo.consumerPartitionIndex != output.edgeInfo.producerPartitionIndex)
      if (output.isSatisfied == false) return false;
    return true;
  }

  private:

  __INLINE__ void satisfyEdge(edgeData_t& edge, const std::shared_ptr<HiCR::LocalMemorySlot> data)
  {
    // Sanity check
    if (edge.isSatisfied == true) HICR_THROW_RUNTIME("Trying to satisfy edge index %lu ('%s') that was already satisfied. This must be a bug in HiCR\n", edge.edgeInfo.config->getName().c_str(), edge.edgeInfo.index);

    // Creating new buffer for the edge's data
    auto edgeMemoryManager = edge.edgeInfo.config->getPayloadMemoryManager();
    auto edgeMemorySpace = edge.edgeInfo.config->getPayloadMemorySpace();
    const size_t dataSize = data->getSize();
    edge.edgeSlot = edgeMemoryManager->allocateLocalMemorySlot(edgeMemorySpace, dataSize);

    // Copying the data to the prompt buffer
    auto edgeCommunicationManager = edge.edgeInfo.config->getPayloadCommunicationManager();
    edgeCommunicationManager->memcpy(edge.edgeSlot, 0, data, 0, dataSize);
    edgeCommunicationManager->fence(edge.edgeSlot, 0, 1);

    // Setting input as satisfied
    edge.isSatisfied = true;
  }

  const promptId_t _promptId;
  std::vector<edgeData_t> _inputs;
  std::vector<edgeData_t> _outputs;

}; // class Prompt

} // namespace hLLM
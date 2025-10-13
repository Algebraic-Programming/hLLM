#pragma once

#include <cstdint>
#include "edge/base.hpp"

namespace hLLM
{

class Request
{
  public:

  struct requestId_t
  {
    sessionId_t sessionId;
    messageId_t messageId;
  };

  struct edgeData_t
  {
    hLLM::edge::edgeInfo_t edgeInfo;
    std::shared_ptr<HiCR::LocalMemorySlot> edgeSlot;
    bool isSatisfied;
  };

  Request() = delete;
  ~Request() = default;

  Request(const requestId_t requestId,
          const std::vector<hLLM::edge::edgeInfo_t>& inputEdges,
          const std::vector<hLLM::edge::edgeInfo_t>& outputEdges) :
          _requestId(requestId)
  {
    // Store data and allocate buffers for input edges
    for (const auto& edgeInfo : inputEdges)  _inputs.push_back(  edgeData_t{.edgeInfo = edgeInfo, .edgeSlot = nullptr, .isSatisfied = false} );
    for (const auto& edgeInfo : outputEdges) _outputs.push_back( edgeData_t{.edgeInfo = edgeInfo, .edgeSlot = nullptr, .isSatisfied = false} );
  }

  // Satisfying an input means that the data for that input has arrived from a peer coordinator to another.
  // Now we make a copy of the data to store in this request object until is is needed for execution
  __INLINE__ void satisfyInput(const size_t edgeVectorPosition, const std::shared_ptr<HiCR::LocalMemorySlot> data) { satisfyEdge(_inputs[edgeVectorPosition], data); }

  // Satisfying an output means that the data for that output has arrived from a replica to a coordinator.
  // Now we make a copy of the data to store in this request object until is is needed for sending out
  __INLINE__ void satisfyOutput(const size_t edgeVectorPosition, const std::shared_ptr<HiCR::LocalMemorySlot> data) { satisfyEdge(_outputs[edgeVectorPosition], data); }

  // Indicates whether the request is ready for local execution (or to be sent to a replica)
  // For this, we need to check that all inputs coming from external sources have been satisfied
  __INLINE__ bool isReadyToExecute()
  {
    for (const auto& input : _inputs) 
      if (input.edgeInfo.consumerPartitionIndex != input.edgeInfo.producerPartitionIndex)
        if (input.isSatisfied == false) return false;
    return true;
  }

  // Indicates whether the request's outputs destined to other partitions are ready for forwarding to the next partition
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

    // Copying the data to the request buffer
    auto edgeCommunicationManager = edge.edgeInfo.config->getPayloadCommunicationManager();
    edgeCommunicationManager->memcpy(edge.edgeSlot, 0, data, 0, dataSize);
    edgeCommunicationManager->fence(edge.edgeSlot, 0, 1);

    // Setting input as satisfied
    edge.isSatisfied = true;
  }

  const requestId_t _requestId;
  std::vector<edgeData_t> _inputs;
  std::vector<edgeData_t> _outputs;

}; // class Request

} // namespace hLLM
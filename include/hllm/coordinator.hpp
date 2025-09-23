#pragma once

#include <memory>
#include <vector>
#include <hicr/core/exceptions.hpp>
#include <hicr/core/definitions.hpp>
#include <taskr/taskr.hpp>
#include "configuration/deployment.hpp"
#include "edge/input.hpp"
#include "edge/output.hpp"

namespace hLLM
{

class Coordinator final
{
  public:

  Coordinator() = delete;

  Coordinator(
    const configuration::Deployment deployment,
    const size_t partitionIdx
  ) :
    _deployment(deployment),
    _partitionIdx(partitionIdx)
  {}

  ~Coordinator() = default;

  __INLINE__ void deploy()
  {
    // Get my partition configuration
    const auto& partitionConfiguration = _deployment.getPartitions()[_partitionIdx];

    // Get my partition name
    const auto& partitionName = partitionConfiguration->getName();

    // Getting list of edges in the deployment
    const auto& edgeConfigs = _deployment.getEdges();

    // Iterating through edges by their index
    for (configuration::Edge::edgeIndex_t edgeIdx = 0; edgeIdx < edgeConfigs.size(); edgeIdx++)
    {
      // Getting edge object by index
      const auto edgeConfig = edgeConfigs[edgeIdx];

      // If I am a consumer in this edge
      if (edgeConfig->getConsumer() == partitionName)
      {
        // Create the input edges to pass this information to the receiving partition
        _partitionInputs.push_back(std::make_shared<edge::Input>(*edgeConfig, edgeIdx, edge::Base::coordinatorReplicaIndex));
      } 

      // If I am a producer in this edge
      if (edgeConfig->getProducer() == partitionName)
      {
        // Create the output edge to pass this information to the receiving partition
        _partitionOutputs.push_back(std::make_shared<edge::Output>(*edgeConfig, edgeIdx, edge::Base::coordinatorReplicaIndex));
      } 
    }

    printf("Deploying Partition Index %lu - Name: %s - %lu Consumer / %lu Producer edges...\n", _partitionIdx, partitionName.c_str(), _partitionInputs.size(), _partitionOutputs.size());
  }

  private:

  const configuration::Deployment _deployment;
  const size_t _partitionIdx;

  // Input edges from other partition coordinators
  std::vector<std::shared_ptr<edge::Input>> _partitionInputs;
  std::vector<std::shared_ptr<edge::Output>> _partitionOutputs;

  // Input edges from my partition's replicas
  std::vector<std::shared_ptr<edge::Input>> _replicaInputs;
  std::vector<std::shared_ptr<edge::Output>> _replicaOutputs;

}; // class Coordinator

} // namespace hLLM
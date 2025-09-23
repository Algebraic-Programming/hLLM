#pragma once

#include <memory>
#include <hicr/core/exceptions.hpp>
#include <hicr/core/definitions.hpp>
#include <taskr/taskr.hpp>
#include "configuration/deployment.hpp"

namespace hLLM
{

class Replica final
{
  public:
  
  Replica() = delete;

  Replica(const configuration::Deployment deployment,
     const configuration::Partition::partitionIndex_t  partitionIdx,
     const configuration::Replica::replicaIndex_t replicaIdx) :
    _deployment(deployment),
    _partitionIdx(partitionIdx),
    _replicaIdx(replicaIdx)
  {}

  ~Replica() = default;

  __INLINE__ void deploy()
  {
    // Get my partition configuration
    const auto& partitionConfiguration = _deployment.getPartitions()[_partitionIdx];

    // Get my partition name
    const auto& partitionName = partitionConfiguration->getName();

    // Getting list of edges in the deployment
    const auto& edges = _deployment.getEdges();

    // Gathering edges where I am the producer
    std::vector<std::shared_ptr<configuration::Edge>> _producerEdges;
    for (const auto& e : edges) if (e->getProducer() == partitionName) _producerEdges.push_back(e);

    // Gathering edges where I am the consumer
    std::vector<std::shared_ptr<configuration::Edge>> _consumerEdges;
    for (const auto& e : edges) if (e->getConsumer() == partitionName) _consumerEdges.push_back(e);

    printf("Deploying Replica Index %lu/%lu - Name: %s - %lu Consumer / %lu Producer edges...\n", _partitionIdx, _replicaIdx, partitionName.c_str(), _consumerEdges.size(), _producerEdges.size());
  }

  private:

  const configuration::Deployment _deployment;
  const configuration::Partition::partitionIndex_t _partitionIdx;
  const configuration::Replica::replicaIndex_t _replicaIdx;

}; // class Replica

} // namespace hLLM
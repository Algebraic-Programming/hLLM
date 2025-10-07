#pragma once

#include "../configuration/partition.hpp"
#include "../configuration/replica.hpp"

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

  Replica(const configuration::Partition::partitionIndex_t partitionIndex, const configuration::Replica::replicaIndex_t replicaIndex) :
    _partitionIndex(partitionIndex),
    _replicaIndex(replicaIndex)
    {

    }

  ~Replica() = default;

  private:

  const configuration::Partition::partitionIndex_t _partitionIndex; 
  const configuration::Replica::replicaIndex_t _replicaIndex;

}; // class Replica

} // namespace hLLM::coordinator
#pragma once

#include <memory>
#include <vector>
#include <hicr/core/exceptions.hpp>
#include <hicr/core/definitions.hpp>
#include <taskr/taskr.hpp>
#include "../../configuration/deployment.hpp"
#include "../../edge/input.hpp"
#include "../../edge/output.hpp"
#include "../../messages/base.hpp"
#include "../../messages/heartbeat.hpp"
#include "../../messages/data.hpp"
#include "../../messages/prompt.hpp"
#include "../../prompt.hpp"
#include "base.hpp"

namespace hLLM::roles::partition
{

class Coordinator final : public Base
{
  private:

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
      const std::vector<edgeInfo_t>& coordinatorInputs,
      const std::vector<edgeInfo_t>& coordinatorOutputs,
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

    [[nodiscard]] __INLINE__ configuration::Replica::replicaIndex_t getReplicaIdx() const { return _replicaIndex; }

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

  public:

  Coordinator() = delete;
  ~Coordinator() = default;

  Coordinator(
    const configuration::Deployment deployment,
    const configuration::Partition::partitionIndex_t partitionIdx,
    taskr::Runtime* const taskr
  ) :
    Base(deployment, partitionIdx, taskr)
  {
    // Get my partition configuration
    const auto& partitionConfiguration = _deployment.getPartitions()[_partitionIdx];

    // Get my partition name
    const auto& partitionName = partitionConfiguration->getName();

    // Getting list of replicas in the partition
    const auto& replicas = partitionConfiguration->getReplicas();

    // Filling replica set
    for (configuration::Replica::replicaIndex_t replicaIndex = 0; replicaIndex < replicas.size(); replicaIndex++)
    {
      // Creating new replica object, to represent an actual replica we can communicate to/from
      auto newReplica = std::make_shared<Replica>(_partitionIdx, replicaIndex, _inputEdges, _outputEdges, _controlEdgeConfig);

      // Adding new replica to the collection
      _replicaMap[replicaIndex] = newReplica;

      // Adding replica to the ready replica queue
      _replicaQueue.push(newReplica);
    }

    // Iterating through input edges to create a connection with replicas and peer coordinators on that input
    for (const auto& edge : _inputEdges)
    {
      const auto edgeIdx = edge.index;
      const auto& edgeConfig = edge.config;
      const auto& edgeName = edgeConfig->getName();
      const auto producerPartitionIdx = edge.producerPartitionIndex;
      const auto consumerPartitionIdx = edge.consumerPartitionIndex;
        
      // Defining edge type, based on whether we expect this data from another coordinator or from the request manager
      auto edgeType = edgeConfig->isPromptEdge() ? edge::edgeType_t::requestManagerToCoordinator : edge::edgeType_t::coordinatorToCoordinator;

      // Create the input edges to pass this information to the receiving partition
      _partitionDataInputs.push_back(std::make_shared<edge::Input>(*edgeConfig, edgeType, edgeIdx, producerPartitionIdx, consumerPartitionIdx, edge::Base::coordinatorReplicaIndex));

      if (edgeConfig->isPromptEdge()) printf("[Coordinator] Prompt Input Edge: Type: %u, EdgeIdx: %lu, CP: %lu, PP: %lu, RI: %lu\n", edgeType, edgeIdx, producerPartitionIdx, consumerPartitionIdx, edge::Base::coordinatorReplicaIndex);
    } 

    // Iterating through output edges to create a connection with replicas and peer coordinators on that output
    for (const auto& edge : _outputEdges)
    {
      const auto edgeIdx = edge.index;
      const auto& edgeConfig = edge.config;
      const auto& edgeName = edgeConfig->getName();
      const auto producerPartitionIdx = edge.producerPartitionIndex;
      const auto consumerPartitionIdx = edge.consumerPartitionIndex;

        // Defining edge type, based on whether we expect to push this data to another coordinator or to the request manager
      auto edgeType = edgeConfig->isResultEdge() ?  edge::edgeType_t::coordinatorToRequestManager : edge::edgeType_t::coordinatorToCoordinator;

      // Create the output edge to pass this information to the receiving partition
      _partitionDataOutputs.push_back(std::make_shared<edge::Output>(*edgeConfig, edgeType, edgeIdx, producerPartitionIdx, consumerPartitionIdx, edge::Base::coordinatorReplicaIndex));

      if (edgeConfig->isResultEdge()) printf("[Coordinator] Result Output Edge: Type: %u, EdgeIdx: %lu, CP: %lu, PP: %lu, RI: %lu\n", edgeType, edgeIdx, producerPartitionIdx, consumerPartitionIdx, edge::Base::coordinatorReplicaIndex);
    } 
  }

  // Gets the memory slots required by the edges
  __INLINE__ void getMemorySlotsToExchange(std::vector<hLLM::edge::memorySlotExchangeInfo_t>& memorySlots)
  {
    for (const auto& edge : _partitionDataInputs)  edge->getMemorySlotsToExchange(memorySlots);
    for (const auto& edge : _partitionDataOutputs) edge->getMemorySlotsToExchange(memorySlots);
    for (const auto& replica : _replicaMap) replica.second->getMemorySlotsToExchange(memorySlots);
  }

  /// This function completes the initialization of the edges, after the memory slot exchanges are completed
  __INLINE__ void initializeEdges(const HiCR::GlobalMemorySlot::tag_t tag)
  {
    for (const auto& edge : _partitionDataInputs)  edge->initialize(tag);
    for (const auto& edge : _partitionDataOutputs) edge->initialize(tag);
    for (const auto& replica : _replicaMap) replica.second->initializeEdges(tag);
  }

  private:

  /// This function subscribes the handlers and services for the coordinator role
  __INLINE__ void initializeImpl() override
  {
    // Get my partition configuration
    const auto& partitionConfiguration = _deployment.getPartitions()[_partitionIdx];

    // Get my partition name
    const auto& partitionName = partitionConfiguration->getName();

    // Welcome message
    printf("Initializing Partition Coordinator Index %lu - Name: %s - %lu Consumer / %lu Producer edges...\n", _partitionIdx, partitionName.c_str(), _partitionDataInputs.size(), _partitionDataOutputs.size());

    // Subscribing to the heartbeat sending service for my replicas
    for (const auto& replica : _replicaMap) subscribeHeartbeatEdge(replica.second->getControlOutput());

    // Subscribing control edges to the message service for my replicas
    for (const auto& replica : _replicaMap)
        subscribeEdgeMessageHandler(hLLM::Role::edgeHandlerSubscription_t { hLLM::messages::messageTypes::heartbeat,
        replica.second->getControlInput(),
        [this](const std::shared_ptr<edge::Input> edge, const hLLM::edge::Message& message){ heartbeatMessageHandler(edge, std::make_shared<hLLM::messages::Heartbeat>(message)); } });

    // Subscribing data input edges to the message service for my replicas results (outputs)
    for (const auto& replica : _replicaMap) for (const auto& dataEdge : replica.second->getDataInputs())
        subscribeEdgeMessageHandler(hLLM::Role::edgeHandlerSubscription_t { hLLM::messages::messageTypes::data,
        dataEdge,
        [this](const std::shared_ptr<edge::Input> edge, const hLLM::edge::Message& message){ outputDataMessageHandler(edge, std::make_shared<hLLM::messages::Data>(message)); } });

    // Subscribing data input edges to the message service for my peers
    for (const auto& dataEdge : _partitionDataInputs) 
        subscribeEdgeMessageHandler(hLLM::Role::edgeHandlerSubscription_t { hLLM::messages::messageTypes::data,
        dataEdge,
        [this](const std::shared_ptr<edge::Input> edge, const hLLM::edge::Message& message){ inputDataMessageHandler(edge, std::make_shared<hLLM::messages::Data>(message)); } });

        // Registering service for job management
    _taskr->addService(&_taskrJobManagementService);
  }

  __INLINE__ void heartbeatMessageHandler(const std::shared_ptr<edge::Input> edge, const std::shared_ptr<hLLM::messages::Heartbeat> message)
  {
    const auto replicaIdx = edge->getReplicaIndex();
    if(_deployment.getHeartbeat().visible == true) printf("[Coordinator %lu] Received heartbeat from replica %lu.\n", _partitionIdx, replicaIdx);
  }

  __INLINE__ void inputDataMessageHandler(const std::shared_ptr<edge::Input> edge, const std::shared_ptr<hLLM::messages::Data> message)
  {
    // Getting prompt id from data
    const auto promptId = message->getPromptId();
    const auto data = message->getData();
    const auto size = message->getSize();
    const auto edgeIdx = edge->getEdgeIndex();
    const auto edgePos = _edgeIndexToVectorPositionMap[edgeIdx];
    
    printf("[Coordinator %lu] Received data for prompt %lu/%lu, edge '%s'.\n", _partitionIdx, promptId.first, promptId.second, edge->getEdgeConfig().getName().c_str());

    // Pointer to the job object
    std::shared_ptr<Job> job;

    // Check if there exists already a job for this incoming data
    // If it does not exist, create a new one. Otherwise, take it from the map
    _jobMapMutex.lock();
    if(_jobMap.contains(promptId) == false)
    {
      // Creating new job entry
      job = std::make_shared<Job>(promptId, _inputEdges, _outputEdges);

      // Adding job to the map
      _jobMap[promptId] = job;

      // Also adding to the queue of pending jobs
      _pendingJobQueueMutex.lock();
      _pendingJobQueue.push(job);
      _pendingJobQueueMutex.unlock();
    } 

    // Otherwise, it exists so grab it from the job map
    else job = _jobMap.at(promptId);
    _jobMapMutex.unlock();
    
    // Getting the input that is satisfied by this message
    auto& input = job->getInputEdges()[edgePos];

    // // Making a copy of the data into the edge buffer -- we don't do it by referece because we want to free up the input channels immediately to avoid deadlocks
    input.storeDataByCopy(data, size);

    // // Setting edge as satisfied
    input.setSatisfied();
  }

  __INLINE__ void outputDataMessageHandler(const std::shared_ptr<edge::Input> edge, const std::shared_ptr<hLLM::messages::Data> message)
  {
    // Getting prompt id from data
    const auto promptId = message->getPromptId();
    const auto data = message->getData();
    const auto size = message->getSize();
    const auto edgeIdx = edge->getEdgeIndex();
    const auto edgePos = _edgeIndexToVectorPositionMap[edgeIdx];
    const auto replicaIdx = edge->getReplicaIndex();

    printf("[Coordinator %lu] Received output data from replica %lu for prompt %lu/%lu, edge '%s'.\n", _partitionIdx, replicaIdx, promptId.first, promptId.second, edge->getEdgeConfig().getName().c_str());

    // Check if there exists already a job for this incoming output data
    _jobMapMutex.lock();
    if(_jobMap.contains(promptId) == false) HICR_THROW_RUNTIME("The prompt id corresponding to the output data does not exist. This must be a bug in hLLM\n");
    
    // Otherwise, it exists so grab it from the job map
    auto& job = _jobMap.at(promptId);
    _jobMapMutex.unlock();

    // Forward messages directly to the next peer
    const auto forwardMessage = messages::Data(data, size, promptId);
    const auto& peerOutput = _partitionDataOutputs[edgePos];

    // Send message only when the peer is ready
    while (peerOutput->isFull(size)); 
    peerOutput->pushMessage(forwardMessage.encode());

    // Getting the input that is satisfied by this message
    auto& output = job->getOutputEdges()[edgePos];

    // Setting edge as satisfied
    output.setSatisfied();

     // Checking if the job is ready to be removed
    bool isJobFinished = true; 
    for (const auto& output : job->getOutputEdges()) if (output.isSatisfied() == false) { isJobFinished = false; break; };

    // If it's finished, take statistics and remove the job from memory
    if (isJobFinished)
    {
      _jobMapMutex.lock();
      _jobMap.erase(promptId);
      _jobMapMutex.unlock();
    }

    // Re-adding replica to the queue of ready replicas
    const auto replica = _replicaMap[replicaIdx];
    _replicaQueueMutex.lock();
    _replicaQueue.push(replica);
    _replicaQueueMutex.unlock();
  }

  /////////// Job management Service3
  // Looks for a double coincidence of job ready to run and a free available replica
  __INLINE__ void jobManagementService()
  {
    // Getting a job from the pending job queue
    std::shared_ptr<Job> job = nullptr;
    _pendingJobQueueMutex.lock();
    if (_pendingJobQueue.empty() == false)
    {
      job = _pendingJobQueue.front();
      _pendingJobQueue.pop();
    } 
    _pendingJobQueueMutex.unlock();

    // If no jobs were in the queue, simply return
    if (job == nullptr) return;

    // Getting job info
    const auto promptId = job->getPromptId();

    // Checking if the job is ready to be sent to a replica
    bool isJobReady = true;
    for (const auto& input : job->getInputEdges()) if (input.isSatisfied() == false) { isJobReady = false; break; };

    // If the job is ready to go, check if there is a ready replica to take on this job
    std::shared_ptr<Replica> replica = nullptr;
    if (isJobReady)
    {
      // Check if there is a ready replica to take on this job
      _replicaQueueMutex.lock();
      if (_replicaQueue.empty() == false)
      {
        replica = _replicaQueue.front();
        _replicaQueue.pop();
      }
      _replicaQueueMutex.unlock();
    }

    // If the job is ready to go, try to send it to one of the replicas
    if (replica != nullptr) 
    {
      // Now we have a ready job and a ready replica, sending the job to the replica
      printf("Sending job for prompt %lu/%lu to replica %lu\n", promptId.first, promptId.second, replica->getReplicaIdx());
      
      // For each of the edges, push the data through the replica's channels
      for (size_t edgePos = 0; edgePos < _partitionDataInputs.size(); edgePos++)
      {
        // Getting corresponding edges
        auto& inputEdge = job->getInputEdges()[edgePos];
        const auto& outputEdge = replica->getDataOutputs()[edgePos];

        // Creating message
        const auto& dataSlot = inputEdge.getDataSlot();
        const auto message = messages::Data((const uint8_t*)dataSlot->getPointer(), dataSlot->getSize(), promptId);
        outputEdge->pushMessage(message.encode());

        // Free up edge data copy
        inputEdge.freeDataSlot();
      }

      // Return now to avoid putting the job and the replica back into their queues
      return;
    }

    // If the job wasn't ready or there was no replica found for it, simply put it back into the queue
    _pendingJobQueueMutex.lock();
    _pendingJobQueue.push(job);
    _pendingJobQueueMutex.unlock();
  }
  taskr::Service::serviceFc_t _jobManagementServiceFunction = [this](){ this->jobManagementService(); };
  taskr::Service _taskrJobManagementService = taskr::Service(_jobManagementServiceFunction, 0);

  // Container for partition replica objects
  std::map<configuration::Replica::replicaIndex_t ,  std::shared_ptr<Replica>> _replicaMap;

  // Mutual exclusion mechanism to access the ready replica queue
  std::mutex _replicaQueueMutex;
  std::queue<std::shared_ptr<Replica>> _replicaQueue;

  // Data Input / Output edges from other partition coordinators
  std::vector<std::shared_ptr<edge::Input>> _partitionDataInputs;
  std::vector<std::shared_ptr<edge::Output>> _partitionDataOutputs;

  // Mutual exclusion mechanism to access the pending job queue
  std::mutex _pendingJobQueueMutex;
  std::queue<std::shared_ptr<Job>> _pendingJobQueue;

  // Map of jobs, indexed by prompt id
  std::mutex _jobMapMutex;
  std::map<Prompt::promptId_t, std::shared_ptr<Job>> _jobMap;

}; // class Coordinator

} // namespace hLLM::coordinator
#pragma once

#include <hicr/core/exceptions.hpp>
#include <hicr/core/definitions.hpp>
#include <taskr/taskr.hpp>
#include "configuration/deployment.hpp"
#include "roles/partition/coordinator.hpp"
#include "roles/partition/replica.hpp"
#include "roles/requestManager.hpp"
#include "edge/base.hpp"
#include "task.hpp"

#define __HLLM_WORKER_ENTRY_POINT_RPC_NAME "[hLLM] Worker Entry Point"
#define __HLLM_REQUEST_DEPLOYMENT_CONFIGURATION_RPC_NAME "[hLLM] Request Deployment Configuration"
#define __HLLM_REQUEST_DEPLOYMENT_STOP_RPC_NAME "[hLLM] Request Deployment Stop"
#define __HLLM_BROADCAST_DEPLOYMENT_STOP_RPC_NAME "[hLLM] Broadcast Deployment Stop"
#define __HLLM_DEFAULT_EXCHANGE_TAG 0x0000A000

namespace hLLM
{

class Engine final
{
  public:

  Engine(HiCR::InstanceManager             *instanceManager,
         HiCR::frontend::RPCEngine         *rpcEngine,
         taskr::Runtime                    *taskr,
         const HiCR::GlobalMemorySlot::tag_t exchangeTag = __HLLM_DEFAULT_EXCHANGE_TAG)
    : _instanceManager(instanceManager),
      _rpcEngine(rpcEngine),
      _taskr(taskr),
      _instanceId(_instanceManager->getCurrentInstance()->getId()),
      _exchangeTag(exchangeTag)
  {
    // Registering entry point function for partition coordinators / replicas. Not for the deployment launcher
    _rpcEngine->addRPCTarget(__HLLM_WORKER_ENTRY_POINT_RPC_NAME, HiCR::backend::pthreads::ComputeManager::createExecutionUnit([this](void*) { entryPoint(); }));

    // Registering deployment information request
    _rpcEngine->addRPCTarget(__HLLM_REQUEST_DEPLOYMENT_CONFIGURATION_RPC_NAME, HiCR::backend::pthreads::ComputeManager::createExecutionUnit([this](void *) { attendDeploymentConfigurationRequest(); }));

    // Registering finalization function (for root to execute)
    _rpcEngine->addRPCTarget(__HLLM_BROADCAST_DEPLOYMENT_STOP_RPC_NAME, HiCR::backend::pthreads::ComputeManager::createExecutionUnit([this](void *) { doLocalTermination(); }));

    // Registering finalization request function (for non-root to request roots to end the entire execution)
    _rpcEngine->addRPCTarget(__HLLM_REQUEST_DEPLOYMENT_STOP_RPC_NAME, HiCR::backend::pthreads::ComputeManager::createExecutionUnit([this](void *) { broadcastTermination(); }));
  }

  ~Engine() = default;

  /**
   * Broadcasting deployment information to all other instances
   * 
   * The broadcaster becomes the deployment launcher instance
   */
  __INLINE__ void initialize(const configuration::Deployment deployment, const HiCR::Instance::instanceId_t deployerInstanceId)
  {
    // printf("hLLM Initializing...\n");

    // The engine is not yet deployed
    _isDeployed = false;

    // Establish the designated instance as deployer instance
    _deployerInstanceId = deployerInstanceId;

    // If I am not the deployer instance, simply request the deployment information from the deployer
    if (_instanceId != _deployerInstanceId)
    {
      // Send request RPC
      _rpcEngine->requestRPC(_deployerInstanceId, __HLLM_REQUEST_DEPLOYMENT_CONFIGURATION_RPC_NAME);

      // Wait for serialized information
      auto returnValue = _rpcEngine->getReturnValue();

      // Receiving raw serialized topology information from the worker
      std::string deploymentString = (char *)returnValue->getPointer();

      // Parsing serialized raw topology into a json object
      auto deploymentJs = nlohmann::json::parse(deploymentString);

      // Creating the deployment object from the json
      _deployment = configuration::Deployment(deploymentJs);

      // Getting my partition id
      _partitionIdx = 0;

      // Freeing return value
      _rpcEngine->getMemoryManager()->freeLocalMemorySlot(returnValue);

      printf("Instance %lu - Got deployment configuration\n", _instanceId);

      // Return now
      return;
    }

    // Assigning deployment
    _deployment = deployment;

    // Getting the instances involved in the deployment (only relevant for the deployer instance)
    for (const auto& partition : _deployment.getPartitions())
    {
      // Getting the instance id assigned to the partition coordinator
      const auto coordinatorInstanceId = partition->getInstanceId();

      // Adding it to the set
      _instanceSet.insert(coordinatorInstanceId);

      // Now getting all replicas for the replica
      for (const auto& replica : partition->getReplicas())
      {
        // Getting the instance id assigned to the replica
        const auto replicaInstanceId = replica->getInstanceId();

        // Adding it to the collection
        _instanceSet.insert(replicaInstanceId);
      }
    }

    // Iterating over the instances involved in the deployment
    for (const auto instanceId : _instanceSet)
      if (instanceId != _deployerInstanceId) // If it's not me, listen for a deployment configuration request
      {
        printf("Deployer %lu: Listening for instance %lu\n", _instanceId, instanceId);
         _rpcEngine->listen();
      }
  }

  __INLINE__ void deploy(const configuration::Deployment deployment)
  {
    // If I am not the deployer instance await for the deployment launcher to request us to start
    if (_instanceId != _deployerInstanceId) { _rpcEngine->listen(); return; } 

    // Considering whether the deployment launcher is actually part of the deployment
    bool isLauncherInDeployment = false;

    // Iterating over the instances involved in the deployment and requesting them
    for (const auto instanceId : _instanceSet)
    {
      // If it's not me, then request the partition to start executing (worker entry point)
      if (instanceId != _deployerInstanceId) _rpcEngine->requestRPC(instanceId, __HLLM_WORKER_ENTRY_POINT_RPC_NAME);

      // If it's me, I am part of the execution so remember to launch later
      if (instanceId == _deployerInstanceId) isLauncherInDeployment = true;
    }

    // If I (the coordinator) am part of the deployment, run my instance now
    if (isLauncherInDeployment == true) entryPoint();
  }

  /**
   * Registers a function that can be a target as initial function for one or more requested instances.
   * 
   * If a requested initial function is not registered by this function, deployment will fail.
   * 
   * @param[in] functionName The name of the function to register. This value will be used to match against the requested partition functions
   * @param[in] fc The actual function to register
   * 
   */
  __INLINE__ void registerFunction(const std::string &functionName, const Task::taskFunction_t fc)
  {
    // Checking if the RPC name was already used
    if (_registeredFunctions.contains(functionName) == true)
    {
      fprintf(stderr, "The function '%s' was already registered.\n", functionName.c_str());
      abort();
    }

    // Adding new RPC to the set
    _registeredFunctions.insert({functionName, fc});
  }

  __INLINE__ void requestTermination()
  {
    const auto &currentInstance = *_instanceManager->getCurrentInstance();

    // If I am the deployer instance, 
    if (currentInstance.getId() == _deployerInstanceId)
    {
       printf("[hLLM] Deployer instance %lu finalizing hLLM...\n", currentInstance.getId());

       // Broadcast termination to others (and myself)
       broadcastTermination(); 
       
       // (deployer) Executing local termination myself now
       doLocalTermination();

       // Return
       return;
    } 

    // If I am not the deployer instance, request the deployer to please broadcast terminationp
    printf("[hLLM] Instance %lu requesting deployer instance %lu to finish execution.\n", currentInstance.getId(), _deployerInstanceId);
    _rpcEngine->requestRPC(_deployerInstanceId, __HLLM_REQUEST_DEPLOYMENT_STOP_RPC_NAME);
  }

  [[nodiscard]] __INLINE__ auto& getDeployment() { return _deployment; }
  [[nodiscard]] __INLINE__ size_t getPartitionIdx() const { return _partitionIdx; }

  [[nodiscard]] __INLINE__ std::shared_ptr<Session> createSession()
  {
    if (_requestManagerRole == nullptr) HICR_THROW_RUNTIME("Trying to connect to hLLM engine directly, but this is not a request manager instance");

    return _requestManagerRole->createSession();
  }

  [[nodiscard]] __INLINE__ const auto isDeployed() const { return _isDeployed; }

  private:

  __INLINE__ void doLocalTermination()
  {
    printf("[hLLM] Instance %lu terminating TaskR...\n", _deployerInstanceId);

    // Stopping the execution of the current and new tasks
    _continueRunning = false;

    // Set TaskR to finish on the last task finishing
    _taskr->setFinishOnLastTask(true);
  }

  __INLINE__ void broadcastTermination()
  {
    const auto &currentInstance = *_instanceManager->getCurrentInstance();

    if (currentInstance.getId() != _deployerInstanceId) HICR_THROW_RUNTIME("Only the deployer instance %lu can broadcast termination", _deployerInstanceId);

    // Send a finalization signal to all other non-root instances
    auto &instances = _instanceManager->getInstances();
    for (auto &instance : instances)
      if (instance->getId() != _deployerInstanceId)
      {
        printf("[hLLM] Instance %lu sending stop RPC to instance %lu.\n", _deployerInstanceId, instance->getId());
        _rpcEngine->requestRPC(instance->getId(), __HLLM_BROADCAST_DEPLOYMENT_STOP_RPC_NAME);
      }
  }

  __INLINE__ nlohmann::json parseConfiguration(const nlohmann::json &config)
  {
    // Copy the configuration
    nlohmann::json parsedConfig = config;

    // Getting the partitions vector
    const auto &partitionsJs = hicr::json::getArray<nlohmann::json>(parsedConfig, "Partitions");

    // Creating configuration for TaskR
    nlohmann::json taskrConfig;
    taskrConfig["Task Worker Inactivity Time (Ms)"] = 100;   // Suspend workers if a certain time of inactivity elapses
    taskrConfig["Task Suspend Interval Time (Ms)"]  = 100;   // Workers suspend for this time before checking back
    taskrConfig["Minimum Active Task Workers"]      = 1;     // Have at least one worker active at all times
    taskrConfig["Service Worker Count"]             = 1;     // Have one dedicated service workers at all times to listen for incoming messages
    taskrConfig["Make Task Workers Run Services"]   = false; // Workers will check for meta messages in between executions

    // Adding taskr configuration
    parsedConfig["TaskR Configuration"] = taskrConfig;

    // Creating deployR request object
    return parsedConfig;
  }

  __INLINE__ void entryPoint()
  {
    configuration::Partition::partitionIndex_t myPartitionIndex = 0;
    configuration::Replica::replicaIndex_t myReplicaIndex = 0;
    bool isPartitionCoordinator = false;
    bool isPartitionReplica = false;
    bool isRequestManager = false;

    // Sanity checks on the deployment object
    _deployment.verify();

    // Checking if I am the request manager
    printf("InstanceId: %lu, userInterfaceInstanceId: %lu\n", _instanceId, _deployment.getUserInterface().instanceId);
    if (_deployment.getUserInterface().instanceId == _instanceId) isRequestManager = true;

    // Perusing the deployment to see what my  partition role(s) is(are), if any
    for (configuration::Partition::partitionIndex_t pIdx = 0; pIdx < _deployment.getPartitions().size(); pIdx++)
    {
      // Getting partition object
      const auto partition = _deployment.getPartitions()[pIdx];

      // Getting the instance id assigned to the partition
      const auto coordinatorInstanceId = partition->getInstanceId();

      // If I am a partition coordinator, mark it now
      if (_instanceId == coordinatorInstanceId)
      {
        isPartitionCoordinator = true;
        myPartitionIndex = pIdx;
      } 

      // Now getting all replicas for the replica
      for (configuration::Replica::replicaIndex_t rIdx = 0; rIdx < partition->getReplicas().size(); rIdx++)
      {
        // Getting the instance id assigned to the replica
        const auto replicaInstanceId = partition->getReplicas()[rIdx]->getInstanceId();

        // If I am a partition 
        if (_instanceId == replicaInstanceId)
        {
          isPartitionReplica = true;
          myPartitionIndex = pIdx;
          myReplicaIndex = rIdx;
        } 
      }
    }

    // Storage for the initial set of HiCR memory slots to exchange for the creation of edges. 
    // This is a low-level aspect that normally shouldn't be exposed at this level, but it is required
    // for all partitions to partitipate since we still don't support peer-to-peer memory slot exchange
    std::vector<edge::memorySlotExchangeInfo_t> memorySlotsToExchange;

    // If I am a partition coordinator, construct the coordinator object
    if (isPartitionCoordinator == true)
    {
      printf("[Instance %lu] I am a partition %lu coordinator\n", _instanceId, myPartitionIndex);
      _partitionCoordinatorRole = std::make_unique<roles::partition::Coordinator>(_deployment, myPartitionIndex, _taskr);

      // Get memory slots to exchange for the partition coordinator
      printf("Registering Partition Coordinator Memory Slots...\n");
      _partitionCoordinatorRole->getMemorySlotsToExchange(memorySlotsToExchange);
    }

    // If I am a replica, construct the replica object:
    // Note: An instance can be simultaneously a partition coordinator and a replica
    if (isPartitionReplica == true)
    {
      printf("[Instance %lu] I am a partition %lu replica %lu\n", _instanceId, myPartitionIndex, myReplicaIndex);
      _partitionReplicaRole = std::make_unique<roles::partition::Replica>(_deployment, myPartitionIndex, myReplicaIndex, _taskr, _registeredFunctions);

      // Get memory slots to exchange for the replica
      printf("Registering Partition Replica Memory Slots...\n");
      _partitionReplicaRole->getMemorySlotsToExchange(memorySlotsToExchange);
    }

    // If I am a Request Manager, construct the role now
    if (isRequestManager == true)
    {
      printf("[Instance %lu] I am the request manager\n", _instanceId);
      _requestManagerRole = std::make_unique<roles::RequestManager>(_deployment, _taskr);

      // Get memory slots to exchange for the replica
      printf("Registering Request Manager Memory Slots...\n");
      _requestManagerRole->getMemorySlotsToExchange(memorySlotsToExchange);
    }

    // Sanity check
    if (isPartitionCoordinator == false &&
       isPartitionReplica == false &&
       isRequestManager == false) HICR_THROW_RUNTIME("Instance %lu is involved in the deployment but no role has been asigned to it. This must be a bug in hLLM", _instanceId);

    ////////// Exchange memory slots now
    // printf("[Instance %lu] Memory Slots to exchange: %lu\n", _instanceId, memorySlotsToExchange.size());

    // Finding all distinct communication managers and storing them in the order in which they were declared.
    // This is important for all intervening instances to do the exchange in the same order
    std::set<HiCR::CommunicationManager*> communicationManagerSet;
    std::vector<HiCR::CommunicationManager*> communicationManagerVector;

    // Adding control communication manager
    const auto controlCommunicationManager = _deployment.getControlBuffer().communicationManager;
    communicationManagerSet.insert(controlCommunicationManager);
    communicationManagerVector.push_back(controlCommunicationManager);

    // Adding edge-specific communication managers
    for (const auto& edge : _deployment.getEdges())
    {
      const auto coordinationComunicationManager = edge->getCoordinationCommunicationManager();
      if (communicationManagerSet.contains(coordinationComunicationManager) == false)
      {
        communicationManagerSet.insert(coordinationComunicationManager);
        communicationManagerVector.push_back(coordinationComunicationManager);
      }

      const auto payloadComunicationManager = edge->getPayloadCommunicationManager();
      if (communicationManagerSet.contains(payloadComunicationManager) == false)
      {
        communicationManagerSet.insert(payloadComunicationManager);
        communicationManagerVector.push_back(payloadComunicationManager);
      }
    }

    // Now creating a map of memory slots to exchange, mapped by communication manager
    std::map<HiCR::CommunicationManager*, std::vector<HiCR::CommunicationManager::globalKeyMemorySlotPair_t>> exchangeMap;
    for (const auto& entry : memorySlotsToExchange)
    {
      // Getting the communication manager used for this memory slot
      const auto& communicationManager = entry.communicationManager;

      // Sanity check
      if (communicationManagerSet.contains(communicationManager) == false) HICR_THROW_RUNTIME("Could not find communication manager in the set. This is a bug in hLLM");

      // Adding memory slot to the exchange map
      // printf("Exchanging Memory Slot with key: %lu\n", entry.globalKey);
      exchangeMap[communicationManager].push_back(HiCR::CommunicationManager::globalKeyMemorySlotPair_t(entry.globalKey, entry.memorySlot));
    } 

    // Finally, doing the exchange, one communication manager at a time, in the order given by the edge ordering
    printf("[Instance %lu] Exchanging Memory Slots...\n", _instanceId);
    for (const auto communicationManager : communicationManagerVector) communicationManager->exchangeGlobalMemorySlots(_exchangeTag, exchangeMap[communicationManager]);

    // Waiting for the finalization of the exchange
    for (const auto communicationManager : communicationManagerVector) communicationManager->fence(_exchangeTag);

    // After the exchange, we can now initialize the edges
    if (isPartitionCoordinator == true) _partitionCoordinatorRole->initializeEdges(_exchangeTag);
    if (isPartitionReplica == true) _partitionReplicaRole->initializeEdges(_exchangeTag);
    if (isRequestManager == true) _requestManagerRole->initializeEdges(_exchangeTag);

    // Starting a new deployment
    _continueRunning = true;

    // Initializing TaskR
    _taskr->initialize();

    // Initializing coordinator and replica roles, whichever applies (or both), so that they add their initial functions to taskR
    if (isPartitionCoordinator == true) _partitionCoordinatorRole->initialize();
    if (isPartitionReplica == true) _partitionReplicaRole->initialize();
    if (isRequestManager == true) _requestManagerRole->initialize();

    // Instruct TaskR to re-add suspended tasks
    _taskr->setTaskCallbackHandler(HiCR::tasking::Task::callback_t::onTaskSuspend, [&](taskr::Task *task) { _taskr->resumeTask(task); });

    // The engine is  now fully deployed
    _isDeployed = true;

    // Set TaskR not to finish on the last task
    _taskr->setFinishOnLastTask(false);

    // Running TaskR
    _taskr->run();
    _taskr->await();
    _taskr->finalize();

    // printf("[hLLM] Instance '%s' TaskR Stopped\n", _partitionName.c_str());
  }

  // For every new partition instance created, we send it the serialized deployment configuration
  __INLINE__ void attendDeploymentConfigurationRequest()
  {
      // Serializing
      const auto serializedDeployment = _deployment.serialize().dump();

      // Returning serialized topology
      _rpcEngine->submitReturnValue((void *)serializedDeployment.c_str(), serializedDeployment.size() + 1);
  }

  // Pointer to the instance's partition coordinator role, if defined
  std::unique_ptr<roles::partition::Coordinator> _partitionCoordinatorRole = nullptr;

  // Pointer to the instance's partition replica role, if defined
  std::unique_ptr<roles::partition::Replica> _partitionReplicaRole = nullptr;

  // Pointer to the instance's request manager role, if defined
  std::unique_ptr<roles::RequestManager> _requestManagerRole = nullptr;

  // A system-wide flag indicating that we should continue executing
  bool _continueRunning;

  // Storage for the partition index within the deployment corresponding to this instance
  size_t _partitionIdx;

  /// A map of registered functions, targets for an partition's initial function
  std::map<std::string, Task::taskFunction_t> _registeredFunctions;

  // Copy of the initial deployment configuration
  configuration::Deployment _deployment;

  // The instance manager to use for creating / relinquishing  replicas
  HiCR::InstanceManager *const _instanceManager;

  // Pointer to the HiCR RPC Engine
  HiCR::frontend::RPCEngine *_rpcEngine;

  // TaskR instance
  taskr::Runtime* const _taskr;

  // My instance Id
  const HiCR::Instance::instanceId_t _instanceId;

  // HiCR Tag to use for channel exchanges
  const HiCR::GlobalMemorySlot::tag_t _exchangeTag;

  // HiCR instance id associated to the instance that will coordinate the deployment
  HiCR::Instance::instanceId_t _deployerInstanceId;

  // For the deployer instance, this holds the ids of all the initially deployed instances
  std::set<HiCR::Instance::instanceId_t> _instanceSet;

  // Flag to indicate the engine has deployed correctly
  volatile bool _isDeployed;

}; // class Engine

} // namespace hLLM

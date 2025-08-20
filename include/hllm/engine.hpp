#pragma once

#include <ranges>
#include <unordered_map>

#include <hicr/core/exceptions.hpp>
#include <hicr/core/definitions.hpp>
#include <deployr/deployr.hpp>
#include <taskr/taskr.hpp>
#include <hicr/backends/hwloc/topologyManager.hpp>
#include <hicr/backends/boost/computeManager.hpp>
#include <hicr/backends/pthreads/computeManager.hpp>

#include "channel.hpp"
#include "task.hpp"

#define __HLLM_BROADCAST_CONFIGURATION "[hLLM] Broadcast Configuration"

namespace hLLM
{

class Engine final
{
  public:

  Engine(HiCR::InstanceManager             *instanceManager,
         HiCR::CommunicationManager        *communicationManager,
         HiCR::MemoryManager               *memoryManager,
         HiCR::frontend::RPCEngine         *rpcEngine,
         std::shared_ptr<HiCR::MemorySpace> channelMemSpace)
    : _deployr(instanceManager, communicationManager, memoryManager, rpcEngine),
      _instanceManager(instanceManager),
      _communicationManager(communicationManager),
      _memoryManager(memoryManager),
      _rpcEngine(rpcEngine),
      _channelMemSpace(channelMemSpace)
  {}

  ~Engine() {}

  __INLINE__ bool initialize()
  {
    // Registering entry point function in DeployR
    _deployr.registerFunction(_entryPointName, [this]() { entryPoint(); });

    // Register broadcast configuration function in hLLM
    auto broadcastConfigurationExecutionUnit = HiCR::backend::pthreads::ComputeManager::createExecutionUnit([this](void *) {
      // Serializing configuration
      const auto serializedConfiguration = _config.dump();

      // Returning serialized configuration
      _rpcEngine->submitReturnValue((void *)serializedConfiguration.c_str(), serializedConfiguration.size() + 1);
    });
    _rpcEngine->addRPCTarget(__HLLM_BROADCAST_CONFIGURATION, broadcastConfigurationExecutionUnit);

    // Initializing DeployR
    _deployr.initialize();

    // Return whether the current instance is root
    return _deployr.getCurrentInstance().isRootInstance();
  }

  __INLINE__ void deploy(const nlohmann::json &config)
  {
    // Storing configuration
    _config = config;

    // Deploying
    _deploy(_config);
  }

  __INLINE__ void run()
  {
    // Broadcast the config to all instances
    broadcastConfiguration();

    // Create channels
    createChannels();

    // Start computation
    _deployr.runInitialFunction();
  }

  __INLINE__ void abort() { _deployr.abort(); }

  /**
   * Registers a function that can be a target as initial function for one or more requested instances.
   * 
   * If a requested initial function is not registered by this function, deployment will fail.
   * 
   * @param[in] functionName The name of the function to register. This value will be used to match against the requested instance functions
   * @param[in] fc The actual function to register
   * 
   */
  __INLINE__ void registerFunction(const std::string &functionName, const function_t fc)
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

  __INLINE__ void terminate()
  {
    const auto &currentInstance = _deployr.getCurrentInstance();
    auto       &rootInstance    = _deployr.getRootInstance();

    // If I am the root instance, broadcast termination directly
    if (currentInstance.isRootInstance() == true) broadcastTermination();

    // If I am not the root instance, request the root to please broadcast termination
    if (currentInstance.isRootInstance() == false)
    {
      printf("[hLLM] Instance %lu requesting root instance %lu to finish execution.\n", currentInstance.getId(), rootInstance.getId());
      _rpcEngine->requestRPC(rootInstance, _requestStopRPCName);
    }
  }

  __INLINE__ void finalize()
  {
    // Finished execution, then finish deployment
    const auto &currentInstance = _deployr.getCurrentInstance();
    printf("[hLLM] Instance %lu finalizing deployr.\n", currentInstance.getId());
    _deployr.finalize();
  }

  private:

  //TODO: not all the instances need the configuration. Find a way to pass channel data to all the other instances
  __INLINE__ void broadcastConfiguration()
  {
    if (_deployr.getCurrentInstance().isRootInstance() == true)
    {
      // Listen for the incoming RPCs and return the hLLM configuration
      for (size_t i = 0; i < _deployr.getInstances().size() - 1; ++i) _rpcEngine->listen();
    }
    else
    {
      auto &rootInstance = _deployr.getRootInstance();

      // Request the root instance to send the configuration
      _rpcEngine->requestRPC(rootInstance, __HLLM_BROADCAST_CONFIGURATION);

      // Get the return value as a memory slot
      auto returnValue = _rpcEngine->getReturnValue(rootInstance);

      // Receive raw information from root
      std::string serializedConfiguration = static_cast<char *>(returnValue->getPointer());

      // Parse the configuration
      _config = nlohmann::json::parse(serializedConfiguration);

      // Free return value
      _rpcEngine->getMemoryManager()->freeLocalMemorySlot(returnValue);
    }
  }

  /**
   * Retrieves one of the channels creates during deployment.
   * 
   * If the provided name is not registered for this instance, the function will produce an exception.
   * 
   * @param[in] name The name of the channel to retrieve
   * @return The requested channel
   */
  __INLINE__ Channel &getChannel(const std::string &name)
  {
    if (_channels.contains(name) == false)
      HICR_THROW_LOGIC("Requested channel ('%s') is not defined for this instance ('%s')\n", name.c_str(), _deployr.getLocalInstance().getName().c_str());

    return _channels.at(name).operator*();
  }

  __INLINE__ nlohmann::json createDeployrRequest(const nlohmann::json &requestJs)
  {
    // Getting the instance vector
    const auto &instancesJs = hicr::json::getArray<nlohmann::json>(_config, "Instances");

    // Creating deployR initial request
    nlohmann::json deployrRequestJs;
    deployrRequestJs["Name"] = requestJs["Name"];

    // Adding a different host type and instance entries per instance requested
    auto hostTypeArray = nlohmann::json::array();
    auto instanceArray = nlohmann::json::array();
    for (const auto &instance : instancesJs)
    {
      const auto &instanceName = hicr::json::getString(instance, "Name");
      const auto &hostTypeName = instanceName + std::string(" Host Type");

      // Creating host type entry
      nlohmann::json hostType;
      hostType["Name"]                = instanceName + std::string(" Host Type");
      hostType["Topology"]["Devices"] = instance["Host Topology"];
      hostTypeArray.push_back(hostType);

      // Creating instance entry
      nlohmann::json newInstance;
      newInstance["Name"]      = instanceName;
      newInstance["Host Type"] = hostTypeName;
      newInstance["Function"]  = _entryPointName;
      instanceArray.push_back(newInstance);
    }
    deployrRequestJs["Host Types"]              = hostTypeArray;
    deployrRequestJs["Instances"]               = instanceArray;
    deployrRequestJs["Channels"]                = requestJs["Channels"];
    deployrRequestJs["Unbuffered Dependencies"] = requestJs["Unbuffered Dependencies"];

    return deployrRequestJs;
  }

  __INLINE__ void doLocalTermination()
  {
    // Stopping the execution of the current and new tasks
    _continueRunning = false;
    _taskr->forceTermination();
  }

  __INLINE__ void broadcastTermination()
  {
    if (_deployr.getCurrentInstance().isRootInstance() == false) HICR_THROW_RUNTIME("Only the root instance can broadcast termination");

    // Send a finalization signal to all other non-root instances
    const auto &currentInstance = _deployr.getCurrentInstance();
    auto        instances       = _deployr.getInstances();
    for (auto &instance : instances)
      if (instance->getId() != currentInstance.getId())
      {
        printf("[hLLM] Instance %lu sending stop RPC to instance %lu.\n", instance->getId(), currentInstance.getId());
        _rpcEngine->requestRPC(*instance, _stopRPCName);
      }

    // (Root) Executing local termination myself now
    doLocalTermination();
  }

  __INLINE__ void _deploy(const nlohmann::json &config)
  {
    // Create request json
    nlohmann::json requestJs;

    // Some information can be passed directly
    requestJs["Name"] = config["Name"];

    parseInstances(config, requestJs);

    // Getting the instance vector
    const auto &instancesJs = hicr::json::getArray<nlohmann::json>(config, "Instances");

    // Extract buffered and unbuffered dependencies
    const auto &[bufferedDependencies, unbufferedDependencies] = splitDependencies(instancesJs);

    // Add buffered and unbuffered dependencies
    auto bufferedDependenciesValues      = bufferedDependencies | std::views::values;
    requestJs["Channels"]                = std::vector<nlohmann::json>(bufferedDependenciesValues.begin(), bufferedDependenciesValues.end());
    requestJs["Unbuffered Dependencies"] = unbufferedDependencies;

    // Creating configuration for TaskR
    nlohmann::json taskrConfig;
    taskrConfig["Task Worker Inactivity Time (Ms)"] = 100;   // Suspend workers if a certain time of inactivity elapses
    taskrConfig["Task Suspend Interval Time (Ms)"]  = 100;   // Workers suspend for this time before checking back
    taskrConfig["Minimum Active Task Workers"]      = 1;     // Have at least one worker active at all times
    taskrConfig["Service Worker Count"]             = 1;     // Have one dedicated service workers at all times to listen for incoming messages
    taskrConfig["Make Task Workers Run Services"]   = false; // Workers will check for meta messages in between executions

    // Creating deployR request object
    auto deployrRequest = createDeployrRequest(requestJs);

    std::cout << deployrRequest << std::endl;
    // Adding hLLM as metadata in the request object
    deployrRequest["hLLM Configuration"] = _config;

    // Adding Deployr configuration
    // deployrRequest["DeployR Configuration"] = deployrConfig;

    // Adding taskr configuration
    deployrRequest["TaskR Configuration"] = taskrConfig;

    _config = deployrRequest;

    // Creating request object
    deployr::Request request(deployrRequest);

    // Deploying request
    try
    {
      _deployr.deploy(request);
    }
    catch (const std::exception &e)
    {
      fprintf(stderr, "[hLLM] Error: Failed to deploy. Reason:\n + '%s'", e.what());
      _deployr.abort();
    }
  }

  void createChannels()
  {
    // Create the channels
    auto channelId = 0;

    for (const auto &channelInfo : hicr::json::getArray<nlohmann::json>(_config, "Channels"))
    {
      // Getting channel's name
      const auto &name = hicr::json::getString(channelInfo, "Name");

      // Getting channel's producer
      const auto &producer = hicr::json::getString(channelInfo, "Producer");

      // Getting channel's consumer
      const auto &consumer = hicr::json::getString(channelInfo, "Consumer");

      // Getting buffer capacity (max token count)
      const auto &bufferCapacity = hicr::json::getNumber<uint64_t>(channelInfo, "Buffer Capacity (Tokens)");

      // Getting buffer size (bytes)
      const auto &bufferSize = hicr::json::getNumber<uint64_t>(channelInfo, "Buffer Size (Bytes)");

      // Get producer HiCR instance id
      const auto producerId         = _deployr.getDeployment().getPairings().at(producer);
      const auto producerInstanceId = _deployr.getDeployment().getHosts()[producerId].getInstanceId();

      // Getting consumer instance id
      const auto consumerId         = _deployr.getDeployment().getPairings().at(consumer);
      const auto consumerInstanceId = _deployr.getDeployment().getHosts()[consumerId].getInstanceId();

      // Creating channel object and increase channelId
      //TODO: for malleability check the channel type to be used
      const auto channelObject = createChannel(channelId++, name, producerInstanceId, consumerInstanceId, bufferCapacity, bufferSize);

      // Adding channel to map, only if defined
      if (channelObject != nullptr) _channels[name] = channelObject;
    }

    // _deployr.runInitialFunction();
    std::cout << "created channels" << std::endl;
  }

  void parseInstances(const nlohmann::json_abi_v3_11_2::json &config, nlohmann::json_abi_v3_11_2::json &requestJs)
  {
    ////// Instances information
    std::vector<nlohmann::json> newInstances;
    for (const auto &instance : config["Instances"])
    {
      nlohmann::json newInstance;
      newInstance["Name"]          = instance["Name"];
      newInstance["Host Topology"] = instance["Host Topology"];
      newInstance["Function"]      = _entryPointName;
      newInstances.push_back(newInstance);
    }
    requestJs["Instances"] = newInstances;
  }

  __INLINE__ std::pair<std::map<std::string, nlohmann::json>, std::map<std::string, nlohmann::json>> splitDependencies(const std::vector<nlohmann::json> &instancesJS)
  {
    std::map<std::string, nlohmann::json> bufferedDependencies;
    std::map<std::string, nlohmann::json> unbufferedDependencies;

    // Get all the consumers first since they hold information on the dependency type
    for (const auto &instance : instancesJS)
    {
      const auto &executionGraph = hicr::json::getArray<nlohmann::json>(instance, "Execution Graph");
      for (const auto &function : executionGraph)
      {
        const auto &inputs = hicr::json::getArray<nlohmann::json>(function, "Inputs");
        for (const auto &input : inputs)
        {
          const auto &inputName = hicr::json::getString(input, "Name");
          if (input["Type"] == "Buffered")
          {
            bufferedDependencies[inputName]             = input;
            bufferedDependencies[inputName]["Consumer"] = hicr::json::getString(instance, "Name");
          }
          else if (input["Type"] == "Unbuffered")
          {
            unbufferedDependencies[inputName]             = input;
            unbufferedDependencies[inputName]["Consumer"] = hicr::json::getString(instance, "Name");
          }
          else
          {
            HICR_THROW_RUNTIME("Input %s defined in instance %s type not supported: %s",
                               hicr::json::getString(input, "Name").c_str(),
                               hicr::json::getString(instance, "Name").c_str(),
                               hicr::json::getString(input, "Type").c_str());
          }
        }
      }
    }

    size_t bufferedProducersCount   = 0;
    size_t unbufferedProducersCount = 0;
    for (const auto &instance : instancesJS)
    {
      const auto &executionGraph = hicr::json::getArray<nlohmann::json>(instance, "Execution Graph");
      for (const auto &function : executionGraph)
      {
        const auto &outputs = hicr::json::getArray<nlohmann::json>(function, "Outputs");
        for (const auto &output : outputs)
        {
          if (bufferedDependencies.contains(output))
          {
            bufferedProducersCount++;
            if (bufferedDependencies[output].contains("Producer"))
            {
              HICR_THROW_RUNTIME("Set two producers for buffered dependency %s is not allowed. Producer: %s, tried to add %s",
                                 output,
                                 hicr::json::getString(bufferedDependencies[output], "Producer"),
                                 hicr::json::getString(instance, "Name").c_str());
            }
            bufferedDependencies[output]["Producer"] = hicr::json::getString(instance, "Name");
          }
          else if (unbufferedDependencies.contains(output))
          {
            unbufferedProducersCount++;
            if (unbufferedDependencies[output].contains("Producer"))
            {
              HICR_THROW_RUNTIME("Set two producers for buffered dependency %s is not allowed. Producer: %s, tried to add %s",
                                 output,
                                 hicr::json::getString(unbufferedDependencies[output], "Producer"),
                                 hicr::json::getString(instance, "Name").c_str());
            }
            unbufferedDependencies[output]["Producer"] = hicr::json::getString(instance, "Name");
          }
          else { HICR_THROW_RUNTIME("Producer %s defined in instance %s has no consumers defined", output.get<std::string>(), instance["Name"]); }
        }
      }
    }

    // Check set equality
    if (bufferedProducersCount != bufferedDependencies.size())
    {
      HICR_THROW_RUNTIME("Inconsistent buffered connections #producers: %zud #consumers: %zu", bufferedProducersCount, bufferedDependencies.size());
    }
    if (unbufferedProducersCount != unbufferedDependencies.size())
    {
      HICR_THROW_RUNTIME("Inconsistent unbuffered connections #producers: %zu #consumers: %zu", unbufferedProducersCount, unbufferedDependencies.size());
    }

    return {bufferedDependencies, unbufferedDependencies};
  }

#define SIZES_BUFFER_KEY 0
#define CONSUMER_COORDINATION_BUFFER_FOR_SIZES_KEY 3
#define CONSUMER_COORDINATION_BUFFER_FOR_PAYLOADS_KEY 4
#define CONSUMER_PAYLOAD_KEY 5
#define _CHANNEL_CREATION_ERROR 1

  /**
      * Function to create a variable-sized token locking channel between N producers and 1 consumer
      *
      * @note This is a collective operation. All instances must participate in this call, even if they don't play a producer or consumer role
      *
      * @param[in] channelTag The unique identifier for the channel. This tag should be unique for each channel
      * @param[in] channelName The name of the channel. This will be the identifier used to retrieve the channel
      * @param[in] producerIds Ids of the instances within the HiCR instance list to serve as producers
      * @param[in] consumerId Ids of the instance within the HiCR instance list to serve as consumer
      * @param[in] bufferCapacity The number of tokens that can be simultaneously held in the channel's buffer
      * @param[in] bufferSize The size (bytes) of the buffer.abort
      * @return A shared pointer of the newly created channel
      */
  __INLINE__ std::shared_ptr<Channel> createChannel(const size_t      channelTag,
                                                    const std::string channelName,
                                                    const size_t      producerId,
                                                    const size_t      consumerId,
                                                    const size_t      bufferCapacity,
                                                    const size_t      bufferSize)
  {
    // printf("Creating channel %lu '%s', producer count: %lu (first: %lu), consumer %lu, bufferCapacity: %lu, bufferSize: %lu\n", channelTag, name.c_str(), producerIds.size(), producerIds.size() > 0 ? producerIds[0] : 0, consumerId, bufferCapacity, bufferSize);

    // Interfaces for the channel
    std::shared_ptr<HiCR::channel::variableSize::MPSC::locking::Consumer> consumerInterface = nullptr;
    std::shared_ptr<HiCR::channel::variableSize::MPSC::locking::Producer> producerInterface = nullptr;

    // Getting my local instance index
    auto localInstanceId = _instanceManager->getCurrentInstance()->getId();

    // Checking if I am consumer
    bool isConsumer = consumerId == localInstanceId;

    // Checking if I am producer
    bool isProducer = producerId == localInstanceId;

    // Finding the first memory space to create our channels
    auto &bufferMemorySpace = _channelMemSpace;

    // Collection of memory slots to exchange and their keys
    std::vector<HiCR::CommunicationManager::globalKeyMemorySlotPair_t> memorySlotsToExchange;

    // Temporary storage for coordination buffer pointers
    std::shared_ptr<HiCR::LocalMemorySlot> localCoordinationBufferForSizes    = nullptr;
    std::shared_ptr<HiCR::LocalMemorySlot> localCoordinationBufferForPayloads = nullptr;

    ////// Pre-Exchange: Create local slots and register them for exchange

    // If I am consumer, create the consumer interface for the channel
    if (isConsumer == true)
    {
      // Getting required buffer sizes
      auto sizesBufferSize = HiCR::channel::variableSize::Base::getTokenBufferSize(sizeof(size_t), bufferCapacity);

      // Allocating sizes buffer as a local memory slot
      auto sizesBufferSlot = _rpcEngine->getMemoryManager()->allocateLocalMemorySlot(bufferMemorySpace, sizesBufferSize);

      // Allocating payload buffer as a local memory slot
      auto payloadBufferSlot = _rpcEngine->getMemoryManager()->allocateLocalMemorySlot(bufferMemorySpace, bufferSize);

      // Getting required buffer size
      auto coordinationBufferSize = HiCR::channel::variableSize::Base::getCoordinationBufferSize();

      // Allocating coordination buffer for internal message size metadata
      localCoordinationBufferForSizes = _rpcEngine->getMemoryManager()->allocateLocalMemorySlot(bufferMemorySpace, coordinationBufferSize);

      // Allocating coordination buffer for internal payload metadata
      localCoordinationBufferForPayloads = _rpcEngine->getMemoryManager()->allocateLocalMemorySlot(bufferMemorySpace, coordinationBufferSize);

      // Initializing coordination buffer (sets to zero the counters)
      HiCR::channel::variableSize::Base::initializeCoordinationBuffer(localCoordinationBufferForSizes);
      HiCR::channel::variableSize::Base::initializeCoordinationBuffer(localCoordinationBufferForPayloads);

      // Adding memory slots to exchange
      memorySlotsToExchange.push_back({SIZES_BUFFER_KEY, sizesBufferSlot});
      memorySlotsToExchange.push_back({CONSUMER_COORDINATION_BUFFER_FOR_SIZES_KEY, localCoordinationBufferForSizes});
      memorySlotsToExchange.push_back({CONSUMER_COORDINATION_BUFFER_FOR_PAYLOADS_KEY, localCoordinationBufferForPayloads});
      memorySlotsToExchange.push_back({CONSUMER_PAYLOAD_KEY, payloadBufferSlot});
    }

    // If I am producer, exchange relevant buffers
    if (isProducer == true)
    {
      // nothing to exchange for now, but this might change in the future if we support other types of channels
    }

    ////// Exchange: local memory slots to become global for them to be used by the remote end
    _rpcEngine->getCommunicationManager()->exchangeGlobalMemorySlots(channelTag, memorySlotsToExchange);

    // Synchronizing so that all actors have finished registering their global memory slots
    _rpcEngine->getCommunicationManager()->fence(channelTag);

    ///// Post exchange: create consumer/producer intefaces

    // If I am a consumer, create consumer interface now
    if (isConsumer == true)
    {
      // Obtaining the globally exchanged memory slots
      auto globalSizesBufferSlot                 = _rpcEngine->getCommunicationManager()->getGlobalMemorySlot(channelTag, SIZES_BUFFER_KEY);
      auto consumerCoordinationBufferForSizes    = _rpcEngine->getCommunicationManager()->getGlobalMemorySlot(channelTag, CONSUMER_COORDINATION_BUFFER_FOR_SIZES_KEY);
      auto consumerCoordinationBufferForPayloads = _rpcEngine->getCommunicationManager()->getGlobalMemorySlot(channelTag, CONSUMER_COORDINATION_BUFFER_FOR_PAYLOADS_KEY);
      auto globalPayloadBuffer                   = _rpcEngine->getCommunicationManager()->getGlobalMemorySlot(channelTag, CONSUMER_PAYLOAD_KEY);

      // Creating producer and consumer channels
      consumerInterface = std::make_shared<HiCR::channel::variableSize::MPSC::locking::Consumer>(*_rpcEngine->getCommunicationManager(),
                                                                                                 globalPayloadBuffer,   /* payloadBuffer */
                                                                                                 globalSizesBufferSlot, /* tokenSizeBuffer */
                                                                                                 localCoordinationBufferForSizes,
                                                                                                 localCoordinationBufferForPayloads,
                                                                                                 consumerCoordinationBufferForSizes,
                                                                                                 consumerCoordinationBufferForPayloads,
                                                                                                 bufferSize,
                                                                                                 bufferCapacity);
    }

    // If I am producer, create the producer interface for the channel
    if (isProducer == true)
    {
      // Getting required buffer size
      auto coordinationBufferSize = HiCR::channel::variableSize::Base::getCoordinationBufferSize();

      // Allocating sizes buffer as a local memory slot
      auto coordinationBufferForSizes = _rpcEngine->getMemoryManager()->allocateLocalMemorySlot(bufferMemorySpace, coordinationBufferSize);

      auto coordinationBufferForPayloads = _rpcEngine->getMemoryManager()->allocateLocalMemorySlot(bufferMemorySpace, coordinationBufferSize);

      auto sizeInfoBuffer = _rpcEngine->getMemoryManager()->allocateLocalMemorySlot(bufferMemorySpace, sizeof(size_t));

      // Initializing coordination buffers for message sizes and payloads (sets to zero the counters)
      HiCR::channel::variableSize::Base::initializeCoordinationBuffer(coordinationBufferForSizes);
      HiCR::channel::variableSize::Base::initializeCoordinationBuffer(coordinationBufferForPayloads);

      // Obtaining the globally exchanged memory slots
      auto sizesBuffer                           = _rpcEngine->getCommunicationManager()->getGlobalMemorySlot(channelTag, SIZES_BUFFER_KEY);
      auto consumerCoordinationBufferForSizes    = _rpcEngine->getCommunicationManager()->getGlobalMemorySlot(channelTag, CONSUMER_COORDINATION_BUFFER_FOR_SIZES_KEY);
      auto consumerCoordinationBufferForPayloads = _rpcEngine->getCommunicationManager()->getGlobalMemorySlot(channelTag, CONSUMER_COORDINATION_BUFFER_FOR_PAYLOADS_KEY);
      auto payloadBuffer                         = _rpcEngine->getCommunicationManager()->getGlobalMemorySlot(channelTag, CONSUMER_PAYLOAD_KEY);

      // Creating producer and consumer channels
      producerInterface = std::make_shared<HiCR::channel::variableSize::MPSC::locking::Producer>(*_rpcEngine->getCommunicationManager(),
                                                                                                 sizeInfoBuffer,
                                                                                                 payloadBuffer,
                                                                                                 sizesBuffer,
                                                                                                 coordinationBufferForSizes,
                                                                                                 coordinationBufferForPayloads,
                                                                                                 consumerCoordinationBufferForSizes,
                                                                                                 consumerCoordinationBufferForPayloads,
                                                                                                 bufferSize,
                                                                                                 sizeof(char),
                                                                                                 bufferCapacity);
    }

    return std::make_shared<Channel>(channelName, _rpcEngine->getMemoryManager(), bufferMemorySpace, consumerInterface, producerInterface);
  }

  //   // If I am consumer, create the consumer interface for the channel
  //   if (isConsumer == true && isProducer == false)
  //   {
  //     // Getting required buffer sizes
  //     auto sizesBufferSize = HiCR::channel::variableSize::Base::getTokenBufferSize(sizeof(size_t), bufferCapacity);

  //     // Allocating sizes buffer as a local memory slot
  //     auto sizesBufferSlot = _memoryManager->allocateLocalMemorySlot(_channelMemSpace, sizesBufferSize);

  //     // Allocating payload buffer as a local memory slot
  //     auto payloadBufferSlot = _memoryManager->allocateLocalMemorySlot(_channelMemSpace, bufferSize);

  //     // Getting required buffer size
  //     auto coordinationBufferSize = HiCR::channel::variableSize::Base::getCoordinationBufferSize();

  //     // Allocating coordination buffer for internal message size metadata
  //     auto coordinationBufferForSizes = _memoryManager->allocateLocalMemorySlot(_channelMemSpace, coordinationBufferSize);

  //     // Allocating coordination buffer for internal payload metadata
  //     auto coordinationBufferForPayloads = _memoryManager->allocateLocalMemorySlot(_channelMemSpace, coordinationBufferSize);

  //     // Initializing coordination buffer (sets to zero the counters)
  //     HiCR::channel::variableSize::Base::initializeCoordinationBuffer(coordinationBufferForSizes);

  //     HiCR::channel::variableSize::Base::initializeCoordinationBuffer(coordinationBufferForPayloads);

  //     // Exchanging local memory slots to become global for them to be used by the remote end
  //     _communicationManager->exchangeGlobalMemorySlots(channelTag,
  //                                                      {{SIZES_BUFFER_KEY, sizesBufferSlot},
  //                                                       {CONSUMER_COORDINATION_BUFFER_FOR_SIZES_KEY, coordinationBufferForSizes},
  //                                                       {CONSUMER_COORDINATION_BUFFER_FOR_PAYLOADS_KEY, coordinationBufferForPayloads},
  //                                                       {CONSUMER_PAYLOAD_KEY, payloadBufferSlot}});

  //     // Synchronizing so that all actors have finished registering their global memory slots
  //     _communicationManager->fence(channelTag);

  //     // Obtaining the globally exchanged memory slots
  //     auto globalSizesBufferSlot                 = _communicationManager->getGlobalMemorySlot(channelTag, SIZES_BUFFER_KEY);
  //     auto consumerCoordinationBufferForSizes    = _communicationManager->getGlobalMemorySlot(channelTag, CONSUMER_COORDINATION_BUFFER_FOR_SIZES_KEY);
  //     auto consumerCoordinationBufferForPayloads = _communicationManager->getGlobalMemorySlot(channelTag, CONSUMER_COORDINATION_BUFFER_FOR_PAYLOADS_KEY);
  //     auto globalPayloadBuffer                   = _communicationManager->getGlobalMemorySlot(channelTag, CONSUMER_PAYLOAD_KEY);

  //     // Creating producer and consumer channels
  //     consumerInterface = std::make_shared<HiCR::channel::variableSize::MPSC::locking::Consumer>(*_communicationManager,
  //                                                                                                globalPayloadBuffer,   /* payloadBuffer */
  //                                                                                                globalSizesBufferSlot, /* tokenSizeBuffer */
  //                                                                                                coordinationBufferForSizes,
  //                                                                                                coordinationBufferForPayloads,
  //                                                                                                consumerCoordinationBufferForSizes,
  //                                                                                                consumerCoordinationBufferForPayloads,
  //                                                                                                bufferSize,
  //                                                                                                bufferCapacity);
  //   }

  //   // If I am producer, create the producer interface for the channel
  //   if (isProducer == true && isConsumer == false)
  //   {
  //     // Getting required buffer size
  //     auto coordinationBufferSize = HiCR::channel::variableSize::Base::getCoordinationBufferSize();

  //     // Allocating sizes buffer as a local memory slot
  //     auto coordinationBufferForSizes = _memoryManager->allocateLocalMemorySlot(_channelMemSpace, coordinationBufferSize);

  //     auto coordinationBufferForPayloads = _memoryManager->allocateLocalMemorySlot(_channelMemSpace, coordinationBufferSize);

  //     auto sizeInfoBuffer = _memoryManager->allocateLocalMemorySlot(_channelMemSpace, sizeof(size_t));

  //     // Initializing coordination buffers for message sizes and payloads (sets to zero the counters)
  //     HiCR::channel::variableSize::Base::initializeCoordinationBuffer(coordinationBufferForSizes);
  //     HiCR::channel::variableSize::Base::initializeCoordinationBuffer(coordinationBufferForPayloads);

  //     // Exchanging local memory slots to become global for them to be used by the remote end
  //     _communicationManager->exchangeGlobalMemorySlots(channelTag, {});

  //     // Synchronizing so that all actors have finished registering their global memory slots
  //     _communicationManager->fence(channelTag);

  //     // Obtaining the globally exchanged memory slots
  //     auto sizesBuffer                           = _communicationManager->getGlobalMemorySlot(channelTag, SIZES_BUFFER_KEY);
  //     auto consumerCoordinationBufferForSizes    = _communicationManager->getGlobalMemorySlot(channelTag, CONSUMER_COORDINATION_BUFFER_FOR_SIZES_KEY);
  //     auto consumerCoordinationBufferForPayloads = _communicationManager->getGlobalMemorySlot(channelTag, CONSUMER_COORDINATION_BUFFER_FOR_PAYLOADS_KEY);
  //     auto payloadBuffer                         = _communicationManager->getGlobalMemorySlot(channelTag, CONSUMER_PAYLOAD_KEY);

  //     // Creating producer and consumer channels
  //     producerInterface = std::make_shared<HiCR::channel::variableSize::MPSC::locking::Producer>(*_communicationManager,
  //                                                                                                sizeInfoBuffer,
  //                                                                                                payloadBuffer,
  //                                                                                                sizesBuffer,
  //                                                                                                coordinationBufferForSizes,
  //                                                                                                coordinationBufferForPayloads,
  //                                                                                                consumerCoordinationBufferForSizes,
  //                                                                                                consumerCoordinationBufferForPayloads,
  //                                                                                                bufferSize,
  //                                                                                                sizeof(char),
  //                                                                                                bufferCapacity);
  //   }

  //   if (isConsumer == true && isProducer == true) {}

  //   // If I am not involved in this channel (neither consumer or producer, simply participate in the exchange)
  //   if (isConsumer == false && isProducer == false)
  //   {
  //     // Exchanging local memory slots to become global for them to be used by the remote end
  //     _communicationManager->exchangeGlobalMemorySlots(channelTag, {});

  //     // Synchronizing so that all actors have finished registering their global memory slots
  //     _communicationManager->fence(channelTag);
  //   }

  //   return std::make_shared<Channel>(channelName, _memoryManager, _channelMemSpace, consumerInterface, producerInterface);
  // }

  __INLINE__ void entryPoint()
  {
    // Starting a new deployment
    _continueRunning = true;

    // Registering finalization function (for root to execute)
    _rpcEngine->addRPCTarget(_stopRPCName, HiCR::backend::pthreads::ComputeManager::createExecutionUnit([this](void *) {
                               printf("[hLLM] Received finalization RPC\n");
                               doLocalTermination();
                             }));

    // Registering finalization request function (for non-root to request roots to end the entire execution)
    _rpcEngine->addRPCTarget(_requestStopRPCName, HiCR::backend::pthreads::ComputeManager::createExecutionUnit([this](void *) {
                               const auto &currentInstance = _deployr.getCurrentInstance();
                               printf("[hLLM] Instance (%lu) - received RPC request to finalize\n", currentInstance.getId());
                               broadcastTermination();
                             }));

    // Getting my instance name after deployment
    const auto &myInstance     = _deployr.getLocalInstance();
    const auto &myInstanceName = myInstance.getName();

    // Getting deployment
    const auto &deployment = _deployr.getDeployment();

    // Getting request from deployment
    const auto &request = deployment.getRequest();

    // Getting request configuration from the request ibject
    const auto &requestJs = request.serialize();

    // Getting LLM engine configuration from request configuration
    _config = hicr::json::getObject(requestJs, "hLLM Configuration");

    // Getting unbuffered dependencies
    const auto &unbufferedDependencies = hicr::json::getObject(requestJs, "Unbuffered Dependencies");

    // Creating HWloc topology object
    hwloc_topology_t topology;

    // initializing hwloc topology object
    hwloc_topology_init(&topology);

    // Initializing HWLoc-based host (CPU) topology manager
    HiCR::backend::hwloc::TopologyManager tm(&topology);

    // Asking backend to check the available devices
    const auto t = tm.queryTopology();

    // Compute resources to use
    HiCR::Device::computeResourceList_t computeResources;

    // Considering all compute devices of NUMA Domain type
    const auto &devices = t.getDevices();
    for (const auto &device : devices)
      if (device->getType() == "NUMA Domain")
      {
        // Getting compute resources in this device
        auto crs = device->getComputeResourceList();

        // Adding it to the list
        for (const auto &cr : crs) computeResources.push_back(cr);
      }

    // Freeing up memory
    hwloc_topology_destroy(topology);

    // Creating taskr object
    const auto &taskRConfig = hicr::json::getObject(requestJs, "TaskR Configuration");
    _taskr                  = std::make_unique<taskr::Runtime>(&_boostComputeManager, &_pthreadsComputeManager, computeResources, taskRConfig);

    // Initializing TaskR
    _taskr->initialize();

    // Finding instance information on the configuration
    const auto    &instancesJs = hicr::json::getArray<nlohmann::json>(_config, "Instances");
    nlohmann::json myInstanceConfig;
    for (const auto &instance : instancesJs)
    {
      const auto &instanceName = hicr::json::getString(instance, "Name");
      if (instanceName == myInstanceName) myInstanceConfig = instance;
    }

    // // Initialize unbuffered dependencies access map
    // for (const auto &dependency : unbufferedDependencies)
    // {
    //   const auto &dependencyName = hicr::json::getString(dependency, "Name");
    //   const auto &producer       = hicr::json::getString(dependency, "Producer");
    //   const auto &consumer       = hicr::json::getString(dependency, "Consumer");

    //   if (consumer == myInstanceName || producer == myInstanceName) { _unbufferedMemorySlotsAccessMap[dependencyName] = nullptr; }
    // }

    // Getting relevant information
    const auto &executionGraph = hicr::json::getArray<nlohmann::json>(myInstanceConfig, "Execution Graph");

    // Creating general TaskR function for all execution graph functions
    _taskrFunction = std::make_unique<taskr::Function>([this](taskr::Task *task) { runTaskRFunction(task); });

    // Checking the execution graph functions have been registered
    taskr::label_t taskrLabelCounter = 0;
    for (const auto &functionJs : executionGraph)
    {
      const auto &fcName = hicr::json::getString(functionJs, "Name");

      // Checking the requested function was registered
      if (_registeredFunctions.contains(fcName) == false)
      {
        fprintf(stderr, "The requested function name '%s' is not registered. Please register it before running the hLLM.\n", fcName.c_str());
        abort();
      }

      // Getting inputs and outputs
      std::vector<std::pair<std::string, std::string>> inputs;
      const auto                                      &inputsJs = hicr::json::getArray<nlohmann::json>(functionJs, "Inputs");
      for (const auto &inputJs : inputsJs)
      {
        const auto &inputName = hicr::json::getString(inputJs, "Name");
        if (unbufferedDependencies.contains(inputName)) { inputs.push_back({inputName, "Unbuffered"}); }
        else { inputs.push_back({inputName, "Buffered"}); }
      }

      std::vector<std::pair<std::string, std::string>> outputs;
      const auto                                      &outputsJs = hicr::json::getArray<std::string>(functionJs, "Outputs");
      for (const auto &outputName : outputsJs)
      {
        if (unbufferedDependencies.contains(outputName)) { outputs.push_back({outputName, "Unbuffered"}); }
        else { outputs.push_back({outputName, "Buffered"}); }
      }

      // Getting label for taskr function
      const auto taskLabel = taskrLabelCounter++;

      // Creating taskr Task corresponding to the LLM engine task
      auto taskrTask = std::make_unique<taskr::Task>(_taskrFunction.get());
      taskrTask->setLabel(taskLabel);

      // Getting function pointer
      const auto &fc = _registeredFunctions[fcName];

      // Creating Engine Task object
      auto newTask = std::make_shared<hLLM::Task>(fcName, fc, inputs, outputs, std::move(taskrTask));

      // Adding task to the label->Task map
      _taskLabelMap.insert({taskLabel, newTask});

      // Adding task to the function name->Task map
      _taskNameMap.insert({fcName, newTask});

      // Adding task to TaskR itself
      _taskr->addTask(newTask->getTaskRTask());
    }

    // Instruct TaskR to re-add suspended tasks
    _taskr->setTaskCallbackHandler(HiCR::tasking::Task::callback_t::onTaskSuspend, [&](taskr::Task *task) { _taskr->resumeTask(task); });

    // Setting service to listen for incoming administrative messages
    std::function<void()> RPCListeningService = [this]() {
      if (_rpcEngine->hasPendingRPCs()) _rpcEngine->listen();
    };
    _taskr->addService(&RPCListeningService);

    // Running TaskR
    _taskr->run();
    _taskr->await();
    _taskr->finalize();

    printf("[hLLM] Instance '%s' TaskR Stopped\n", myInstanceName.c_str());
  }

  __INLINE__ static bool isValueNull(const std::unordered_map<std::string, Channel::token_t *> &collection, const std::string &key) { return collection.at(key) == nullptr; }

  __INLINE__ void runTaskRFunction(taskr::Task *task)
  {
    const auto &taskLabel    = task->getLabel();
    const auto &taskObject   = _taskLabelMap.at(taskLabel);
    const auto &function     = taskObject->getFunction();
    const auto &functionName = taskObject->getName();
    const auto &inputs       = taskObject->getInputs();
    const auto &outputs      = taskObject->getOutputs();

    // Resolving pointers to all the required input channels
    // and gather unbuffered inputs
    std::map<std::string, Channel *> inputChannels;
    std::vector<std::string>         unbufferedInputs;
    for (const auto &[input, type] : inputs)
    {
      if (type == "Buffered") { inputChannels[input] = &getChannel(input); }
      else { unbufferedInputs.push_back(input); }
    }

    // Resolving pointers to all the required output channels
    // and gather unbuffered outputs
    std::map<std::string, Channel *> outputChannels;
    std::vector<std::string>         unbufferedOutputs;
    for (const auto &[output, type] : outputs)
    {
      if (type == "Buffered") { outputChannels[output] = &getChannel(output); }
      else { unbufferedOutputs.push_back(output); }
    }

    // Function to check for pending operations
    auto pendingOperationsCheck = [&]() {
      // If execution must stop now, return true to go back to the task and finish it
      if (_continueRunning == false) return true;

      // The task is not ready if any of its inputs are not yet available
      for (const auto &[_, inputChannel] : inputChannels)
        if (inputChannel->isEmpty()) return false;

      // The task is not ready if any of its output channels are still full
      for (const auto &[_, outputChannel] : outputChannels)
        if (outputChannel->isFull()) return false;

      // // The task is not ready if any of the unbuffered inputs are not available
      // for (const auto &input : unbufferedInputs)
      // {
      //   if (isValueNull(_unbufferedMemorySlotsAccessMap, input) == true) return false;
      // }

      // // The task is not ready if any of the unbuffered outputs are available to be consumed
      // for (const auto &output : unbufferedOutputs)
      //   if (isValueNull(_unbufferedMemorySlotsAccessMap, output) == false) return false;

      // All dependencies are satisfied, enable this task for execution
      return true;
    };

    // Initiate infinite loop
    while (_continueRunning)
    {
      // Adding task dependencies
      task->addPendingOperation(pendingOperationsCheck);

      // Suspend execution until all dependencies are met
      task->suspend();

      // Another exit point (the more the better)
      if (_continueRunning == false) break;

      // Inputs dependencies must be satisfied by now; getting them values
      for (const auto &[input, type] : inputs)
      {
        Channel::token_t token;

        // if (type == "Buffered")
        // {
        // Peeking token from input channel
        token = inputChannels[input]->peek();
        // }
        // else
        // {
        //   // Get from unbuffered memory
        //   token = *_unbufferedMemorySlotsAccessMap[input];

        //   // Set to null the entry in the unbuffered map so new task will wait for the input to become available
        //   _unbufferedMemorySlotsAccessMap[input] = nullptr;
        // }
        // Pushing token onto the task
        taskObject->setInput(input, token);
      }

      // Actually run the function now
      // printf("Running Task: %lu - Function: (%s)\n", taskLabel, functionName.c_str());
      function(taskObject.get());

      // Another exit point (the more the better)
      if (_continueRunning == false) break;

      // Checking input messages were properly consumed and popping the used token
      for (const auto &[input, type] : inputs)
      {
        if (taskObject->hasInput(input) == true)
        {
          fprintf(stderr, "Function '%s' has not consumed required input '%s'\n", functionName.c_str(), input.c_str());
          abort();
        }

        // Popping consumed channel from channel
        // if (type == "Buffered") {
        // }
        inputChannels[input]->pop();
        //  }
      }

      // Validating and pushing output messages
      for (const auto &[output, type] : outputs)
      {
        // Checking if output has been produced by the task
        if (taskObject->hasOutput(output) == false)
        {
          fprintf(stderr, "Function '%s' has not pushed required output '%s'\n", functionName.c_str(), output.c_str());
          abort();
        }

        // Now getting output token
        const auto outputToken = taskObject->getOutput(output);

        // if (type == "Buffered")
        // {
        // Pushing token onto channel
        if (outputChannels[output]->push(outputToken.buffer, outputToken.size) == false) { HICR_THROW_RUNTIME("Channel full"); }

        // }
        // else
        // {
        //   // Set token in unbuffered memory slots
        //   if (_unbufferedMemorySlotsAccessMap.at(output) != nullptr)
        //   {
        //     HICR_THROW_RUNTIME("Output %s not null %s", output, (char *)_unbufferedMemorySlotsAccessMap[output]->buffer);
        //   }
        //   _unbufferedMemorySlotsPool[output]      = outputToken;
        //   _unbufferedMemorySlotsAccessMap[output] = &_unbufferedMemorySlotsPool.at(output);
        // }
      }

      // Clearing output token maps
      taskObject->clearOutputs();
    }
  }

  // A system-wide flag indicating that we should continue executing
  bool _continueRunning;

  std::unique_ptr<taskr::Function> _taskrFunction;

  /// A map of registered functions, targets for an instance's initial function
  std::map<std::string, function_t> _registeredFunctions;

  // Copy of the initial configuration file
  nlohmann::json _config;

  // Name of the hLLM entry point after deployment
  const std::string _entryPointName     = "__hLLM Entry Point__";
  const std::string _stopRPCName        = "__hLLM Stop__";
  const std::string _requestStopRPCName = "__LLM Request Engine Stop__";

  // DeployR instance
  deployr::DeployR _deployr;

  // TaskR instance
  std::unique_ptr<taskr::Runtime> _taskr;

  // Map relating task labels to their hLLM task
  std::map<taskr::label_t, std::shared_ptr<hLLM::Task>> _taskLabelMap;

  // Map relating function name to their hLLM task
  std::map<std::string, std::shared_ptr<hLLM::Task>> _taskNameMap;

  HiCR::InstanceManager *const      _instanceManager;
  HiCR::CommunicationManager *const _communicationManager;
  HiCR::MemoryManager *const        _memoryManager;
  // Pointer to the HiCR RPC Engine
  HiCR::frontend::RPCEngine *_rpcEngine;

  std::shared_ptr<HiCR::MemorySpace> _channelMemSpace;

  ///// HiCR Objects for TaskR

  // Initializing Boost-based compute manager to instantiate suspendable coroutines
  HiCR::backend::boost::ComputeManager _boostComputeManager;

  // Initializing Pthreads-based compute manager to instantiate processing units
  HiCR::backend::pthreads::ComputeManager _pthreadsComputeManager;

  /// Map of created channels
  std::map<std::string, std::shared_ptr<Channel>> _channels;

  // // Map storing pointers to access unbuffered memory slots
  // std::unordered_map<std::string, Channel::token_t *> _unbufferedMemorySlotsAccessMap;

  // // Map storing pointers to access unbuffered memory slots
  // std::unordered_map<std::string, Channel::token_t> _unbufferedMemorySlotsPool;
}; // class Engine

} // namespace hLLM
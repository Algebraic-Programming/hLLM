#pragma once

#include "partition.hpp"
#include "edge.hpp"
#include <vector>
#include <memory>
#include <string>
#include <nlohmann_json/parser.hpp>
#include <hicr/core/definitions.hpp>

namespace hLLM::configuration
{

class Deployment final
{
  public:

  /**
   * Heartbeat is a runtime check mechanism to verify neighbors and replicas are still alive. 
   * Every 'interval' milliseconds, the coordinator sends a heartbeat message to its own replicas and vice-versa
   * If the 'tolerance' interval passes without a heartbeat, then the other side is deemed non-responsive
   * Recovery or fail tolerance mechanisms can be activated upon this happening
   */ 
  struct heartbeat_t
  {
    // Whether to enable the heartbeat service or not
    bool enabled;

    // Heartbeat interval (every how many ms do we send a heartbeat message to replicas and neighboring coordinators)
    size_t interval;

    // Tolerance is how long to wait before alerting that we haven't received a heartbeat
    size_t tolerance;
  };

  /**
   * The control buffer is the edge between coordinators and replicas to exchange control messages.
   * Control messages are not related to the application, but instead to managing deployment aspects
   * (e.g., load balancing, fail detection, migration)
   */ 
  static constexpr size_t _defaultControlBufferCapacity = 256;
  static constexpr size_t _defaultControlBufferSize = _defaultControlBufferCapacity * 1024;
  struct controlBuffer_t
  {
    // Capacity (the maximum number of pending tokens in the buffer)
    size_t capacity = _defaultControlBufferCapacity;

    // Size  (the size in bytes that the buffer can hold)
    size_t size = _defaultControlBufferSize;

    // HiCR-specific objects to create the control buffers. These are to be set at runtime
    HiCR::CommunicationManager* communicationManager  = nullptr;
    HiCR::MemoryManager* memoryManager  = nullptr;
    std::shared_ptr<HiCR::MemorySpace> memorySpace  = nullptr;
  };

  struct settings_t
  {
    heartbeat_t heartbeat;
    controlBuffer_t controlBuffer;
  };

  Deployment(const std::string& name) : _name(name) {};
  Deployment(const nlohmann::json& js) { deserialize(js); };
  Deployment() = default;
  ~Deployment() = default;

  __INLINE__ void setName(const std::string& name) { _name = name; }
  __INLINE__ void addPartition(const std::shared_ptr<Partition> partition) { _partitions.push_back(partition); }
  __INLINE__ void addChannel(const std::shared_ptr<Edge> channel) { _edges.push_back(channel); }

  [[nodiscard]] __INLINE__ std::string getName() const { return _name; }
  [[nodiscard]] __INLINE__ auto& getPartitions() const { return _partitions; }
  [[nodiscard]] __INLINE__ auto& getEdges() const { return _edges; }
  [[nodiscard]] __INLINE__ auto& getHeartbeat() const { return _settings.heartbeat; }
  [[nodiscard]] __INLINE__ auto& getControlBuffer() { return _settings.controlBuffer; }
  [[nodiscard]] __INLINE__ const auto& getControlBufferConst() const { return _settings.controlBuffer; }

  [[nodiscard]] __INLINE__ nlohmann::json serialize() const 
  {
    nlohmann::json js;

    js["Name"] = _name;

    std::vector<nlohmann::json> partitionsJs;
    for (const auto& p : _partitions) partitionsJs.push_back(p->serialize());
    js["Partitions"] = partitionsJs;

    std::vector<nlohmann::json> edgesJs;
    for (const auto& e : _edges) edgesJs.push_back(e->serialize());
    js["Edges"] = edgesJs;

    ////////////////////// Parsing settings
    auto settings = std::map<std::string, nlohmann::json>();
    
    // Heartbeat
    auto heartbeat = std::map<std::string, nlohmann::json>();
    heartbeat["Enabled"] = _settings.heartbeat.enabled;
    heartbeat["Interval"] = _settings.heartbeat.interval;
    heartbeat["Tolerance"] = _settings.heartbeat.tolerance;
    settings["Heartbeat"] = heartbeat;

    // Control Buffer
    auto controlBuffer = std::map<std::string, nlohmann::json>();
    controlBuffer["Capacity"] = _settings.controlBuffer.capacity;
    controlBuffer["Size"] = _settings.controlBuffer.size;
    settings["Control Buffer"] = controlBuffer;

    js["Settings"] = settings;

    return js;
  }

  __INLINE__ void deserialize (const nlohmann::json& js)
  {
    // Clearing objects
    _partitions.clear();
    _edges.clear();

    _name = hicr::json::getString(js, "Name");

    const auto& partitions = hicr::json::getArray<nlohmann::json>(js, "Partitions");
    for (const auto& p : partitions) _partitions.push_back(std::make_shared<Partition>(p));

    const auto& edges = hicr::json::getArray<nlohmann::json>(js, "Edges");
    for (const auto& e : edges) _edges.push_back(std::make_shared<Edge>(e));

    // Getting settings
    nlohmann::json settingsJs = hicr::json::getObject(js, "Settings");

    nlohmann::json heartbeatJs = hicr::json::getObject(settingsJs, "Heartbeat");
    _settings.heartbeat.enabled = hicr::json::getBoolean(heartbeatJs, "Enabled");
    _settings.heartbeat.interval = hicr::json::getNumber<size_t>(heartbeatJs, "Interval");
    _settings.heartbeat.tolerance = hicr::json::getNumber<size_t>(heartbeatJs, "Tolerance");

    nlohmann::json controlBufferJs = hicr::json::getObject(settingsJs, "Control Buffer");
    _settings.controlBuffer.capacity = hicr::json::getNumber<size_t>(controlBufferJs, "Capacity");
    _settings.controlBuffer.size = hicr::json::getNumber<size_t>(controlBufferJs, "Size");
  }
  
  // Includes all kinds of sanity checks relevant to a deployment
  __INLINE__ void verify() const
  {
    // Getting all partition's edges in a set
    std::set<std::string> partitionNameSet;
    for (const auto& partition : _partitions) partitionNameSet.insert(partition->getName());

    // For each edge, remember which partition is the consumer and which is the producer
    std::map<std::string, std::string> edgeConsumerPartitionMap;
    std::map<std::string, std::string> edgeProducerPartitionMap;
    for (const auto& partition : _partitions)
      for (const auto& task : partition->getTasks())
      {
        for (const auto& edge : task->getInputs())
        {
          // Check that the edge is not used as input more than once. All edges must be 1-to-1
          if (edgeConsumerPartitionMap.contains(edge)) HICR_THROW_LOGIC("Deployment specifies edge '%s' used as input more than once\n", edge.c_str());
          edgeConsumerPartitionMap[edge] = partition->getName();
        } 

        for (const auto& edge : task->getOutputs())
        {
          // Check that the edge is not used as input more than once. All edges must be 1-to-1
          if (edgeProducerPartitionMap.contains(edge)) HICR_THROW_LOGIC("Deployment specifies edge '%s' used as output more than once\n", edge.c_str());
          edgeProducerPartitionMap[edge] = partition->getName();
        } 
      }

    // Check whether all edges have consumer+producer partitions that do exist
    for (const auto& edge : _edges)
    {
      if (edgeConsumerPartitionMap.contains(edge->getName()) == false || edgeProducerPartitionMap.contains(edge->getName()) == false) 
        HICR_THROW_LOGIC("Deployment specifies edge '%s' but it is either not used as input or output (or neither)\n", edge->getName().c_str());

       // Setting producer and consumer partitions for the edge
       edge->setProducer(edgeProducerPartitionMap[edge->getName()]);
       edge->setConsumer(edgeConsumerPartitionMap[edge->getName()]);
    }
  }

  private:

  std::string _name;
  std::vector<std::shared_ptr<Partition>> _partitions;
  std::vector<std::shared_ptr<Edge>> _edges;
  settings_t _settings;


}; // class Deployment

} // namespace hLLM::configuration
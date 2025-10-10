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

   /**
   * User interface relates to how the user connects to hLLM, feeds a prompt and gets a response
   */ 
  struct userInterface_t
  {
    // Indicates which input in the execution graph is fed by the user
    std::string input;

    // Indicates which output in the execution graph is fed to the user
    std::string output;
  };

  struct settings_t
  {
    heartbeat_t heartbeat;
    controlBuffer_t controlBuffer;
    userInterface_t userInterface;
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

    // User Interface
    auto userInterface = std::map<std::string, nlohmann::json>();
    userInterface["Input"] = _settings.userInterface.input;
    userInterface["Output"] = _settings.userInterface.output;
    settings["User Interface"] = userInterface;

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

    nlohmann::json userInterfaceJs = hicr::json::getObject(settingsJs, "User Interface");
    _settings.userInterface.input = hicr::json::getString(userInterfaceJs, "Input");
    _settings.userInterface.output = hicr::json::getString(userInterfaceJs, "Output");
  }
  
  // Includes all kinds of sanity checks relevant to a deployment
  __INLINE__ void verify() const
  {
    // Getting all partition's edges in a set
    std::set<std::string> partitionNameSet;
    for (const auto& partition : _partitions) partitionNameSet.insert(partition->getName());

    // Getting a list of all edges to check the tasks specifying inputs/outputs actually refer to one of these, or user interface
    std::set<std::string> edgeNameSet;
    
    // First, the user interface input/outputs are counted as edges for the purposes of this check
    edgeNameSet.insert(_settings.userInterface.input);
    edgeNameSet.insert(_settings.userInterface.output);
    for (const auto& edge : _edges)
    {
      const auto& edgeName = edge->getName();
      if (edgeNameSet.contains(edgeName)) HICR_THROW_LOGIC("Deployment specifies repeated edge or user input '%s'\n", edgeName.c_str());
      edgeNameSet.insert(edgeName);
    }

    // For each input / output, remember which partition is its consumer / producer
    std::map<std::string, std::string> inputPartitionMap;
    std::map<std::string, std::string> outputPartitionMap;
    for (const auto& partition : _partitions)
      for (const auto& task : partition->getTasks())
      {
        // Make sure all tasks have at least one input
        if (task->getInputs().size() == 0) HICR_THROW_LOGIC("Deployment specifies task in partition '%s' with function name '%s' without any inputs\n", partition->getName().c_str(), task->getFunctionName().c_str());

        // Check that the edge is not used as input more than once. All edges must be 1-to-1
        for (const auto& input : task->getInputs())
        {
          if (inputPartitionMap.contains(input)) HICR_THROW_LOGIC("Deployment specifies input '%s' used more than once\n", input.c_str());
          if (edgeNameSet.contains(input) == false) HICR_THROW_LOGIC("Deployment specifies task '%s' with an undefined input '%s'\n", task->getFunctionName(), input.c_str());
          inputPartitionMap[input] = partition->getName();
        } 

        // Make sure all tasks have at least one output
        if (task->getOutputs().size() == 0) HICR_THROW_LOGIC("Deployment specifies task in partition '%'' with function name '%s' without any outputs\n", partition->getName().c_str(), task->getFunctionName().c_str());

        // Check that the output is not used as input more than once. All edges must be 1-to-1
        for (const auto& output : task->getOutputs())
        {
          if (outputPartitionMap.contains(output)) HICR_THROW_LOGIC("Deployment specifies output '%s' used more than once\n", output.c_str());
          if (edgeNameSet.contains(output) == false) HICR_THROW_LOGIC("Deployment specifies task '%s' with an undefined output '%s'\n", task->getFunctionName(), output.c_str());
          outputPartitionMap[output] = partition->getName();
        } 
      }

    // Check whether all edges have consumer+producer partitions that do exist
    for (const auto& edge : _edges)
    {
      if (inputPartitionMap.contains(edge->getName()) == false || outputPartitionMap.contains(edge->getName()) == false) 
        HICR_THROW_LOGIC("Deployment specifies edge '%s' but it is either not used as input or output (or neither)\n", edge->getName().c_str());

       // Setting producer and consumer partitions for the edge
       edge->setProducer(outputPartitionMap[edge->getName()]);
       edge->setConsumer(inputPartitionMap[edge->getName()]);
    }
  }

  private:

  std::string _name;
  std::vector<std::shared_ptr<Partition>> _partitions;
  std::vector<std::shared_ptr<Edge>> _edges;
  settings_t _settings;


}; // class Deployment

} // namespace hLLM::configuration
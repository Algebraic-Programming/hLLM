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
  }
  
  // Includes all kinds of sanity checks relevant to a deployment
  __INLINE__ void verify() const
  {
    // Getting all partition's edges in a set
    std::set<std::string> partitionNameSet;
    for (const auto& partition : _partitions) partitionNameSet.insert(partition->getName());

    // Used edges name set, also checking whether they are used as input or output
    std::set<std::string> inputEdgeNameSet;
    std::set<std::string> outputEdgeNameSet;
    for (const auto& partition : _partitions)
      for (const auto& task : partition->getTasks())
      {
        for (const auto& edge : task->getInputs())  inputEdgeNameSet.insert(edge);
        for (const auto& edge : task->getOutputs()) outputEdgeNameSet.insert(edge);
      }

    // Check whether all edges have consumer+producer partitions that do exist
    for (const auto& edge : _edges)
    {
      if (partitionNameSet.contains(edge->getConsumer()) == false) HICR_THROW_LOGIC("Deployment specifies edge '%s' with consumer '%s', but the latter is not defined\n", edge->getName().c_str(), edge->getConsumer().c_str());
      if (partitionNameSet.contains(edge->getProducer()) == false) HICR_THROW_LOGIC("Deployment specifies edge '%s' with producer '%s', but the latter is not defined\n", edge->getName().c_str(), edge->getProducer().c_str());
      if (inputEdgeNameSet.contains(edge->getName()) == false || outputEdgeNameSet.contains(edge->getName()) == false) HICR_THROW_LOGIC("Deployment specifies edge '%s' but it is either not used as producer or consumer (or neither)\n", edge->getName().c_str());
    }
  }

  private:

  std::string _name;
  std::vector<std::shared_ptr<Partition>> _partitions;
  std::vector<std::shared_ptr<Edge>> _edges;

}; // class Deployment

} // namespace hLLM::configuration
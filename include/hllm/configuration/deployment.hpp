#pragma once

#include "partition.hpp"
#include "channel.hpp"
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
  __INLINE__ void addChannel(const std::shared_ptr<Channel> channel) { _channels.push_back(channel); }

  [[nodiscard]] __INLINE__ std::string getName() const { return _name; }
  [[nodiscard]] __INLINE__ auto& getPartitions() const { return _partitions; }
  [[nodiscard]] __INLINE__ auto& getChannels() const { return _channels; }

  [[nodiscard]] __INLINE__ nlohmann::json serialize() const 
  {
    nlohmann::json js;

    js["Name"] = _name;

    std::vector<nlohmann::json> partitionsJs;
    for (const auto& p : _partitions) partitionsJs.push_back(p->serialize());
    js["Partitions"] = partitionsJs;

    std::vector<nlohmann::json> channelsJs;
    for (const auto& c : _channels) channelsJs.push_back(c->serialize());
    js["Channels"] = channelsJs;

    return js;
  }

  __INLINE__ void deserialize (const nlohmann::json& js)
  {
    // Clearing objects
    _partitions.clear();
    _channels.clear();

    _name = hicr::json::getString(js, "Name");

    const auto& partitions = hicr::json::getArray<nlohmann::json>(js, "Partitions");
    for (const auto& p : partitions) _partitions.push_back(std::make_shared<Partition>(p));

    const auto& channels = hicr::json::getArray<nlohmann::json>(js, "Channels");
    for (const auto& c : channels) _channels.push_back(std::make_shared<Channel>(c));
  }
  
  private:

  std::string _name;
  std::vector<std::shared_ptr<Partition>> _partitions;
  std::vector<std::shared_ptr<Channel>> _channels;

}; // class Deployment

} // namespace hLLM::configuration
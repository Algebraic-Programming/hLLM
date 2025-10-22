#pragma once

#include <cstdint>
#include <nlohmann_json/parser.hpp>
#include <hicr/core/definitions.hpp>
#include <hicr/core/instance.hpp>

namespace hLLM::configuration
{

class Replica final
{
  public:
  
  typedef uint64_t replicaIndex_t;

  Replica(const nlohmann::json js) { deserialize(js); };
  Replica(const std::string& name, const HiCR::Instance::instanceId_t instanceId) : _name(name), _instanceId(instanceId) {}
  ~Replica() = default;

  __INLINE__ void setName(const std::string& name) { _name = name; }
  __INLINE__ void setInstanceId(const HiCR::Instance::instanceId_t instanceId) { _instanceId = instanceId; }

  [[nodiscard]] __INLINE__ auto getName() const { return _name; }
  [[nodiscard]] __INLINE__ auto getInstanceId() const { return _instanceId; }

  [[nodiscard]] __INLINE__ nlohmann::json serialize() const 
  {
    nlohmann::json js;

    js["Name"] = _name;
    js["Instance Id"] = _instanceId;

    return js;
  }

  __INLINE__ void deserialize (const nlohmann::json& js)
  {
    _name = hicr::json::getString(js, "Name");
    if (js.contains("Instance Id")) _instanceId = hicr::json::getNumber<HiCR::Instance::instanceId_t>(js, "Instance Id"); // Optional, as it is determined at runtime
  }

  private:

  std::string _name;
  HiCR::Instance::instanceId_t _instanceId;  

}; // class Replica

} // namespace hLLM::configuration
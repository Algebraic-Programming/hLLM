#pragma once

#include <cstdint>
#include <memory>
#include "task.hpp"
#include <nlohmann_json/parser.hpp>
#include <hicr/core/definitions.hpp>
#include <hicr/core/instance.hpp>

namespace hLLM::configuration
{

class Partition final
{
  public:

  typedef size_t partitionId_t;

  Partition(const nlohmann::json js) { deserialize(js); };
  Partition(const partitionId_t id, const std::string& name, const HiCR::Instance::instanceId_t instanceId)
    : _id(id), _name(name), _instanceId(instanceId) {}
  ~Partition() = default;

  __INLINE__ void setId(const partitionId_t id) { _id = id; }
  __INLINE__ void setName(const std::string& name) { _name = name; }
  __INLINE__ void setInstanceId(const HiCR::Instance::instanceId_t instanceId) { _instanceId = instanceId; }
  __INLINE__ void addTask(const std::shared_ptr<Task> task) { _tasks.push_back(task); }

  [[nodiscard]] __INLINE__ auto getId() const { return _id; }
  [[nodiscard]] __INLINE__ auto getName() const { return _name; }
  [[nodiscard]] __INLINE__ auto getInstanceId() const { return _instanceId; }
  [[nodiscard]] __INLINE__ auto& getTasks() const { return _tasks; }

  [[nodiscard]] __INLINE__ nlohmann::json serialize() const 
  {
    nlohmann::json js;

    js["Id"] = _id;
    js["Name"] = _name;
    js["Instance Id"] = _instanceId;

    std::vector<nlohmann::json> tasksJs;
    for (const auto& t : _tasks) tasksJs.push_back(t->serialize());
    js["Tasks"] = tasksJs;

    return js;
  }

  __INLINE__ void deserialize (const nlohmann::json& js)
  {
    // Clearing objects
    _tasks.clear();

    _id = hicr::json::getNumber<partitionId_t>(js, "Id");
    _name = hicr::json::getString(js, "Name");
    if (js.contains("Instance Id")) _id = hicr::json::getNumber<HiCR::Instance::instanceId_t>(js, "Instance Id"); // Optional, as it is determined at runtime
    
    const auto& tasks = hicr::json::getArray<nlohmann::json>(js, "Tasks");
    for (const auto& t : tasks) _tasks.push_back(std::make_shared<Task>(t));
  }

  private:

  partitionId_t _id;
  std::string _name;
  HiCR::Instance::instanceId_t _instanceId;  
  std::vector<std::shared_ptr<Task>> _tasks;

}; // class Partition

} // namespace hLLM::configuration
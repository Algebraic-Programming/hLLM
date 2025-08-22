#pragma once

#include "engine.hpp"

namespace hLLM
{

void Engine::parseInstances(const nlohmann::json_abi_v3_11_2::json &config, nlohmann::json_abi_v3_11_2::json &requestJs)
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

__INLINE__ std::unordered_map<std::string, nlohmann::json> Engine::parseDependencies(const std::vector<nlohmann::json> &instancesJS)
{
  std::unordered_map<std::string, nlohmann::json> dependencies;

  // Get the consumers
  for (const auto &instance : instancesJS)
  {
    const auto &executionGraph = hicr::json::getArray<nlohmann::json>(instance, "Execution Graph");
    for (const auto &function : executionGraph)
    {
      const auto &inputs = hicr::json::getArray<nlohmann::json>(function, "Inputs");
      for (const auto &input : inputs)
      {
        const auto &inputName   = hicr::json::getString(input, "Name");
        dependencies[inputName] = input;
        dependencies[inputName].erase("Name");
        dependencies[inputName]["Consumer"] = hicr::json::getString(instance, "Name");
        dependencies[inputName]["Type"]     = hicr::json::getString(input, "Type");
      }
    }
  }

  size_t producersCount = 0;
  for (const auto &instance : instancesJS)
  {
    const auto &executionGraph = hicr::json::getArray<nlohmann::json>(instance, "Execution Graph");
    for (const auto &function : executionGraph)
    {
      const auto &outputs = hicr::json::getArray<nlohmann::json>(function, "Outputs");
      for (const auto &output : outputs)
      {
        if (dependencies.contains(output) == false)
        {
          HICR_THROW_RUNTIME("Producer %s defined in instance %s has no consumers defined", output.get<std::string>(), instance["Name"]);
        }

        if (dependencies[output].contains("Producer"))
        {
          HICR_THROW_RUNTIME("Set two producers for buffered dependency %s is not allowed. Producer: %s, tried to add %s",
                             output,
                             hicr::json::getString(dependencies[output], "Producer"),
                             hicr::json::getString(instance, "Name").c_str());
        }

        producersCount++;
        dependencies[output]["Producer"] = hicr::json::getString(instance, "Name");
      }
    }
  }

  // Check set equality
  if (producersCount != dependencies.size()) { HICR_THROW_RUNTIME("Inconsistent buffered connections #producers: %zud #consumers: %zu", producersCount, dependencies.size()); }
  return dependencies;
}
} // namespace hLLM
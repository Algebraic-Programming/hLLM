#pragma once

#include "engine.hpp"

namespace hLLM
{

__INLINE__ void Engine::validateConfiguration(const nlohmann::json &config)
{
  const auto &partitions = hicr::json::getArray<nlohmann::json>(config, "Partitions");

  // Check partition id uniqueness
  std::set<partitionId_t> seenPartitionIds;
  for (const auto &partition : partitions)
  {
    const auto &partitionId = hicr::json::getNumber<partitionId_t>(partition, "Partition ID");
    if (seenPartitionIds.contains(partitionId)) { HICR_THROW_FATAL("The configuration has a duplicated partition ID: %lu", partitionId); }
  }
}

__INLINE__ std::unordered_map<std::string, nlohmann::json> Engine::parseDependencies(const std::vector<nlohmann::json> &partitionsJs)
{
  std::unordered_map<std::string, nlohmann::json> dependenciesMap;

  // Get the consumers
  for (const auto &partition : partitionsJs)
  {
    const auto &executionGraph = hicr::json::getArray<nlohmann::json>(partition, "Execution Graph");
    for (const auto &function : executionGraph)
    {
      const auto &inputs = hicr::json::getArray<nlohmann::json>(function, "Inputs");
      for (const auto &input : inputs)
      {
        const auto &inputName      = hicr::json::getString(input, "Name");
        dependenciesMap[inputName] = input;
        dependenciesMap[inputName].erase("Name");
        dependenciesMap[inputName]["Consumer"] = hicr::json::getNumber<partitionId_t>(partition, "Partition ID");
        dependenciesMap[inputName]["Type"]     = hicr::json::getString(input, "Type");
      }
    }
  }

  size_t producersCount = 0;
  for (const auto &partition : partitionsJs)
  {
    const auto &executionGraph = hicr::json::getArray<nlohmann::json>(partition, "Execution Graph");
    for (const auto &function : executionGraph)
    {
      const auto &outputs = hicr::json::getArray<nlohmann::json>(function, "Outputs");
      for (const auto &output : outputs)
      {
        if (dependenciesMap.contains(output) == false)
        {
          HICR_THROW_RUNTIME("Producer %s defined in partition %s has no consumers defined", output.get<std::string>().c_str(), partition["Name"].get<std::string>().c_str());
        }

        if (dependenciesMap[output].contains("Producer"))
        {
          HICR_THROW_RUNTIME("Set two producers for buffered dependency %s is not allowed. Producer: %s, tried to add %s",
                             output.get<std::string>().c_str(),
                             hicr::json::getNumber<partitionId_t>(dependenciesMap[output], "Producer"),
                             hicr::json::getString(partition, "Name").c_str());
        }

        producersCount++;
        dependenciesMap[output]["Producer"] = hicr::json::getNumber<partitionId_t>(partition, "Partition ID");
      }
    }
  }

  // Check set equality
  if (producersCount != dependenciesMap.size())
  {
    HICR_THROW_RUNTIME("Inconsistent buffered connections #producers: %zud #consumers: %zu", producersCount, dependenciesMap.size());
  }
  return dependenciesMap;
}
} // namespace hLLM
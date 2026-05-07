#pragma once

#include <string>
#include <fstream>
#include <nlohmann_json/json.hpp>

#include <hicr/core/instanceManager.hpp>
#include <modules/configuration/deployment.hpp>

#define _REPLICAS_PER_PARTITION 1

void readAndParseConfiguration(char *argv[], hLLM::configuration::Deployment &deployment, std::shared_ptr<HiCR::InstanceManager> &instanceManager)
{
  // Getting config file name from arguments
  std::string hllmConfigFilePath = std::string(argv[1]);

  // Parsing request file contents to a JSON object
  std::ifstream hllmConfigFs(hllmConfigFilePath);
  auto          hllmConfigJs = nlohmann::json::parse(hllmConfigFs);

  // Parsing config file using hLLM
  deployment.deserialize(hllmConfigJs);

  // Calculating the number of instances required (1 per partition that runs the coordinator and a replica)
  const auto instancesRequired = deployment.getPartitions().size() * _REPLICAS_PER_PARTITION;

  // Checking I have the correct number of instances (only one)
  if (instanceManager->getInstances().size() != instancesRequired)
  {
    fprintf(stderr, "Error: %lu instances provided, but %lu are required\n", instanceManager->getInstances().size(), instancesRequired);
    instanceManager->abort(-1);
  }

  // Assigning this example to run in a single instance
  auto instance = instanceManager->getInstances().begin();

  // Assigning instance ids to the partitions
  for (auto p : deployment.getPartitions())
  {
    // Getting instance Id that will run this partition (only one)
    const auto partitionInstanceId = (*instance)->getId();
    
    // Update instance 
    instance++;

    // Setting this partition to be executed by the same instance than replica zero
    p->setCoordinatorInstanceId(partitionInstanceId);

    // Adding a replicas to this partition, with the same instance as the coordinator
    for (size_t i = 0; i < _REPLICAS_PER_PARTITION; i++)
    {
      const auto replica = std::make_shared<hLLM::configuration::Replica>(partitionInstanceId);
      p->addReplica(replica);
    }
  }
  printf("[Instance %lu] Parsed deployment configuration\n", instanceManager->getCurrentInstance()->getId());
}
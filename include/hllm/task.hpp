#pragma once

#include <unordered_map>
#include <hicr/core/exceptions.hpp>
#include <hicr/core/definitions.hpp>
#include <taskr/taskr.hpp>
#include "configuration/task.hpp"
#include "configuration/replica.hpp"
#include "configuration/partition.hpp"

namespace hLLM
{

namespace roles::partition { class Replica; }

class Task final
{
  friend class roles::partition::Replica;

  public:

  using taskFunction_t = std::function<void(Task *task)>;

  Task() = delete;

  Task(const hLLM::configuration::Task                   taskConfig,
       const taskFunction_t                              &function,
       std::unique_ptr<taskr::Task>                      taskrTask)
    : _taskConfig(taskConfig),
      _function(function),
      _taskrTask(std::move(taskrTask))
  {
    // Adding input token holders
    for (const auto& input : taskConfig.getInputs()) _inputs[input] = nullptr;
  }

  ~Task() = default;

  __INLINE__ const std::shared_ptr<HiCR::LocalMemorySlot> getInput(const std::string &inputName)
  {
    // Check whether the input token exists for this task
    if (_inputs.contains(inputName) == false)
        HICR_THROW_RUNTIME("Task '%s' trying to access input '%s' which has not been declared for this task.\n", _taskConfig.getFunctionName().c_str(), inputName.c_str());

    // Check whether the input has been given (sanity check)
    if (_inputs[inputName] == nullptr)
        HICR_THROW_RUNTIME("Task '%s' trying to access input '%s' which has not been provided. This must be a bug in hLLM.\n", _taskConfig.getFunctionName().c_str(), inputName.c_str());

    // Then, get the token
    const auto token = _inputs.at(inputName);

    // Then, erasing it to indicate it was consumed
    _inputs[inputName] = nullptr;

    // Returning consumed token
    return token;
  }

  __INLINE__ void setOutput(const std::string &outputName, const std::shared_ptr<HiCR::LocalMemorySlot> &memorySlot)
  {
    if (_outputEdges.contains(outputName) == false) 
      HICR_THROW_RUNTIME("Task '%s' is setting output '%s' which is not defined for this task.\n", _taskConfig.getFunctionName().c_str(), outputName.c_str());

    if (_outputsSent.contains(outputName) == true) 
      HICR_THROW_RUNTIME("Task '%s' is setting output '%s' twice.\n", _taskConfig.getFunctionName().c_str(), outputName.c_str());

    // Getting edge to send the output through
    const auto& outputEdge = _outputEdges[outputName];

    // Creating new message
    const auto message = messages::Data((const uint8_t*)memorySlot->getPointer(), memorySlot->getSize(), _promptId);

    // Encoding message
    const auto rawMessage = message.encode();

    // Wait until there is enough space in the output buffer before sending
    outputEdge->lock();
    while(outputEdge->isFull(rawMessage.getSize()) == true);
    outputEdge->pushMessage(rawMessage);
    outputEdge->unlock();

    // Set output as sent
    _outputsSent.insert(outputName);
  }

  [[nodiscard]] __INLINE__ configuration::Partition::partitionIndex_t getPartitionIdx() const { return _partitionIdx; }
  [[nodiscard]] __INLINE__ configuration::Replica::replicaIndex_t getReplicaIdx() const { return _replicaIdx; }
  [[nodiscard]] __INLINE__ hLLM::Prompt::promptId_t getPromptId() const { return _promptId; }

  private:

  __INLINE__ taskFunction_t getFunction() const { return _function; }
  __INLINE__ const hLLM::configuration::Task& getConfig() const { return _taskConfig; }
  __INLINE__ taskr::Task *getTaskRTask() const { return _taskrTask.get(); }
  __INLINE__ void setPartitionIdx(const configuration::Partition::partitionIndex_t partitionIdx) { _partitionIdx = partitionIdx; }
  __INLINE__ void setReplicaIdx(const configuration::Replica::replicaIndex_t replicaIdx) { _replicaIdx = replicaIdx; }
  __INLINE__ void setPromptId(const hLLM::Prompt::promptId_t promptId) { _promptId = promptId; }
  __INLINE__ void setOutputEdge(const std::string& output, const std::shared_ptr<hLLM::edge::Output> edge) { _outputEdges[output] = edge; }

  __INLINE__ bool isReady() const
  {
    for (const auto& input : _inputs) if (input.second == nullptr) return false;
    return true;
  }

  __INLINE__ void verifyOutputsSent() const
  {
    // Check all outputs have been sent
    for (const auto& output : _taskConfig.getOutputs())
      if (_outputsSent.contains(output) == false)
        HICR_THROW_RUNTIME("Task '%s' did not set output '%s' before completion.\n", _taskConfig.getFunctionName().c_str(), output.c_str());  
  }

  __INLINE__ void setInput(const std::string &inputName, const std::shared_ptr<HiCR::LocalMemorySlot> token)
  {
    // Check whether the input token exists for this task
    if (_inputs.contains(inputName) == false)
        HICR_THROW_RUNTIME("Task '%s' trying to set input '%s' which has not been declared for this task. This must be a bug in hLLM.\n", _taskConfig.getFunctionName().c_str(), inputName.c_str());

    // Check whether the input has been given (sanity check)
    if (_inputs[inputName] != nullptr)
        HICR_THROW_RUNTIME("Task '%s' trying to set input '%s' which has already been set. This must be a bug in hLLM.\n", _taskConfig.getFunctionName().c_str(), inputName.c_str());

     _inputs[inputName] = token;
  }

  const hLLM::configuration::Task                    _taskConfig;
  const taskFunction_t                               _function;
  const std::unique_ptr<taskr::Task>                 _taskrTask;

  std::map<std::string, std::shared_ptr<HiCR::LocalMemorySlot>> _inputs;

  // A map relating an output name to the edge through which the data is sent
  std::map<std::string, std::shared_ptr<hLLM::edge::Output>> _outputEdges;
  std::set<std::string> _outputsSent;

  hLLM::Prompt::promptId_t _promptId;
  configuration::Partition::partitionIndex_t _partitionIdx;
  configuration::Replica::replicaIndex_t _replicaIdx;

}; // class Task

} // namespace hLLM
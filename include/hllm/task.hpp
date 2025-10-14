#pragma once

#include <unordered_map>
#include <hicr/core/exceptions.hpp>
#include <hicr/core/definitions.hpp>
#include <taskr/taskr.hpp>
#include "configuration/task.hpp"

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
    // Adding input and output token holders
    for (const auto& input : taskConfig.getInputs()) _inputTokens[input] = nullptr;
    for (const auto& output : taskConfig.getOutputs()) _outputTokens[output] = nullptr;
  }

  ~Task() = default;

  __INLINE__ const std::shared_ptr<HiCR::LocalMemorySlot> getInput(const std::string &inputName)
  {
    // Check whether the input token exists for this task
    if (_inputTokens.contains(inputName) == false)
        HICR_THROW_RUNTIME("Function '%s' trying to access input '%s' which has not been declared for this task.\n", _taskConfig.getFunctionName().c_str(), inputName.c_str());

    // Check whether the input has been given (sanity check)
    if (_inputTokens[inputName] == nullptr)
        HICR_THROW_RUNTIME("Function '%s' trying to access input '%s' which has not been provided. This must be a bug in hLLM.\n", _taskConfig.getFunctionName().c_str(), inputName.c_str());

    // Then, get the token
    const auto token = _inputTokens.at(inputName);

    // Then, erasing it to indicate it was consumed
    _inputTokens[inputName] = nullptr;

    // Returning consumed token
    return token;
  }

  __INLINE__ void setOutput(const std::string &outputName, const std::shared_ptr<HiCR::LocalMemorySlot> &memorySlot)
  {
    if (_outputTokens.contains(outputName) == false) 
      HICR_THROW_RUNTIME("Function '%s' is setting output '%s' which has not been declared for this task.\n", _taskConfig.getFunctionName().c_str(), outputName.c_str());

    if (_outputTokens[outputName] != nullptr) 
      HICR_THROW_RUNTIME("Function '%s' is setting output '%s' twice.\n", _taskConfig.getFunctionName().c_str(), outputName.c_str());

    _outputTokens[outputName] = memorySlot;
  }

  private:

  __INLINE__ taskFunction_t getFunction() const { return _function; }
  __INLINE__ const hLLM::configuration::Task& getConfig() const { return _taskConfig; }
  __INLINE__ taskr::Task *getTaskRTask() const { return _taskrTask.get(); }

  __INLINE__ bool isReady() const
  {
    for (const auto& input : _inputTokens) if (input.second == nullptr) return false;
    return true;
  }

  __INLINE__ std::shared_ptr<HiCR::LocalMemorySlot> getOutput(const std::string &outputName)
  { 
    if (_outputTokens.contains(outputName) == false) 
      HICR_THROW_RUNTIME("Function '%s' is getting output '%s' which has not been provided for this task.\n", _taskConfig.getFunctionName().c_str(), outputName.c_str());

    if (_outputTokens[outputName] != nullptr) 
      HICR_THROW_RUNTIME("Function '%s' is getting output '%s' twice. This must be a bug in hLLM\n", _taskConfig.getFunctionName().c_str(), outputName.c_str());
    
    // First, get the token
    const auto token = _outputTokens.at(outputName);

    // Then, erasing it from the map to indicate it was consumed
    _outputTokens[outputName] = nullptr;

    // Returning consumed token
    return token;
  }

  __INLINE__ void setInput(const std::string &inputName, const std::shared_ptr<HiCR::LocalMemorySlot> token)
  {
    // Check whether the input token exists for this task
    if (_inputTokens.contains(inputName) == false)
        HICR_THROW_RUNTIME("Function '%s' trying to set input '%s' which has not been declared for this task. This must be a bug in hLLM.\n", _taskConfig.getFunctionName().c_str(), inputName.c_str());

    // Check whether the input has been given (sanity check)
    if (_inputTokens[inputName] != nullptr)
        HICR_THROW_RUNTIME("Function '%s' trying to set input '%s' which has already been set. This must be a bug in hLLM.\n", _taskConfig.getFunctionName().c_str(), inputName.c_str());

     _inputTokens[inputName] = token;
  }

  const hLLM::configuration::Task                    _taskConfig;
  const taskFunction_t                               _function;
  const std::unique_ptr<taskr::Task>                 _taskrTask;

  std::map<std::string, std::shared_ptr<HiCR::LocalMemorySlot>> _inputTokens;
  std::map<std::string, std::shared_ptr<HiCR::LocalMemorySlot>> _outputTokens;

}; // class Task

} // namespace hLLM
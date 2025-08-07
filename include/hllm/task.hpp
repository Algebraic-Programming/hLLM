
#pragma once

#include <hicr/core/exceptions.hpp>
#include <hicr/core/definitions.hpp>
#include <taskr/taskr.hpp>
#include <deployr/deployr.hpp>

namespace hLLM
{

class Engine;
class Task;

typedef std::function<void(hLLM::Task *task)> function_t;

class Task final
{
  friend class Engine;

  public:

  Task() = delete;

  Task(const std::string                                     &name,
       const function_t                                      &function,
       const std::vector<std::pair<std::string, std::string>> inputs,
       const std::vector<std::pair<std::string, std::string>> outputs,
       std::unique_ptr<taskr::Task>                           taskrTask)
    : _name(name),
      _function(function),
      _inputs(inputs),
      _outputs(outputs),
      _taskrTask(std::move(taskrTask))
  {}

  __INLINE__ std::string getName() const { return _name; }

  ~Task() = default;

  __INLINE__ const deployr::Channel::token_t getInput(const std::string &inputName)
  {
    // First, get the token
    const auto &token = _inputTokens.at(inputName);

    // Then, erasing it from the map to indicate it was consumed
    _inputTokens.erase(inputName);

    // Returning consumed token
    return token;
  }

  __INLINE__ void setOutput(const std::string &outputName, const void *bufferData, const size_t bufferSize)
  {
    if (_outputTokens.contains(outputName))
    {
      fprintf(stderr, "Function '%s' is setting output '%s' twice.\n", _name.c_str(), outputName.c_str());
      abort();
    }
    _outputTokens[outputName] = deployr::Channel::token_t{.success = true, .buffer = (void *)bufferData, .size = bufferSize};
  }

  __INLINE__ void waitFor(taskr::Task::pendingOperation_t operation)
  {
    _taskrTask->addPendingOperation(operation);
    _taskrTask->suspend();
  }

  private:

  __INLINE__ function_t getFunction() const { return _function; }
  __INLINE__ taskr::Task *getTaskRTask() const { return _taskrTask.get(); }
  __INLINE__ const std::vector<std::pair<std::string, std::string>> &getInputs() const { return _inputs; }
  __INLINE__ const std::vector<std::pair<std::string, std::string>> &getOutputs() const { return _outputs; }

  __INLINE__ const deployr::Channel::token_t getOutput(const std::string &outputName)
  { // First, get the token
    const auto &token = _outputTokens.at(outputName);

    // Then, erasing it from the map to indicate it was consumed
    _outputTokens.erase(outputName);

    // Returning consumed token
    return token;
  }
  __INLINE__ void setInput(const std::string &inputName, const deployr::Channel::token_t token) { _inputTokens[inputName] = token; }
  __INLINE__ bool hasOutput(const std::string &outputName) { return _outputTokens.contains(outputName); }
  __INLINE__ bool hasInput(const std::string &inputName) { return _inputTokens.contains(inputName); }
  __INLINE__ void clearOutputs() { _outputTokens.clear(); }

  const std::string                                      _name;
  const function_t                                       _function;
  const std::vector<std::pair<std::string, std::string>> _inputs;
  const std::vector<std::pair<std::string, std::string>> _outputs;
  const std::unique_ptr<taskr::Task>                     _taskrTask;

  std::map<std::string, deployr::Channel::token_t> _inputTokens;
  std::map<std::string, deployr::Channel::token_t> _outputTokens;

}; // class Task

} // namespace hLLM
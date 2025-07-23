
#pragma once

#include <hicr/core/exceptions.hpp>
#include <hicr/core/definitions.hpp>
#include <taskr/taskr.hpp>
#include <deployr/deployr.hpp>

namespace llmEngine
{

class LLMEngine;
class Task;

typedef std::function<void(llmEngine::Task *task)> function_t;

/**
 * LLMEngine Task type, similar to a TaskR task but has the ability to set outputs and get inputs
 */
class Task final
{
  friend class LLMEngine;

  public:

  Task()  = delete;
  ~Task() = default;

  /**
   * The Task constructor
   * 
   * @param[in] name Unique name of the task
   * @param[in] function The function which has to be executed
   * @param[in] inputs The input tokens to read from another instance
   * @param[in] outputs The tokens to pass for another instance
   * @param[in] dependencies A list of which instances it is related
   * @param[in] taskrTask The taskR Task this task has to relate
   */
  Task(const std::string             &name,
       const function_t              &function,
       const std::vector<std::string> inputs,
       const std::vector<std::string> outputs,
       const std::vector<std::string> dependencies,
       std::unique_ptr<taskr::Task>   taskrTask)
    : _name(name),
      _function(function),
      _inputs(inputs),
      _outputs(outputs),
      _dependencies(dependencies),
      _taskrTask(std::move(taskrTask))
  {}

  /**
   * Returns the set name of this task
   * 
   * \return The string
   */
  __INLINE__ std::string getName() const { return _name; }

  /**
   * Returns the stored token by name
   * 
   * @param[in] inputName The returnable token of type inputName
   * \return The desired token if the inputName 
   */
  __INLINE__ const deployr::Channel::token_t getInput(const std::string &inputName)
  {
    // First, get the token
    const auto &token = _inputTokens.at(inputName);

    // Then, erasing it from the map to indicate it was consumed
    _inputTokens.erase(inputName);

    // Returning consumed token
    return token;
  }

  /**
   * Specify an output token to be passabled to other Instances
   * 
   * @param[in] outputName The desired token (if it exists abort, no duplicates allowed)
   * @param[in] bufferData The pointer to the buffer, make sure that the lifetime of the buffer is not changing
   * @param[in] bufferSize The size of this buffer
   */
  __INLINE__ void setOutput(const std::string &outputName, const void *bufferData, const size_t bufferSize)
  {
    if (_outputTokens.contains(outputName))
    {
      fprintf(stderr, "Function '%s' is setting output '%s' twice.\n", _name.c_str(), outputName.c_str());
      abort();
    }
    _outputTokens[outputName] = deployr::Channel::token_t{.success = true, .buffer = (void *)bufferData, .size = bufferSize};
  }

  private:

  __INLINE__ function_t getFunction() const { return _function; }
  __INLINE__ taskr::Task *getTaskRTask() const { return _taskrTask.get(); }
  __INLINE__ const std::vector<std::string> &getInputs() const { return _inputs; }
  __INLINE__ const std::vector<std::string> &getOutputs() const { return _outputs; }
  __INLINE__ const std::vector<std::string> &getDependencies() const { return _dependencies; }

  __INLINE__ size_t getExecutionCounter() const { return _executionCounter; }
  __INLINE__ void   advanceExecutionCounter() { _executionCounter++; }

  __INLINE__ const deployr::Channel::token_t getOutput(const std::string &outputName) const { return _outputTokens.at(outputName); }
  __INLINE__ void                            setInput(const std::string &inputName, const deployr::Channel::token_t token) { _inputTokens[inputName] = token; }
  __INLINE__ bool                            hasOutput(const std::string &outputName) { return _outputTokens.contains(outputName); }
  __INLINE__ bool                            hasInput(const std::string &inputName) { return _inputTokens.contains(inputName); }
  __INLINE__ void                            clearOutputs() { _outputTokens.clear(); }

  size_t                             _executionCounter = 0;
  const std::string                  _name;
  const function_t                   _function;
  const std::vector<std::string>     _inputs;
  const std::vector<std::string>     _outputs;
  const std::vector<std::string>     _dependencies;
  const std::unique_ptr<taskr::Task> _taskrTask;

  std::map<std::string, deployr::Channel::token_t> _inputTokens;
  std::map<std::string, deployr::Channel::token_t> _outputTokens;

}; // class Task

} // namespace llmEngine
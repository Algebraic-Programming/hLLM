
#pragma once

#include <hicr/core/exceptions.hpp>
#include <hicr/core/definitions.hpp>
#include <taskr/taskr.hpp>

namespace llmEngine
{

class Task final
{
  public:

  Task() = delete;
  Task(const std::string& functionName,
       const std::function<void()>& function,
       const std::vector<std::string> inputs,
       const std::vector<std::string> outputs,
       const std::vector<std::string> dependencies,
       std::unique_ptr<taskr::Task> taskrTask)
    : _functionName(functionName),
      _function(function),
      _inputs(inputs),
      _outputs(outputs),
      _dependencies(dependencies),
      _taskrTask(std::move(taskrTask))
  {
  }

  ~Task() = default;

  __INLINE__ std::string getFunctionName() const { return _functionName; }
  __INLINE__ std::function<void()> getFunction() const { return _function; }
  __INLINE__ taskr::Task* getTaskRTask() const { return _taskrTask.get(); }
  __INLINE__ const std::vector<std::string>& getInputs() const { return _inputs; }
  __INLINE__ const std::vector<std::string>& getOutputs() const { return _outputs; }
  __INLINE__ const std::vector<std::string>& getDependencies() const { return _dependencies; }

  __INLINE__ size_t getExecutionCounter() const { return _executionCounter; }
  __INLINE__ void advanceExecutionCounter() { _executionCounter++; }

  private:

  size_t _executionCounter = 0;
  const std::string _functionName;
  const std::function<void()> _function;
  const std::vector<std::string> _inputs;
  const std::vector<std::string> _outputs;
  const std::vector<std::string> _dependencies;
  const std::unique_ptr<taskr::Task> _taskrTask;

}; // class Task

} // namespace llmEngine
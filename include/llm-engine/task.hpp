
#pragma once

#include <hicr/core/exceptions.hpp>
#include <hicr/core/definitions.hpp>
#include <taskr/taskr.hpp>

namespace llmEngine
{

class Task final
{
  public:

  Task(const std::string& functionName, const std::function<void()>& function, std::unique_ptr<taskr::Task> taskrTask)
    : _functionName(functionName),
      _function(function),
      _taskrTask(std::move(taskrTask))
  {
  }

  ~Task() {}

  __INLINE__ size_t getIterationCount() const { return _iterationCount; }
  __INLINE__ std::string getFunctionName() const { return _functionName; }
  __INLINE__ std::function<void()> getFunction() const { return _function; }
  __INLINE__ taskr::Task* getTaskRTask() const { return _taskrTask.get(); }

  private:

  size_t _iterationCount = 0;
  const std::string _functionName;
  const std::function<void()> _function;
  const std::unique_ptr<taskr::Task> _taskrTask;

}; // class Task

} // namespace llmEngine
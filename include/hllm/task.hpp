#pragma once

#include <unordered_map>

#include <hicr/core/exceptions.hpp>
#include <hicr/core/definitions.hpp>
// #include <hicr/frontends/channel/variableSize/mpsc/locking/consumer.hpp>
// #include <hicr/frontends/channel/variableSize/mpsc/locking/producer.hpp>
#include <taskr/taskr.hpp>
// #include "channels/consumer.hpp"
// #include "channels/producer.hpp"

namespace hLLM
{

class Engine;
class Task;

using function_t = std::function<void(hLLM::Task *task)>;

class Task final
{
  friend class Engine;

  public:

  Task() = delete;

  Task(const std::string                                 &name,
       const function_t                                  &function,
      //  std::unordered_map<std::string, channel::Consumer> consumers,
      //  std::unordered_map<std::string, channel::Producer> producers,
       std::unique_ptr<taskr::Task>                       taskrTask)
    : _name(name),
      _function(function),
      // _consumers(std::move(consumers)),
      // _producers(std::move(producers)),
      _taskrTask(std::move(taskrTask))
  {}

  __INLINE__ std::string getName() const { return _name; }

  ~Task() = default;

  __INLINE__ const std::shared_ptr<HiCR::LocalMemorySlot> getInput(const std::string &inputName)
  {
    // First, get the token
    const auto token = _inputTokens.at(inputName);

    // Then, erasing it from the map to indicate it was consumed
    _inputTokens.erase(inputName);

    // Returning consumed token
    return token;
  }

  __INLINE__ void setOutput(const std::string &outputName, const std::shared_ptr<HiCR::LocalMemorySlot> &memorySlot)
  {
    if (_outputTokens.contains(outputName))
    {
      fprintf(stderr, "Function '%s' is setting output '%s' twice.\n", _name.c_str(), outputName.c_str());
      abort();
    }
    _outputTokens[outputName] = memorySlot;
  }

  __INLINE__ void waitFor(taskr::Task::pendingOperation_t operation)
  {
    _taskrTask->addPendingOperation(operation);
    _taskrTask->suspend();
  }

  __INLINE__ bool checkInputsReadiness()
  {
    // for (auto &[_, consumer] : _consumers)
    // {
    //   if (consumer.isEmpty()) return false;
    // }
    return true;
  }

  __INLINE__ bool checkOutputsReadiness()
  {
    // for (auto &[_, producer] : _producers)
    // {
    //   if (producer.isFull()) return false;
    // }
    return true;
  }

  __INLINE__ void setInputs()
  {
    // for (auto &[input, consumer] : _consumers)
    // {
    //   // Get token
    //   auto token = consumer.peek();

    //   // Pushing token onto the task
    //   setInput(input, token);
    // }
  }

  __INLINE__ void popInputs()
  {
    // for (auto &[input, consumer] : _consumers)
    // {
    //   if (hasInput(input) == true)
    //   {
    //     fprintf(stderr, "Function '%s' has not consumed required input '%s'\n", _name.c_str(), input.c_str());
    //     abort();
    //   }

    //   consumer.pop();
    // }
  }

  __INLINE__ void pushOutputs()
  {
    // for (auto &[output, producer] : _producers)
    // {
    //   // Checking if output has been produced by the task
    //   if (hasOutput(output) == false)
    //   {
    //     fprintf(stderr, "Function '%s' has not pushed required output '%s'\n", _name.c_str(), output.c_str());
    //     abort();
    //   }

    //   // Now getting output token
    //   const auto outputToken = getOutput(output);

    //   // Pushing token onto channel
    //   if (producer.push(outputToken) == false) { HICR_THROW_RUNTIME("Channel full"); }
    // }
  }

  private:

  __INLINE__ function_t getFunction() const { return _function; }
  __INLINE__ taskr::Task *getTaskRTask() const { return _taskrTask.get(); }

  __INLINE__ std::shared_ptr<HiCR::LocalMemorySlot> getOutput(const std::string &outputName)
  { // First, get the token
    const auto token = _outputTokens.at(outputName);

    // Then, erasing it from the map to indicate it was consumed
    _outputTokens.erase(outputName);

    // Returning consumed token
    return token;
  }
  __INLINE__ void setInput(const std::string &inputName, const std::shared_ptr<HiCR::LocalMemorySlot> token) { _inputTokens[inputName] = token; }
  __INLINE__ bool hasOutput(const std::string &outputName) { return _outputTokens.contains(outputName); }
  __INLINE__ bool hasInput(const std::string &inputName) { return _inputTokens.contains(inputName); }
  __INLINE__ void clearOutputs() { _outputTokens.clear(); }

  const std::string                                  _name;
  const function_t                                   _function;
  // std::unordered_map<std::string, channel::Consumer> _consumers;
  // std::unordered_map<std::string, channel::Producer> _producers;
  const std::unique_ptr<taskr::Task>                 _taskrTask;

  std::map<std::string, std::shared_ptr<HiCR::LocalMemorySlot>> _inputTokens;
  std::map<std::string, std::shared_ptr<HiCR::LocalMemorySlot>> _outputTokens;

}; // class Task

} // namespace hLLM
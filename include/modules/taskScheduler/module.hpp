#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <hicr/core/computeManager.hpp>
#include <taskr/taskr.hpp>

#include <modules/module.hpp>

namespace hLLM::modules::taskScheduler
{

class Module final : public hLLM::modules::Module
{
  public:

  using taskFunction_t = taskr::function_t;

  Module(std::shared_ptr<HiCR::ComputeManager> computeManager, std::shared_ptr<taskr::Runtime> taskr)
    : hLLM::modules::Module(),
      _computeManager(computeManager),
      _taskr(taskr)
  {
    if (_computeManager == nullptr) HICR_THROW_LOGIC("Null compute manager passed to taskScheduler module.");
    if (_taskr == nullptr) HICR_THROW_LOGIC("Null taskr runtime passed to taskScheduler module.");
  }

  ~Module() override = default;

  __INLINE__ void addTask(const std::string &name, const taskFunction_t &function)
  {
    if (_taskNameToIndex.contains(name)) HICR_THROW_LOGIC("Task '%s' is already registered in taskScheduler module.", name.c_str());
    // Store function with stable ownership
    _functions.push_back(std::make_unique<taskr::Function>(_computeManager.get(), function));
    // Store task with stable ownership
    _tasks.push_back(std::make_unique<taskr::Task>(_functions.back().get()));
    // Store index for lookup
    _taskNameToIndex[name] = _tasks.size() - 1;
  }

  void initialize() override
  {
    // Re-add suspended tasks
    _taskr->setTaskCallbackHandler(HiCR::tasking::Task::callback_t::onTaskSuspend, [&](taskr::Task *task) { _taskr->resumeTask(task); });

    // Keep runtime alive until explicit stop/termination policy
    _taskr->setFinishOnLastTask(false);

    // Register all tasks currently known
    for (auto &task : _tasks) _taskr->addTask(task.get());

    // Initialize runtime
    _taskr->initialize();
  }

  void run() override { _taskr->run(); }

  void terminate() override
  {
    // Set finish on last task to true to allow runtime termination once all tasks have finished executing
    _taskr->setFinishOnLastTask(true);
  }

  void await() override
  {
    // Wait for runtime to finish
    _taskr->await();
  }

  void finalize() override { _taskr->finalize(); }

  protected:

  void service() override {}

  private:

  std::shared_ptr<HiCR::ComputeManager> _computeManager;
  std::shared_ptr<taskr::Runtime>       _taskr;

  std::vector<std::unique_ptr<taskr::Function>> _functions;
  std::vector<std::unique_ptr<taskr::Task>>     _tasks;
  std::unordered_map<std::string, size_t>       _taskNameToIndex;
};
} // namespace hLLM::modules::taskScheduler
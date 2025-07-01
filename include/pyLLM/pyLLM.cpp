#include <iostream>  // for std::cout
#include <vector>
#include <string>

#include <pybind11_json/pybind11_json.hpp>
#include <pybind11/pybind11.h>
#include <pybind11/functional.h> // std::function interpreter
#include <pybind11/stl.h>        // std::set interpreter

#include <taskr/taskr.hpp>  // llmEngine::Task has a TaskR task as parameter
#include "llm-engine/llm-engine.hpp"

namespace py = pybind11;

namespace llmEngine
{

// LLMEngine initialize wrapper for python
bool initialize_engine_from_python(LLMEngine& llmengine, const std::vector<std::string>& args) 
{
  int argc = static_cast<int>(args.size());
  std::vector<char*> argv_cstr;

  for (const auto& s : args) {
    argv_cstr.push_back(const_cast<char*>(s.c_str()));
  }

  argv_cstr.push_back(nullptr);  // mimic C-style argv termination

  char** argv = argv_cstr.data();

  return llmengine.initialize(&argc, &argv);
}
    
PYBIND11_MODULE(llmEngine, m)
{
  m.doc() = "pybind11 plugin for LMM-Engine";

  m.def(
    "initialize_engine",
    &initialize_engine_from_python,
    "Initialize Engine with sys.argv"
  );

  py::class_<LLMEngine>(m, "LLMEngine")
    .def(py::init<>())
    .def("run", &LLMEngine::run)
    .def("abort", &LLMEngine::abort)
    .def("registerFunction", &LLMEngine::registerFunction)
    .def("terminate", &LLMEngine::terminate)
    .def("finalize", &LLMEngine::finalize);

  py::class_<Task>(m, "Task")
    // .def(py::init<const std::string&, 
    //               const function_t&, 
    //               const std::vector<std::string>, 
    //               const std::vector<std::string>, 
    //               const std::vector<std::string>,
    //               std::unique_ptr<taskr::Task>>(),
    //               py::arg("name"),
    //               py::arg("function"),
    //               py::arg("inputs"),
    //               py::arg("outputs"),
    //               py::arg("dependencies"),
    //               py::arg("taskrTask"))
    .def("getName", &Task::getName)
    .def("getInput", &Task::getInput)
    .def("setOutput", &Task::setOutput);
}

}   // namespace llmEngine
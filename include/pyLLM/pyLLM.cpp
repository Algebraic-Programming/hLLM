
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <taskr/taskr.hpp>
#include "llm-engine/llm-engine.hpp"

namespace py = pybind11;

namespace llmEngine
{

// LLMEngine initialize method for python
void initialize_engine_from_python(Engine& engine, const std::vector<std::string>& args) 
{
    int argc = static_cast<int>(args.size());
    std::vector<char*> argv_cstr;

    for (const auto& s : args) {
        argv_cstr.push_back(const_cast<char*>(s.c_str()));
    }

    argv_cstr.push_back(nullptr);  // mimic C-style argv termination

    char** argv = argv_cstr.data();
    engine.initialize(&argc, &argv);  // no need for 'this->' here
}
    
PYBIND11_MODULE(llmEngine, m)
{
  m.doc() = "pybind11 plugin for LMM-Engine";

  m.def("initialize_engine", &initialize_engine_from_python, "Initialize Engine with sys.argv");

  py::class_<LLMEngine>(m, "LLMEngine")
    .def(py::init<>())
    .def("initialize", &LLMEngine::initialize)
    .def("run", &LLMEngine::run)
    .def("abort", &LLMEngine::abort)
    .def("registerFunction", &LLMEngine::registerFunction)
    .def("terminate", &LLMEngine::terminate)
    .def("finalize", &LLMEngine::finalize);

  py::class_<Task>(m, "Task")
    .def(py::init<const std::string&, 
                  const function_t&, 
                  const std::vector<std::string>, 
                  const std::vector<std::string>, 
                  const std::vector<std::string>,
                  std::unique_ptr<taskr::Task>>(),
                  py::arg("name"),
                  py::arg("function"),
                  py::arg("inputs"),
                  py::arg("outputs"),
                  py::arg("dependencies"),
                  py::arg("taskrTask"))
    .def("getName", &Task::getName)
    .def("getInput", &Task::getInput)
    .def("setOutput", &Task::setOutput);
}

}   // namespace llmEngine
#include <iostream>  // for std::cout
#include <vector>
#include <string>

#include <pybind11_json/pybind11_json.hpp> // For nlohmann json file-based parameters

#include <pybind11/pybind11.h>
#include <pybind11/functional.h> // std::function interpreter
#include <pybind11/stl.h>        // std::set interpreter

#include <taskr/taskr.hpp>  // llmEngine::Task has a TaskR task as parameter
#include "llm-engine/llm-engine.hpp"

namespace py = pybind11;

namespace llmEngine
{

/**
 * LLMEngine initialize wrapper for python
 * as the sys.argv is handled differently in python compared to C/C++
 */
bool initialize_engine_from_python(LLMEngine& llmengine, const std::vector<std::string>& args) 
{
  py::gil_scoped_release release; // Not sure if this is needed/helps

  int argc = static_cast<int>(args.size());
  std::vector<char*> argv_cstr;

  for (const auto& s : args) {
    argv_cstr.push_back(const_cast<char*>(s.c_str()));
  }

  argv_cstr.push_back(nullptr);  // mimic C-style argv termination

  char** argv = argv_cstr.data();

  return llmengine.initialize(&argc, &argv);
}

/**
 * As the python json load does handle numbers to be any kind, 
 * we have to transform them into unsigned
 */
void convert_numbers_to_unsigned(nlohmann::json& j)
{
    if (j.is_object()) {
        for (auto& el : j.items()) {
            convert_numbers_to_unsigned(el.value());
        }
    } else if (j.is_array()) {
        for (auto& el : j) {
            convert_numbers_to_unsigned(el);
        }
    } else if (j.is_number()) {
        int64_t val = j.get<int64_t>();
        if (val < 0) {
            throw std::runtime_error("Negative value found where unsigned expected.");
        }
        j = static_cast<uint64_t>(val);  // Re-inserts as number_unsigned
    }
}

/**
 * Convert json config numbers to be unsigned and then run llmEngine
 */
void convert_run(LLMEngine& llmEngine_, nlohmann::json& configJs)
{
  // specifically convert all numbers to be unsigned
  convert_numbers_to_unsigned(configJs);

  llmEngine_.run(configJs);
}

/**
 * the LLMEngine::Task setOutput method python compatible 
 * as pybind11 doesn't know how to handle void* properly
 */
void setOutputWrapper(llmEngine::Task& task, const std::string& name, const std::string& data) {
    task.setOutput(name, static_cast<const void*>(data.data()), data.size()+1);
}

/**
 * Represents a message exchanged between two instances through a channel 
 * but for python based token as pybind11 doesn't know how to handle void*
 */
struct token_py
{
  /// Whether the exchange succeeded
  bool success;

  /// the buffer, if the exchange succeeded
  std::string buffer;

  /// Size of the token received
  size_t size;
};

/**
 * the LLMEngine::Task getInput method python compatible 
 * as pybind11 doesn't know how to handle void* properly
 */
const token_py getInputWrapper(llmEngine::Task& task, const std::string& name)
{
  const deployr::Channel::token_t tmp_token = task.getInput(name);

  token_py token;
  token.success = tmp_token.success;
  token.buffer  = std::string(static_cast<const char*>(tmp_token.buffer), tmp_token.size);
  token.size    = tmp_token.size;

  printf("the buffer: %s with size: %ld\n", token.buffer.c_str(), token.size); fflush(stdout);

  return token;
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
    .def("initialize", &initialize_engine_from_python)
    .def("run", &convert_run)
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
    .def("getInput", &getInputWrapper)
    .def("setOutput", &setOutputWrapper);


    py::class_<token_py>(m, "token_py")
      .def_readonly("success", &token_py::success)
      .def_readonly("buffer", &token_py::buffer);
}

}   // namespace llmEngine
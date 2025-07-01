project('LLM-Engine', [ 'cpp', 'c'],
  subproject_dir : 'extern',
  default_options : [
  'cpp_std=c++20',
  'buildtype=release'
  ]
)

####### Storage for LLM-Engine dependencies

LLMEngineDependencies = [ ]

# Getting selected distributed engine

engine = get_option('engine')

if engine == 'mpi'
  mpirunExecutable = find_program('mpirun', '/usr/bin/mpirun', '/usr/local/bin/mpirun', required : true)
endif

####### Getting HiCR dependency

# Selecting default HiCR Backends
HiCRBackends = [ 'hwloc', 'pthreads', 'boost', engine ]

HiCRProject = subproject('HiCR', required: true, default_options: [ 'backends=' + ','.join(HiCRBackends), 'frontends=tasking' ])
HiCRBuildDep = HiCRProject.get_variable('hicrBuildDep')
LLMEngineDependencies += HiCRBuildDep

####### Getting DeployR dependency

DeployRProject = subproject('DeployR', required: true, default_options: [ 'engine=' + engine ])
DeployRBuildDep = DeployRProject.get_variable('DeployRBuildDep')
LLMEngineDependencies += DeployRBuildDep

####### Getting TaskR dependency

TaskRProject = subproject('TaskR', required: true, default_options: [  ])
TaskRBuildDep = TaskRProject.get_variable('TaskRBuildDep')
LLMEngineDependencies += TaskRBuildDep

####### Creating LLMEngine dependency

# Warning handling option
warningAsErrorFlags=[]
if get_option('compileWarningsAsErrors') == true
  warningAsErrorFlags=[ '-Werror' ]
endif

LLMEngineBuildCppArgs = [
 '-Wfatal-errors',
  warningAsErrorFlags
 ]

includes = ['include']

if get_option('buildPyLLM')
  includes += ['extern/pybind11_json/include', 'extern/nlohmann/include']
endif

LLMEngineBuildIncludes = include_directories(includes)

####### Collect the dependencies

LLMEngineBuildDep = declare_dependency(
    compile_args: LLMEngineBuildCppArgs,
    include_directories: LLMEngineBuildIncludes,
    dependencies: LLMEngineDependencies
    )

####### Build PyLMM
if get_option('buildPyLLM')
  subdir('include/pyLLM')
endif

####### Build test / example targets only if HiCR is being loaded as a subproject

if meson.is_subproject() == false

  # Build example targets
  if get_option('buildExamples')
  subdir('examples')
  endif

  # Build test targets
  if get_option('buildTests')
  subdir('tests')
  endif

endif

project(
  'hLLM',
  ['cpp', 'c'],
  subproject_dir: 'extern',
  default_options: ['cpp_std=c++20', 'buildtype=release'],
)

####### Storage for hLLM dependencies

hLLMDependencies = []

# Getting selected distributed engine

engine = get_option('engine')

####### Getting HiCR dependency

HiCRDistributedBackend = []
if engine == 'cloudr' or engine == 'mpi'
  mpirunExecutable = find_program('mpirun', '/usr/bin/mpirun', '/usr/local/bin/mpirun', required : true)
  HiCRDistributedBackend = [ 'mpi' ]
endif

# Selecting default HiCR Backends
HiCRBackends = ['hwloc', 'pthreads', 'boost'] + HiCRDistributedBackend
HiCRProject = subproject(
  'HiCR',
  required: true,
  default_options: ['backends=' + ','.join(HiCRBackends), 'frontends=tasking'],
)
HiCRBuildDep = HiCRProject.get_variable('hicrBuildDep')
hLLMDependencies += HiCRBuildDep

if engine == 'cloudr' or engine == 'mpi'
  mpirunExecutable = HiCRProject.get_variable('mpirunExecutable')
endif

####### Getting DeployR dependency

DeployRProject = subproject('DeployR', required: true, default_options: ['engine=' + engine])
DeployRBuildDep = DeployRProject.get_variable('DeployRBuildDep')
hLLMDependencies += DeployRBuildDep

####### Getting TaskR dependency

TaskRProject = subproject('TaskR', required: true, default_options: [])
TaskRBuildDep = TaskRProject.get_variable('TaskRBuildDep')
hLLMDependencies += TaskRBuildDep

####### Creating hLLM dependency

# Warning handling option
warningAsErrorFlags = []
if get_option('compileWarningsAsErrors') == true
  warningAsErrorFlags = ['-Werror']
endif

hLLMBuildCppArgs = ['-Wfatal-errors', warningAsErrorFlags]

includes = ['include']

if get_option('buildPyLLM')
  includes += ['extern/pybind11_json/include', 'extern/nlohmann_json/include']
endif

hLLMBuildIncludes = include_directories(includes)

####### Collect the dependencies

hLLMBuildDep = declare_dependency(
  compile_args: hLLMBuildCppArgs,
  include_directories: hLLMBuildIncludes,
  dependencies: hLLMDependencies,
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
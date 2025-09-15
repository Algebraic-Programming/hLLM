project(
  'hLLM',
  ['cpp', 'c'],
  subproject_dir: 'extern',
  default_options: ['cpp_std=c++20', 'buildtype=release'],
)

####### Storage for hLLM dependencies

hLLMDependencies = []

# Getting selected distributed engines

engines = get_option('engines')

####### Getting HiCR dependency

HiCRDistributedBackend = []
if 'cloudr' in engines or 'mpi' in engines
  HiCRDistributedBackend = ['mpi']
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

if 'cloudr' in engines or 'mpi' in engines
  mpirunExecutable = HiCRProject.get_variable('mpirunExecutable')
endif

####### Getting DeployR dependency

DeployRProject = subproject('DeployR', required: true, default_options: ['engines=' + ','.join(engines)])
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

hLLMBuildIncludes = include_directories(['include'])

####### Collect the dependencies

hLLMBuildDep = declare_dependency(
  compile_args: hLLMBuildCppArgs,
  include_directories: hLLMBuildIncludes,
  dependencies: hLLMDependencies,
)

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
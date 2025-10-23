project(
  'hLLM',
  ['cpp', 'c'],
  subproject_dir: 'extern',
  default_options: ['cpp_std=c++20', 'buildtype=release'],
)

####### Storage for hLLM dependencies

# Warning handling option
warningAsErrorFlags = []
if get_option('compileWarningsAsErrors') == true
  warningAsErrorFlags = ['-Werror']
endif

hLLMDependencies = []
hLLMBuildCppArgs = ['-Wfatal-errors', warningAsErrorFlags]
hLLMBuildIncludes = include_directories(['include'])

# Getting selected distributed engines

engines = get_option('engines')

# Storing required HiCR backends and frontends
additionalHiCRBackends = []
additionalHiCRFrontends = []

####### Getting CloudR dependency (if needed)

if 'cloudr' in engines
  mpirunExecutable = find_program('mpirun', '/usr/bin/mpirun', '/usr/local/bin/mpirun', required : true)
  CloudRProject = subproject('CloudR', required: true, default_options: [  ])
  CloudRBuildDep = CloudRProject.get_variable('CloudRBuildDep')
  hLLMDependencies += CloudRBuildDep
  hLLMBuildCppArgs += '-D_DEPLOYR_DISTRIBUTED_ENGINE_CLOUDR'
  
  CloudRHiCRBackends = CloudRProject.get_variable('CloudRHiCRBackends')
  CloudRHiCRFrontends = CloudRProject.get_variable('CloudRHiCRFrontends')

  additionalHiCRBackends +=  CloudRHiCRBackends
  additionalHiCRFrontends += CloudRHiCRFrontends
endif

####### Getting HiCR dependency

if 'cloudr' in engines or 'mpi' in engines
  if 'mpi' not in additionalHiCRBackends
    additionalHiCRBackends += ['mpi']
  endif
endif

# Selecting default HiCR Backends
HiCRBackends = ['hwloc', 'pthreads', 'boost']

# adding additional backends but only if not already listed
foreach b : additionalHiCRBackends
  if b not in HiCRBackends
    HiCRBackends += [b]
  endif
endforeach

HiCRFrontends = [ ] + additionalHiCRFrontends
HiCRProject = subproject(
  'HiCR',
  required: true,
  default_options: ['backends=' + ','.join(HiCRBackends), 'frontends=' + ','.join(HiCRFrontends)],
)
HiCRBuildDep = HiCRProject.get_variable('hicrBuildDep')
hLLMDependencies += HiCRBuildDep

if 'cloudr' in engines or 'mpi' in engines
  mpirunExecutable = HiCRProject.get_variable('mpirunExecutable')
endif

####### Getting TaskR dependency

TaskRProject = subproject('TaskR', required: true, default_options: [])
TaskRBuildDep = TaskRProject.get_variable('TaskRBuildDep')
hLLMDependencies += TaskRBuildDep

####### Creating hLLM dependency

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
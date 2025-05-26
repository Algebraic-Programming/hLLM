project('LLM-Engine', [ 'cpp', 'c'],
  subproject_dir : 'extern',
  default_options : [
  'cpp_std=c++20',
  'buildtype=release'
  ]
)

####### Storage for DeployR dependencies

deployrDependencies = [ ]

####### Getting HiCR dependency

# Selecting default HiCR Backends
HiCRBackends = [ 'hwloc', 'pthreads' ]

# Getting selected distributed engine

engine = get_option('engine')

# If, selected, adding the dependency for the distributed engine
if engine != 'local'
  HiCRBackends += engine
endif

HiCRProject = subproject('HiCR', required: true, default_options: [ 'backends=' + ','.join(HiCRBackends), 'frontends=RPCEngine,channel' ])
HiCRBuildDep = HiCRProject.get_variable('hicrBuildDep')
deployrDependencies += HiCRBuildDep

####### Getting Hopcroft Karp dependency

HopcroftKarpProject = subproject('hopcroft_karp', required: true)
HopcroftKarpBuildDep = HopcroftKarpProject.get_variable('HopcroftKarpBuildDep')
deployrDependencies += HopcroftKarpBuildDep

##### Handling distributed engine options

if engine == 'mpi'
DeployRDistributedCppFlag = '-D_DEPLOYR_DISTRIBUTED_ENGINE_MPI'
mpirunExecutable = find_program('mpirun', '/usr/bin/mpirun', '/usr/local/bin/mpirun', required : true)
endif

if engine == 'lpf'
DeployRDistributedCppFlag = '-D_DEPLOYR_DISTRIBUTED_ENGINE_LPF'
endif

if engine == 'local'
DeployRDistributedCppFlag = '-D_DEPLOYR_DISTRIBUTED_ENGINE_LOCAL'
endif

####### Creating DeployR dependency

# Warning handling option
warningAsErrorFlags=[]
if get_option('compileWarningsAsErrors') == true
  warningAsErrorFlags=[ '-Werror' ]
endif

DeployRBuildCppArgs = [
DeployRDistributedCppFlag,
 '-Wfatal-errors',
  warningAsErrorFlags
 ]

DeployRBuildIncludes = include_directories([
   'include'
  ])

####### Collect the dependencies

DeployRBuildDep = declare_dependency(
    compile_args: DeployRBuildCppArgs,
    include_directories: DeployRBuildIncludes,
    dependencies: deployrDependencies
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

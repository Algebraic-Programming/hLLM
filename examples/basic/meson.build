testSuite = [ 'examples', 'basic' ]

basic = executable('basic', [ 'basic.cpp'], dependencies: hLLMBuildDep)

if engine == 'mpi'
	if get_option('buildTests')
	  test('basic', mpirunExecutable, args : [ '-np', '3', '--oversubscribe', mpi.full_path(), meson.current_source_dir() + '/basic.json'], timeout: 60, suite: testSuite )
	endif
endif

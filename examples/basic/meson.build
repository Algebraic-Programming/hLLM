testSuite = [ 'examples', 'basic' ]

if engine == 'mpi'
	basic = executable('basic', [ 'basic.cpp'], dependencies: LLMEngineBuildDep)
	if get_option('buildTests')
	  test('basic', mpirunExecutable, args : [ '-np', '3', '--oversubscribe', mpi.full_path(), meson.current_source_dir() + '/basic.json'], timeout: 60, suite: testSuite )
	endif
endif

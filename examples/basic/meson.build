testSuite = [ 'examples', 'basic' ]

if engine == 'mpi'
	basic = executable('basic', [ 'basic.cpp'], dependencies: LLMEngineBuildDep)
	if get_option('buildTests')
	  test('basic', mpirunExecutable, args : [ '-np', '5', '--oversubscribe', basic.full_path(), meson.current_source_dir() + '/config.json'], timeout: 60, suite: testSuite )
	endif
endif

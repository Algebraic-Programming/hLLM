testSuite = [ 'examples', 'basic' ]

basic = executable('basic', [ 'basic.cpp'], dependencies: LLMEngineBuildDep)

if get_option('buildTests')

	if engine == 'mpi'
		test('basic', mpirunExecutable, args : [ '-np', '5', '--oversubscribe', basic.full_path(), meson.current_source_dir() + '/config.json'], timeout: 60, suite: testSuite )
	
		if get_option('buildPyLLM')
			test('pybasic_mpi',
				mpirunExecutable,
				args: ['-np', '5', py.path(), 'basic.py', meson.current_source_dir() + '/config.json'],
				env: ['PYTHONPATH=' + meson.project_build_root() + '/include/pyLLM/'],
				suite: testSuite,
				workdir: meson.current_source_dir())
		endif
	elif engine == 'cloudr'
		test(
			'basic',
			mpirunExecutable,
			env: ['DEPLOYR_CLOUDR_CONFIG_FILE_PATH=' + meson.current_source_dir() + '/cloudr.json'], 
			args : [ '-np', '6', '--oversubscribe', basic.full_path(), meson.current_source_dir() + '/config.json'], 
			timeout: 60, 
			suite: testSuite ) # atm deadlocks in the finalization
	
		if get_option('buildPyLLM')
			test('pybasic_mpi',
				mpirunExecutable,
				args: ['-np', '6', py.path(), 'basic.py', meson.current_source_dir() + '/config.json'],
				env: ['DEPLOYR_CLOUDR_CONFIG_FILE_PATH=' + meson.current_source_dir() + '/cloudr.json', 'PYTHONPATH=' + meson.project_build_root() + '/include/pyLLM/'],
				suite: testSuite,
				workdir: meson.current_source_dir())
		endif
	endif
endif
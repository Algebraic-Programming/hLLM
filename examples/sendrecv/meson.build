testSuite = [ 'examples', 'sendrecv' ]

sendrecv = executable('sendrecv', [ 'sendrecv.cpp'], dependencies: LLMEngineBuildDep)

if get_option('buildTests')

	if engine == 'mpi'
		test('sendrecv', mpirunExecutable, args : [ '-np', '2', '--oversubscribe', sendrecv.full_path(), meson.current_source_dir() + '/config.json'], timeout: 30, suite: testSuite )
	
		if get_option('buildPyLLM')
			test('py_sendrecv',
				mpirunExecutable,
				args: ['-np', '2', py.path(), 'sendrecv.py', meson.current_source_dir() + '/config.json'],
				env: ['PYTHONPATH=' + meson.project_build_root() + '/include/pyLLM/'],
				suite: testSuite,
				workdir: meson.current_source_dir())
		endif
	elif engine == 'cloudr'
		test(
			'sendrecv',
			mpirunExecutable,
			env: ['DEPLOYR_CLOUDR_CONFIG_FILE_PATH=' + meson.current_source_dir() + '/cloudr.json'], 
			args : [ '-np', '3', '--oversubscribe', sendrecv.full_path(), meson.current_source_dir() + '/config.json'], 
			timeout: 30, 
			suite: testSuite ) # atm deadlocks in the finalization
	
		if get_option('buildPyLLM')
			test('py_sendrecv',
				mpirunExecutable,
				args: ['-np', '3', py.path(), 'sendrecv.py', meson.current_source_dir() + '/config.json'],
				env: ['DEPLOYR_CLOUDR_CONFIG_FILE_PATH=' + meson.current_source_dir() + '/cloudr.json', 'PYTHONPATH=' + meson.project_build_root() + '/include/pyLLM/'],
				suite: testSuite,
				workdir: meson.current_source_dir())
		endif
	endif
endif
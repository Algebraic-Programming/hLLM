testSuite = [ 'examples', 'basic' ]

basic = executable('basic', [ 'basic.cpp'], dependencies: LLMEngineBuildDep)

if engine == 'mpi'
	if get_option('buildTests')
		test('basic', mpirunExecutable, args : [ '-np', '5', '--oversubscribe', basic.full_path(), meson.current_source_dir() + '/config.json'], timeout: 60, suite: testSuite )

		if get_option('buildPyLLM')
			test('pybasic_mpi',
				mpirunExecutable,
				args: ['-np', '5', py.path(), 'basic.py'],
				env: ['PYTHONPATH=' + meson.project_build_root() + '/include/pyLLM/'],
				suite: testSuite,
				workdir: meson.current_source_dir())
		endif
	endif
endif
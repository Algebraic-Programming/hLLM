testSuite = ['examples', 'basic']

if 'mpi' in engines
	e = executable('mpi', ['mpi.cpp'], dependencies: hLLMBuildDep)
	if get_option('buildTests')
		test(
			'mpi',
			mpirunExecutable,
			args: [
				'-np', '5',
				'--oversubscribe', e.full_path(),
				meson.current_source_dir() + '/config.json',
			],
			timeout: 60,
			suite: testSuite,
		)
	endif
endif

if 'cloudr' in engines
	e = executable('cloudr', ['cloudr.cpp'], dependencies: hLLMBuildDep)
	if get_option('buildTests')
		test(
			'cloudr',
			mpirunExecutable,
			args: [
				'-np', '6',
				'--oversubscribe', e.full_path(),
				meson.current_source_dir() + '/config.json',
				meson.current_source_dir() + '/cloudr.json'
			],
			timeout: 60,
			suite: testSuite,
		)
	endif
endif
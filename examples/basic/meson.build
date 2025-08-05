testSuite = ['examples', 'basic']

basic = executable('basic', ['basic.cpp'], dependencies: hLLMBuildDep)

if engine == 'mpi'
	if get_option('buildTests')
		test(
			'basic',
			mpirunExecutable,
			args: [
				'-np', '5',
				'--oversubscribe', basic.full_path(),
				meson.current_source_dir() + '/config.json',
			],
			timeout: 60,
			suite: testSuite,
		)
	endif
endif

if engine == 'cloudr'
	if get_option('buildTests')
		test(
			'basic',
			mpirunExecutable,
			args: [
				'-np', '6',
				'--oversubscribe', basic.full_path(),
				meson.current_source_dir() + '/config.json',
			],
			env: {
				'DEPLOYR_CLOUDR_CONFIG_FILE_PATH': meson.current_source_dir() + '/cloudr.json',
			},
			timeout: 60,
			suite: testSuite,
		)
	endif
endif
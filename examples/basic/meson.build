testSuite = ['examples', 'basic']

if 'mpi' in engines
	executable = executable('mpi', ['mpi.cpp'], dependencies: hLLMBuildDep)
	if get_option('buildTests')
		test(
			'mpi',
			mpirunExecutable,
			args: [
				'-np', '5',
				'--oversubscribe', executable.full_path(),
				meson.current_source_dir() + '/config.json',
			],
			timeout: 60,
			suite: testSuite,
		)
	endif
endif

#if 'cloudr' in engines
#	basic = executable('basic', ['cloudr.cpp'], dependencies: hLLMBuildDep)
#	if get_option('buildTests')
#		test(
#			'basic',
#			mpirunExecutable,
#			args: [
#				'-np', '6',
#				'--oversubscribe', basic.full_path(),
#				meson.current_source_dir() + '/config.json',
#			],
#			env: {
#				'DEPLOYR_CLOUDR_CONFIG_FILE_PATH': meson.current_source_dir() + '/cloudr.json',
#			},
#			timeout: 60,
#			suite: testSuite,
#		)
#	endif
#endif
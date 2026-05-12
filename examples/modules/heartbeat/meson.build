testSuite = ['examples', 'modules']

e = executable('heartbeat', ['heartbeat.cpp'], dependencies: hLLMBuildDep)
if get_option('buildTests')
	test(
		'heartbeat',
		mpirunExecutable,
		args: [
			'-np', '5',
			'--oversubscribe', e.full_path(),
			meson.current_source_dir() + '/policy.json',
		],
		timeout: 60,
		suite: testSuite,
	)
endif
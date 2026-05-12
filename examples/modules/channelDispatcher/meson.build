testSuite = ['examples', 'modules']

e = executable('channelDispatcher', ['channelDispatcher.cpp'], dependencies: hLLMBuildDep)
if get_option('buildTests')
	test(
		'channelDispatcher',
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
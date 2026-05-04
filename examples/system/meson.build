testSuite = ['examples', 'system']

e = executable('system', ['system.cpp'], dependencies: hLLMBuildDep)
if get_option('buildTests')
	test(
		'system',
		mpirunExecutable,
		args: [
			'-np', '4',
			'--oversubscribe', e.full_path(),
			'4',
		],
		timeout: 60,
		suite: testSuite,
	)
endif
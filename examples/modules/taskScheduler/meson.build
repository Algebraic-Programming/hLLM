testSuite = ['examples', 'modules']

e = executable('taskScheduler', ['taskScheduler.cpp'], dependencies: hLLMBuildDep)
if get_option('buildTests')
	test(
		'taskScheduler',
		mpirunExecutable,
		args: [
			'-np', '5',
			'--oversubscribe', e.full_path()
		],
		timeout: 60,
		suite: testSuite,
	)
endif
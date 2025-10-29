testSuite = ['examples', 'basic']

e = executable('basic', ['basic.cpp'], dependencies: hLLMBuildDep)
if get_option('buildTests')
	test(
		'basic',
		mpirunExecutable,
		args: [
			'-np', '1',
			'--oversubscribe', e.full_path(),
			meson.current_source_dir() + '/config.json',
		],
		timeout: 60,
		suite: testSuite,
	)
endif
# torch executable for the kernels
libtorch_root = '/home/nbaumann/library/libtorch'

torch_inc = include_directories(
  join_paths(libtorch_root, 'include'),
  join_paths(libtorch_root, 'include', 'torch', 'csrc', 'api', 'include')
)

torch_libs = [
  join_paths(libtorch_root, 'lib', 'libtorch.so'),
  join_paths(libtorch_root, 'lib', 'libtorch_cpu.so'),
  join_paths(libtorch_root, 'lib', 'libc10.so')
]

testSuite = ['examples', 'serving']

if 'mpi' in engines
	executable = executable(
		'mpi', 
		['mpi.cpp'], 
		dependencies: [hLLMBuildDep],
		include_directories: torch_inc,
		link_args: torch_libs,
		install_rpath: join_paths(libtorch_root, 'lib')
		)

	if get_option('buildTests')
		test(
			'mpi',
			mpirunExecutable,
			args: [
				'-np', '4',
				'--oversubscribe', executable.full_path(),
				meson.current_source_dir() + '/config.json',
			],
			timeout: 30,
			suite: testSuite,
		)
	endif

	# For me for testing
	attention_example = executable(
		'attention_example',
		'attention_example.cpp',
		include_directories: torch_inc,
		link_args: torch_libs,
		install_rpath: join_paths(libtorch_root, 'lib')
	)
	if get_option('buildTests')
		test(
			'attention_example',
			attention_example,
			timeout: 30,
			suite: testSuite,
		)
	endif
endif

#if 'cloudr' in engines
#	basic = executable('basic', ['cloudr.cpp'], dependencies: [hLLMBuildDep, libtorch_dep])
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
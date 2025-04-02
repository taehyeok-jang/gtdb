# ---------------------------------------------------------------------------
# BENCHMARK
# ---------------------------------------------------------------------------

# make check-benchmarks

add_custom_target(check-benchmarks)

file(GLOB_RECURSE BENCHMARKS_CC
	 ${CMAKE_SOURCE_DIR}/test/benchmark/*/*.cc
)

# make XYZ_test

foreach (benchmark_source ${BENCHMARKS_CC})

    # Get a human readable name
    get_filename_component(benchmark_filename ${benchmark_source} NAME)
    string(REPLACE ".cc" "" benchmark_name ${benchmark_filename})

    # Add the benchmark target separately and as part of "make check-benchmarks".
    add_executable(${benchmark_name} EXCLUDE_FROM_ALL ${benchmark_source})
    add_dependencies(check-benchmarks ${benchmark_name})

    target_link_libraries(${benchmark_name} 
    					  ${SHARED_LIBRARY} 
    					  benchmark
    					  gtest  
    					  Threads::Threads)
    					  
    # Set benchmark target properties and dependencies.
    set_target_properties(${benchmark_name}
        PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/benchmark"
        COMMAND ${benchmark_name}
    )     
                
endforeach(benchmark_source ${BENCHMARKS_CC})

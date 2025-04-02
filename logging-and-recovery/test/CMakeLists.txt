# ---------------------------------------------------------------------------
# TESTS
# ---------------------------------------------------------------------------

enable_testing()

# make check-tests

add_custom_target(check-tests COMMAND ${CMAKE_CTEST_COMMAND})
add_custom_target(check-tests-valgrind COMMAND ${CMAKE_CTEST_COMMAND})

file(GLOB_RECURSE TEST_CC
#	 ${CMAKE_SOURCE_DIR}/test/unit/*.cc 
	 ${CMAKE_SOURCE_DIR}/test/unit/*/*.cc
)

# make XYZ_test

foreach (test_source ${TEST_CC})

    # Get a human readable name
    get_filename_component(test_filename ${test_source} NAME)
    string(REPLACE ".cc" "" test_name ${test_filename})

    # Add the test target separately and as part of "make check-tests".
    add_executable(${test_name}  ${test_source})
    add_dependencies(check-tests ${test_name})
    add_executable(${test_name}_valgrind  ${test_source})
    add_dependencies(check-tests-valgrind ${test_name}_valgrind)

    target_link_libraries(${test_name} 
    					  ${SHARED_LIBRARY} 
    					  benchmark
    					  gtest  gmock
    					  Threads::Threads)

	  target_link_libraries(${test_name}_valgrind 
    					  ${SHARED_LIBRARY} 
    					  benchmark
    					  gtest  gmock
    					  Threads::Threads)
    					  
    # Set test target properties and dependencies.
    set_target_properties(${test_name}
        PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/test"
        COMMAND ${test_name}
    )
    
    set_target_properties(${test_name}_valgrind
        PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/test"
        COMMAND ${test_name}_valgrind
    )    

    # Add the test under CTest.
    add_test(${test_name} 
   			  ${CMAKE_BINARY_DIR}/test/${test_name}
    		 --gtest_color=yes 
    		 --gtest_output=xml:${CMAKE_BINARY_DIR}/test/unit_${test_name}.xml
    )	 
    
    # Add the test under CTest.
    add_test(${test_name}_valgrind  
			  ${VALGRIND_BIN} 
              ${VALGRIND_OPTIONS}    
   			 --suppressions=${VALGRIND_SUPPRESSIONS_FILE}
   			  ${CMAKE_BINARY_DIR}/test/${test_name}_valgrind
    		 --gtest_color=yes 
    		 --gtest_output=xml:${CMAKE_BINARY_DIR}/test/unit_${test_name}_valgrind.xml
    )
            
endforeach(test_source ${TEST_CC})

# make check

add_custom_target(check)

add_dependencies(check 
				 check-tools
				 check-benchmarks				 
				 check-tests 
#                 check-clang-tidy
#                 check-clang-format
#                 check-cpplint
)

# control ordering
add_dependencies(check-tests check-tools)
add_dependencies(check-tests check-benchmarks)


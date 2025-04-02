# ---------------------------------------------------------------------------
# TOOLS
# ---------------------------------------------------------------------------

# make check-tools

add_custom_target(check-tools)

file(GLOB_RECURSE TOOLS_CC
	 ${CMAKE_SOURCE_DIR}/test/tool/*/*.cc
)

# make XYZ_test

foreach (tool_source ${TOOLS_CC})

    # Get a human readable name
    get_filename_component(tool_filename ${tool_source} NAME)
    string(REPLACE ".cc" "" tool_name ${tool_filename})

    # Add the tool target separately and as part of "make check-tools".
    add_executable(${tool_name} ${tool_source})
    add_dependencies(check-tools ${tool_name})

    target_link_libraries(${tool_name} 
    					  ${SHARED_LIBRARY} 
    					  gtest gmock 
    					  Threads::Threads)
    					  
    # Set tool target properties and dependencies.
    set_target_properties(${tool_name}
        PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/tool"
        COMMAND ${tool_name}
    )     
                
endforeach(tool_source ${TOOLS_CC})


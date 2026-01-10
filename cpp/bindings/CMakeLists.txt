# Optionally gate building bindings (in case a pure C++ build is needed)
option(MSRL_BUILD_PYTHON "Build Python bindings" ON)

if (NOT MSRL_BUILD_PYTHON)
  message(STATUS "MSRL_BUILD_PYTHON=OFF: skipping Python bindings")
  return()
endif()

# Define ONLY module target;
# Python + nanobind are already configured in the parent
nanobind_add_module(_core
  microstructure_nb.cpp
)

set_target_properties(_core PROPERTIES OUTPUT_NAME "_core")

target_include_directories(_core PRIVATE
  ${CMAKE_CURRENT_LIST_DIR}/../include
)

# Link to stable aliases created in the parent
target_link_libraries(_core PRIVATE
  msrl::sim
  msrl::replay
)

if (MSVC)
  target_compile_options(_core PRIVATE /W4 /permissive- /EHsc /bigobj)
  target_compile_options(_core PRIVATE
    $<$<CONFIG:Release>:/O2>
    $<$<CONFIG:RelWithDebInfo>:/O2>
    $<$<CONFIG:MinSizeRel>:/O1>
  )
else()
  target_compile_options(_core PRIVATE -Wall -Wextra -Wpedantic)
endif()

install(TARGETS _core
  LIBRARY DESTINATION microstructure_rl
  RUNTIME DESTINATION microstructure_rl
)

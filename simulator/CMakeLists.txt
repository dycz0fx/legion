cmake_minimum_required(VERSION 3.1)

# set the project name
project(simulator)

# specify the C++ standard
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_BUILD_TYPE Debug)

add_library (simulator simulator.cc machine_model.cc)

# add the executable
add_executable(main main.cc)

# add the libraries
target_link_libraries(main simulator)


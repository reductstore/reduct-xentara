cmake_minimum_required(VERSION 3.25)

project(reductstore-xentara VERSION 0.1.0 LANGUAGES CXX)

include(CTest)
include(FetchContent)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED YES)
set(CMAKE_CXX_EXTENSIONS NO)

find_package(XentaraUtils 2.1.1 REQUIRED)
find_package(XentaraPlugin 2.1.1 REQUIRED)

set(REDUCT_CPP_USE_FETCHCONTENT ON CACHE BOOL "" FORCE)
FetchContent_Declare(
	reductcpp
	GIT_REPOSITORY https://github.com/reductstore/reduct-cpp.git
	GIT_TAG 6bddc6ac7d428f8c9313967ca87865b2a243ab02
	GIT_SHALLOW TRUE
)
FetchContent_MakeAvailable(reductcpp)

add_library(reductstore-xentara-core STATIC
	src/core/Batching.cpp
	src/core/Configuration.cpp
	src/core/Serialization.cpp
	src/core/Transmission.cpp
)
target_include_directories(reductstore-xentara-core PUBLIC include)
target_link_libraries(reductstore-xentara-core PUBLIC reductcpp::reductcpp)
target_compile_features(reductstore-xentara-core PUBLIC cxx_std_20)
set_target_properties(reductstore-xentara-core PROPERTIES POSITION_INDEPENDENT_CODE ON)

add_library(reductstore-xentara MODULE
	src/plugin/Plugin.cpp
)
target_link_libraries(reductstore-xentara PRIVATE reductstore-xentara-core Xentara::xentara-utils Xentara::xentara-plugin)
target_compile_features(reductstore-xentara PRIVATE cxx_std_20)
set_target_properties(reductstore-xentara PROPERTIES PREFIX "lib")
install_xentara_plugin(reductstore-xentara NAME "ReductStore")

if(BUILD_TESTING)
	add_executable(reductstore-xentara-core-tests tests/CoreTests.cpp)
	target_link_libraries(reductstore-xentara-core-tests PRIVATE reductstore-xentara-core)
	add_test(NAME reductstore-xentara-core-tests COMMAND reductstore-xentara-core-tests)
	option(REDUCTSTORE_XENTARA_ENABLE_INTEGRATION_TESTS "Enable tests requiring a local ReductStore server" OFF)
	if(REDUCTSTORE_XENTARA_ENABLE_INTEGRATION_TESTS)
		add_executable(reductstore-xentara-integration-tests tests/IntegrationTests.cpp)
		target_link_libraries(reductstore-xentara-integration-tests PRIVATE reductstore-xentara-core)
		add_test(NAME reductstore-xentara-integration-tests COMMAND reductstore-xentara-integration-tests)
	endif()
endif()

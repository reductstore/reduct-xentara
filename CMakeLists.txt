cmake_minimum_required(VERSION 3.25)

# TODO: rename project and add description, URL etc.
project(xentara-template-uplink LANGUAGES CXX)

# Force the use of C++ 20
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED YES)

# Tell MSVC to set __cplusplus to the correct value
if(CMAKE_CXX_COMPILER_ID STREQUAL "MSVC")
	add_compile_options("/Zc:__cplusplus")
endif()

# Find the Xentara utility and plugin libraries
find_package(XentaraUtils REQUIRED)
find_package(XentaraPlugin REQUIRED)

# Add the plugin library target
add_library(
	${PROJECT_NAME} MODULE

	"src/Attributes.cpp"
	"src/Attributes.hpp"
	"src/CustomError.cpp"
	"src/CustomError.hpp"
	"src/Events.cpp"
	"src/Events.hpp"
	"src/Skill.cpp"
	"src/Skill.hpp"
	"src/Tasks.cpp"
	"src/Tasks.hpp"
	"src/TemplateClient.cpp"
	"src/TemplateClient.hpp"
	"src/TemplateRecord.cpp"
	"src/TemplateRecord.hpp"
	"src/TemplateTransaction.cpp"
	"src/TemplateTransaction.hpp"
)

# Link against the Xentara utility and plugin libraries
target_link_libraries(
	${PROJECT_NAME}

	PRIVATE
		Xentara::xentara-utils
		Xentara::xentara-plugin
)

# Make output names adhere to Xentara convetions under Windows
if(CMAKE_SYSTEM_NAME STREQUAL "Windows")
	set_target_properties(
		${PROJECT_NAME}
		
		PROPERTIES
# TODO: adjust output name to match project name
			OUTPUT_NAME XentaraTemplateUplink
			DEBUG_POSTFIX d
	)
endif()

# Generate the plugin manifest and add the plugin files to the install target
install_xentara_plugin(${PROJECT_NAME})

# Try to find Doxygen
find_package(Doxygen QUIET)

# Generate Doxygen documentation, if Doxygen was found
if(Doxygen_FOUND)
	# Get the list of source files from the library
	get_target_property(target_sources ${PROJECT_NAME} SOURCES)

	# Set some useful Doxygen options
	set(DOXYGEN_QUIET YES)
	set(DOXYGEN_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/docs")
	set(DOXYGEN_EXTRACT_PRIVATE YES)
	set(DOXYGEN_GENERATE_TREEVIEW YES)
	set(DOXYGEN_EXTRACT_ANON_NSPACES YES)
	set(DOXYGEN_SUBGROUPING NO)
	set(DOXYGEN_WARN_IF_UNDOCUMENTED NO)

	# Under Windows, Doxygen generates file names that are too long for it to handle, resulting in
	# errors when generating the documentation. As a workaround, we tell Doxygen to generate short file names.
	if(CMAKE_HOST_SYSTEM_NAME STREQUAL "Windows")
		set(DOXYGEN_SHORT_NAMES YES)
	endif()

	# Add the tag files for the Xentara documentation
	set(DOXYGEN_TAGFILES
		"${XentaraUtils_DOXYGEN_TAG_FILE}=https://docs.xentara.io/xentara-utils"
		"${XentaraPlugin_DOXYGEN_TAG_FILE}=https://docs.xentara.io/xentara-plugin"
	)

	# Add a target to generate the Doxygen documentation
	doxygen_add_docs(docs ${target_sources})
endif()

# Batch Processing Data Uplink Template for Xentara
This repository contains skeleton code for a Xentara data uplink with the following features:

Connection based | Batch processing
:--------------: | :--------------:
NO               | YES

## Prerequisites

This skill template requires the Xentara development environment, as well as a Xentara licence. You can get a Xentara
licence in the [Xentara Online Shop](https://www.xentara.io/product/xentara-for-industrial-automation/).

The documentation for Xentara can be found at https://docs.xentara.io/xentara.

This skill template uses the Xentara Utility Library, as well as the Xentara Plugin Framework. The corresponding documentation can be found here:

https://docs.xentara.io/xentara-utils/  
https://docs.xentara.io/xentara-plugin/

## Build Environment

The repository include a [CMakeLists.txt](CMakeLists.txt) file for use with [CMake](https://cmake.org/), which allows you to build the source code
out of the box, as long as the Xentara development environment is installed. If you whish to use a different build system, you must generate the
necessary build configuration file yourself.

## Source Code Documentation

The source code in this repository is documented using [Doxygen](https://doxygen.nl/) comments. If you have Doxygen installed, you can
generate detailed documentation for all classes, functions etc., including a TODO list. To generate the documentation using CMake, just
build the target *docs* by executing the following command in your [build directory](https://cmake.org/cmake/help/latest/manual/cmake.1.html#generate-a-project-buildsystem):

~~~sh
cmake --build . --target docs
~~~

This will generate HTML documentation in the subdirectory *docs/html*.

## Xentara Skill Element Templates

*(See [Skill Elements](https://docs.xentara.io/xentara/xentara_skills.html#xentara_skill_elements) in the [Xentara documentation](https://docs.xentara.io/xentara/))*

### Client Template

[src/TemplateClient.hpp](src/TemplateClient.hpp)  
[src/TemplateClient.cpp](src/TemplateClient.cpp)

The client skill element template provides template code for services that do not require a persistent connection to the service instance.
Such services are either entirely stateless, or require a single initialization and deinitialization call. This includes web services
that use the HTTP or HTTPs protocols, for example.

### Transaction Template

[src/TemplateTransaction.hpp](src/TemplateTransaction.hpp)  
[src/TemplateTransaction.cpp](src/TemplateTransaction.cpp)

The transaction skill element template provides template code for transactions that accumulate data over a period of time, and only send it to the service instance
at greater intervals.

The template code has the following features:

- The skill element publishes a [Xentara task](https://docs.xentara.io/xentara/xentara_element_members.html#xentara_tasks) called *collect*,
  which collect a set of records and buffers it for future sending.
- The skill element publishes a [Xentara task](https://docs.xentara.io/xentara/xentara_element_members.html#xentara_tasks) called *send*,
  which sends the collected records to the service instance.
- The *send* task can be executed at greater intervals than the *collect* task, to collect multiple sets of records and send them to the
  service instance using a single transaction
- The skill element publishes [Xentara events](https://docs.xentara.io/xentara/xentara_element_members.html#xentara_events) to signal when
  a transaction was sent, or if a send error occurred.

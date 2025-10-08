cmake_minimum_required(VERSION 3.22)
project(PqxxExecutor VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -lpthread")
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

find_package(PostgreSQL REQUIRED)

add_library(PostgreSQLConnection SHARED src/PostgreSQLConnection.cpp)
target_link_libraries(PostgreSQLConnection PostgreSQL::PostgreSQL)

add_library(PostgreSQLQuery SHARED src/PostgreSQLQuery.cpp)
target_link_libraries(PostgreSQLQuery PostgreSQL::PostgreSQL
                      PostgreSQLConnection)

add_library(PostgreSQLUtils SHARED src/PostgreSQLUtils.cpp)
target_link_libraries(PostgreSQLUtils PostgreSQL::PostgreSQL PostgreSQLQuery)

add_executable(PqxxExecutor main.cpp)
target_link_libraries(PqxxExecutor PostgreSQLUtils)

# Install targets and create export set
install(
  TARGETS PostgreSQLConnection PostgreSQLQuery PostgreSQLUtils
  EXPORT PqxxExecutorTargets
  LIBRARY DESTINATION lib/pqxx-executor
  ARCHIVE DESTINATION lib/pqxx-executor
  RUNTIME DESTINATION bin
)

install(FILES include/PostgreSQLConnection.h include/PostgreSQLQuery.h
              include/PostgreSQLUtils.h
        DESTINATION include/pqxx-executor)

# Create and install package configuration files
include(CMakePackageConfigHelpers)

# Create a version file
write_basic_package_version_file(
    "${CMAKE_CURRENT_BINARY_DIR}/PqxxExecutorConfigVersion.cmake"
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY AnyNewerVersion
)

# Create configuration file
configure_package_config_file(
    "${CMAKE_SOURCE_DIR}/cmake/PqxxExecutorConfig.cmake.in"
    "${CMAKE_CURRENT_BINARY_DIR}/PqxxExecutorConfig.cmake"
    INSTALL_DESTINATION lib/cmake/PqxxExecutor
)

# Install the configuration files
install(
    FILES
        "${CMAKE_CURRENT_BINARY_DIR}/PqxxExecutorConfig.cmake"
        "${CMAKE_CURRENT_BINARY_DIR}/PqxxExecutorConfigVersion.cmake"
    DESTINATION lib/cmake/PqxxExecutor
)

# Export targets for find_package
install(EXPORT PqxxExecutorTargets
    FILE PqxxExecutorTargets.cmake
    NAMESPACE PqxxExecutor::
    DESTINATION lib/cmake/PqxxExecutor
)

export(EXPORT PqxxExecutorTargets
    FILE "${CMAKE_CURRENT_BINARY_DIR}/PqxxExecutorTargets.cmake"
    NAMESPACE PqxxExecutor::
)

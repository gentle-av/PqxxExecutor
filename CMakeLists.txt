cmake_minimum_required(VERSION 3.22)
project(PqxxExecutor)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -lpthread")
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

find_package(PostgreSQL REQUIRED)

add_library(PostgreSQLConnection src/PostgreSQLConnection.cpp)
target_link_libraries(PostgreSQLConnection PostgreSQL::PostgreSQL)

add_library(PostgreSQLQuery src/PostgreSQLQuery.cpp)
target_link_libraries(PostgreSQLQuery PostgreSQL::PostgreSQL
                      PostgreSQLConnection)

add_library(PostgreSQLUtils src/PostgreSQLUtils.cpp)
target_link_libraries(PostgreSQLUtils PostgreSQL::PostgreSQL PostgreSQLQuery)

add_executable(PqxxExecutor main.cpp)
target_link_libraries(PqxxExecutor PostgreSQLUtils)

cmake_minimum_required(VERSION 2.8)
project(savime)

set(CMAKE_VERBOSE_MAKEFILE ON)
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -DDEBUG \
    -DTIME -rdynamic -m64 -fpic -fopenmp")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE}  \
    -DTIME -rdynamic -m64 -fpic -fopenmp -O3")

option(BUILD_SAVIME_CLIENT "Build Savime client" ON)
option(BUILD_WITH_RDMA "Build Savime with RDMA staging library" OFF)
option(BUILD_WITH_CATALYST "Build with Catalyst library support" OFF)
option(BUILD_WITH_FULL_TYPE_SUPPORT "Build with support for 8/16 bit
       signed and unsigned integers. Takes a lot of time to compile." OFF)
option(REGENEREATE_PARSER "Regenerate Flex and BISON files." OFF)


list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR})
find_package(TBB)


include_directories(.)
include_directories(configuration)
include_directories(connection)
include_directories(core/include)
include_directories(engine/include)
include_directories(lib)
include_directories(metada)
include_directories(optimizer)
include_directories(parser)
include_directories(query)
include_directories(session)
include_directories(storage)

set (Savime_PACKAGE_VERSION \"1.0\")
configure_file (
        "${PROJECT_SOURCE_DIR}/config.h.in"
        "${PROJECT_BINARY_DIR}/config.h"
)


if(REGENEREATE_PARSER)
    find_package(BISON)
    find_package(FLEX)
    execute_process(COMMAND ${PROJECT_SOURCE_DIR}/parser/build_grammar.sh
            OUTPUT_VARIABLE BUILD_GRAMMAR_OUT)
    message(STATUS ${BUILD_GRAMMAR_OUT})
endif(REGENEREATE_PARSER)


add_library(savime STATIC lib/savime_lib.cpp)
install(TARGETS savime DESTINATION lib)

if(BUILD_SAVIME_CLIENT)
    add_executable(savimec client/client.cpp)
    target_link_libraries(savimec savime)
    install (TARGETS savimec DESTINATION bin)
endif(BUILD_SAVIME_CLIENT)

if(BUILD_WITH_FULL_TYPE_SUPPORT)
    add_definitions( -DFULL_TYPE_SUPPORT )
endif(BUILD_WITH_FULL_TYPE_SUPPORT)

if(BUILD_WITH_CATALYST)

    #Get include and lib paths for catalyst
    execute_process(COMMAND find /usr/local/lib/ -maxdepth 1 -type d
        COMMAND grep paraview OUTPUT_VARIABLE VTK_LIB_FILES
        OUTPUT_STRIP_TRAILING_WHITESPACE)

    string(REGEX REPLACE "\n$" " " VTK_LIB_FILES ${VTK_LIB_FILES})
    execute_process(COMMAND find ${VTK_LIB_FILES} -maxdepth 1 -type f
            -name libvtk* OUTPUT_VARIABLE VTK_LIB_FILES
            OUTPUT_STRIP_TRAILING_WHITESPACE)
    string(REGEX REPLACE "\n" ";" VTK_LIB_FILES ${VTK_LIB_FILES})
    message(STATUS "VTK LIBs Found: ${VTK_LIB_FILES}")

    execute_process(COMMAND find /usr/local/include/ -maxdepth 1 -type d
            COMMAND grep paraview OUTPUT_VARIABLE VTK_INC_FILES)
    string(REGEX REPLACE "\n$" "" VTK_INC_FILES ${VTK_INC_FILES})
    message(STATUS "VTK include dir used: ${VTK_INC_FILES}")
    include_directories(${VTK_INC_FILES})

    add_definitions( -DCATALYST )

    add_executable(savime-bin
        configuration/default_config_manager.cpp
        configuration/default_config_manager.h
        connection/default_connection_manager.cpp
        connection/default_connection_manager.h
        core/include/abstract_storage_manager.h
        core/include/builder.h
        core/include/config_manager.h
        core/include/connection_manager.h
        core/include/dynamic_bitset.h
        core/include/engine.h
        core/include/metadata.h
        core/include/optimizer.h
        core/include/parser.h
        core/include/query_data_manager.h
        core/include/rtree.h
        core/include/savime.h
        core/include/savime_hash.h
        core/include/session.h
        core/include/storage_manager.h
        core/include/system_logger.h
        core/include/types.h
        core/include/util.h
        core/builder.cpp
        core/main.cpp
        core/metadata.cpp
        core/query_data_manager.cpp
        core/system_logger.cpp
        engine/include/aggregate_buffer.h
        engine/include/aggregate_config.h
        engine/include/aggregate_engine.h
        engine/include/create_tar.h
        engine/include/ddl_operators.h
        engine/include/default_engine.h
        engine/include/dimjoin.h
        engine/include/dml_operators.h
        engine/include/filter.h
        engine/include/load_subtar.h
        engine/include/subset.h
        engine/ddl_operators.cpp
        engine/default_engine.cpp
        engine/dml_operators.cpp
        engine/viz.cpp
        lib/protocol.h
        lib/savime_lib.cpp
        lib/savime_lib.h
        metada/default_metada_manager.h
        metada/default_metadata_manager.cpp
        optimizer/default_optimizer.cpp
        optimizer/default_optimizer.h
        parser/bison.cpp
        parser/bison.h
        parser/default_parser.cpp
        parser/default_parser.h
        parser/flex.cpp
        parser/flex.h
        parser/schema_builder.cpp
        parser/schema_builder.h
        parser/tree.h
        query/default_query_data_manager.cpp
        query/default_query_data_manager.h
        session/default_session.cpp
        session/default_session.h
        session/default_session_manager.cpp
        session/default_session_manager.h
        storage/default_apply_template.h
        storage/default_storage_manager.cpp
        storage/default_storage_manager.h
        storage/default_template.h
        storage/template_builder.h core/include/symbols.h)
    find_package(OpenMP REQUIRED)
    target_compile_options(savime-bin PRIVATE ${OpenMP_CXX_FLAGS})
    target_link_libraries(savime-bin ${OpenMP_CXX_FLAGS})
    set_target_properties(savime-bin
            PROPERTIES OUTPUT_NAME savime)

    target_link_libraries(savime-bin ${VTK_LIB_FILES})

    if(TBB_FOUND)
        message(STATUS "TBB library found")
        target_link_libraries(savime-bin ${TBB_LIBRARIES})
        target_include_directories(savime-bin PRIVATE ${TBB_INCLUDE_DIRS})
        add_definitions( -DTBB_SUPPORT )
    endif(TBB_FOUND)

    find_package(OpenMP REQUIRED)
    target_compile_options(savime-bin PRIVATE ${OpenMP_CXX_FLAGS})
    target_link_libraries(savime-bin ${OpenMP_CXX_FLAGS})
    install(TARGETS savime-bin DESTINATION bin)

    add_executable(savime-catalyst engine/catalyst.cpp)
    target_link_libraries(savime-catalyst ${VTK_LIB_FILES})
    target_link_libraries(savime-catalyst ${OpenMP_CXX_FLAGS})
    install(TARGETS savime-catalyst DESTINATION bin)

else(BUILD_WITH_CATALYST)

    add_executable(savime-bin
            configuration/default_config_manager.cpp
            configuration/default_config_manager.h
            connection/default_connection_manager.cpp
            connection/default_connection_manager.h
            core/include/abstract_storage_manager.h
            core/include/builder.h
            core/include/config_manager.h
            core/include/connection_manager.h
            core/include/dynamic_bitset.h
            core/include/engine.h
            core/include/metadata.h
            core/include/optimizer.h
            core/include/parser.h
            core/include/query_data_manager.h
            core/include/rtree.h
            core/include/savime.h
            core/include/savime_hash.h
            core/include/session.h
            core/include/storage_manager.h
            core/include/system_logger.h
            core/include/types.h
            core/include/util.h
            core/builder.cpp
            core/main.cpp
            core/metadata.cpp
            core/query_data_manager.cpp
            core/system_logger.cpp
            engine/include/aggregate_buffer.h
            engine/include/aggregate_config.h
            engine/include/aggregate_engine.h
            engine/include/create_tar.h
            engine/include/ddl_operators.h
            engine/include/default_engine.h
            engine/include/dimjoin.h
            engine/include/dml_operators.h
            engine/include/filter.h
            engine/include/load_subtar.h
            engine/include/subset.h
            engine/ddl_operators.cpp
            engine/default_engine.cpp
            engine/dml_operators.cpp
            lib/protocol.h
            lib/savime_lib.cpp
            lib/savime_lib.h
            metada/default_metada_manager.h
            metada/default_metadata_manager.cpp
            optimizer/default_optimizer.cpp
            optimizer/default_optimizer.h
            parser/bison.cpp
            parser/bison.h
            parser/default_parser.cpp
            parser/default_parser.h
            parser/flex.cpp
            parser/flex.h
            parser/schema_builder.cpp
            parser/schema_builder.h
            parser/tree.h
            query/default_query_data_manager.cpp
            query/default_query_data_manager.h
            session/default_session.cpp
            session/default_session.h
            session/default_session_manager.cpp
            session/default_session_manager.h
            storage/default_apply_template.h
            storage/default_storage_manager.cpp
            storage/default_storage_manager.h
            storage/default_template.h
            storage/template_builder.h core/include/symbols.h)

    set_target_properties(savime-bin
            PROPERTIES OUTPUT_NAME savime)

    target_compile_options(savime-bin PRIVATE ${OpenMP_CXX_FLAGS})
    target_link_libraries(savime-bin ${OpenMP_CXX_FLAGS})

    if(TBB_FOUND)
        message(STATUS "TBB library found")
        target_link_libraries(savime-bin ${TBB_LIBRARIES})
        target_include_directories(savime-bin PRIVATE ${TBB_INCLUDE_DIRS})
        add_definitions( -DTBB_SUPPORT )
    endif(TBB_FOUND)

    install (TARGETS savime-bin DESTINATION bin)

endif(BUILD_WITH_CATALYST)

if(BUILD_WITH_RDMA)

    add_definitions( -DSTAGING )
    add_library(savime-mapped-memory STATIC
            staging/mapped_memory/mapped_memory.cpp)
    target_link_libraries(savime-mapped-memory pthread)
    install (TARGETS savime-mapped-memory DESTINATION lib)

    add_library(savime-rdmap STATIC
            staging/rdmap/rdmap.cpp)
    target_link_libraries(savime-rdmap pthread)
    install (TARGETS savime-rdmap DESTINATION lib)

    add_library(savime-staging STATIC
            staging/rdmap/rdmap.cpp
            staging/rdma_utils.cpp
            staging/staging.cpp
            lib/savime_lib.cpp)
    target_link_libraries(savime-staging  rdmacm)
    target_link_libraries(savime-staging  ibverbs)
    install (TARGETS savime-staging DESTINATION lib)

    add_executable(savime-stserver staging/server.cpp)
    target_link_libraries(savime-stserver savime-staging)
    target_link_libraries(savime-stserver savime-mapped-memory)
    target_link_libraries(savime-stserver savime-rdmap)
    target_link_libraries(savime-stserver rdmacm)
    target_link_libraries(savime-stserver ibverbs)

else(BUILD_WITH_RDMA)

    add_library(savime-staging STATIC
            staging/staging_dummy.cpp)
    install (TARGETS savime-staging DESTINATION lib)

endif(BUILD_WITH_RDMA)
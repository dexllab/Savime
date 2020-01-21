cmake_minimum_required(VERSION 2.8)
project(savime)

set(CMAKE_VERBOSE_MAKEFILE ON)
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -DDEBUG \
    -g -DTIME -rdynamic -m64 -fpic -fopenmp -lboost_regex -lboost_iostreams -ljsoncpp -O0")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE}  \
    -g -DTIME -rdynamic -m64 -fpic -fopenmp -lboost_regex -lboost_iostreams -ljsoncpp -O3")


option(BUILD_SAVIME_CLIENT "Build Savime client" ON)
option(BUILD_SAVIME_TESTS "Build Savime Test Suit" ON)
option(BUILD_WITH_RDMA "Build Savime with RDMA staging library" OFF)
option(BUILD_WITH_CATALYST "Build with Catalyst library support" OFF)
option(BUILD_WITH_FULL_TYPE_SUPPORT "Build with support for 8/16 bit
       signed and unsigned integers. Takes a lot of time to compile." OFF)
option(REGENEREATE_PARSER "Regenerate Flex and Bison files." OFF)

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



find_package(CURL REQUIRED)
include_directories(${CURL_INCLUDE_DIR})

if(REGENEREATE_PARSER)
    find_package(BISON)
    find_package(FLEX)
    execute_process(COMMAND ${PROJECT_SOURCE_DIR}/parser/build_grammar.sh
            OUTPUT_VARIABLE BUILD_GRAMMAR_OUT)
    message(STATUS ${BUILD_GRAMMAR_OUT})
endif(REGENEREATE_PARSER)


add_library(savime STATIC lib/savime_lib.cpp engine/include/json_parser.h engine/predictor.cpp engine/include/predictor.h)
install(TARGETS savime DESTINATION lib)

if(BUILD_SAVIME_CLIENT)
    add_executable(savimec client/client.cpp)
    target_link_libraries(savimec savime)
    install (TARGETS savimec DESTINATION bin)
endif(BUILD_SAVIME_CLIENT)

if(BUILD_WITH_FULL_TYPE_SUPPORT)
    add_definitions( -DFULL_TYPE_SUPPORT )
endif(BUILD_WITH_FULL_TYPE_SUPPORT)

if(BUILD_SAVIME_TESTS)

    add_executable(savime-test
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
    core/metadata.cpp
    core/query_data_manager.cpp
    core/system_logger.cpp
            engine/misc/json.cpp
            engine/misc/include/json.h
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
            engine/default_engine.cpp
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
    storage/template_builder.h core/include/symbols.h
    test/test_main.cpp test/storage/mock_default_storage_manager.h test/mock_builder.cpp test/mock_builder.h
    test/engine/mock_engine.cpp test/engine/mock_engine.h test/parser/mock_parser.h
    test/metadata/metadata_test.cpp test/metadata/metadata_test.h test/query/mock_query_data.h
    test/storage/storage_test.cpp test/storage/mock_default_storage_manager.cpp test/test_utils.cpp
    test/test_utils.h test/mock_logger.h test/query/teste_query_data.cpp test/parser/parser_test.cpp
    test/test_queries_creator.h test/engine/engine_test.cpp test/test_queries_creator.cpp test/test_queries_results.cpp
    engine/scan.cpp engine/select.cpp engine/filter.cpp engine/subset.cpp engine/logical.cpp engine/comparison.cpp
    engine/arithmetic.cpp engine/cross_join.cpp engine/equijoin.cpp engine/dimjoin.cpp engine/aggregate.cpp
    engine/user_defined.cpp engine/create_tars.cpp engine/create_tar.cpp engine/create_type.cpp engine/create_dataset.cpp
    engine/load_subtar.cpp engine/drop_tars.cpp engine/drop_type.cpp engine/drop_dataset.cpp engine/save.cpp engine/show.cpp
    engine/store.cpp engine/drop_tar.cpp test/optimizer/mock_optimizer.h test/optimizer/test_optimizer.cpp optimizer/query_cost.cpp
    optimizer/dependency_checker.cpp optimizer/query_graph.cpp optimizer/operation_splitter.cpp engine/split.cpp engine/reorient.cpp
    engine/predict.cpp engine/predictor.cpp engine/include/predictor.h)

    target_compile_options(savime-test PRIVATE ${OpenMP_CXX_FLAGS})
    target_link_libraries(savime-test ${OpenMP_CXX_FLAGS})
    target_link_libraries(savime-test ${CURL_LIBRARIES})
    target_link_libraries(savime-test -ljsoncpp)


    if(TBB_FOUND)
        target_link_libraries(savime-test ${TBB_LIBRARIES})
        target_include_directories(savime-test PRIVATE ${TBB_INCLUDE_DIRS})
        add_definitions( -DTBB_SUPPORT )
    endif(TBB_FOUND)

endif(BUILD_SAVIME_TESTS)

if(BUILD_WITH_CATALYST)

    #Get include and lib paths for catalyst
    execute_process(COMMAND find /usr/local/lib/ -maxdepth 1 -type d
        COMMAND grep paraview OUTPUT_VARIABLE VTK_LIB_FILES
        OUTPUT_STRIP_TRAILING_WHITESPACE)

    string(REGEX REPLACE "\n$" " " VTK_LIB_FILES ${VTK_LIB_FILES})
    #execute_process(COMMAND find ${VTK_LIB_FILES} -maxdepth 1 -type f
    #        -name libvtk* OUTPUT_VARIABLE VTK_LIB_FILES
    #        OUTPUT_STRIP_TRAILING_WHITESPACE)
    execute_process(COMMAND find  /usr/local/lib/ -maxdepth 1 -type f
                    -name libvtk* OUTPUT_VARIABLE VTK_LIB_FILES
                    OUTPUT_STRIP_TRAILING_WHITESPACE)

    message(STATUS ${VTK_LIB_FILES})

    string(REGEX REPLACE "\n" ";" VTK_LIB_FILES ${VTK_LIB_FILES})
    message(STATUS "VTK LIBs Found: ${VTK_LIB_FILES}")

    execute_process(COMMAND find /usr/local/include/ -maxdepth 1 -type d
            COMMAND grep paraview OUTPUT_VARIABLE VTK_INC_FILES)
    string(REGEX REPLACE "\n$" "" VTK_INC_FILES ${VTK_INC_FILES})
    message(STATUS "VTK include dir used: ${VTK_INC_FILES}")
    include_directories(${VTK_INC_FILES})

    #add_definitions( -DCATALYST )


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
            engine/misc/json.cpp
            engine/misc/include/json.h
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
        engine/default_engine.cpp
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
        storage/template_builder.h core/include/symbols.h engine/scan.cpp engine/select.cpp engine/filter.cpp
        engine/subset.cpp engine/logical.cpp engine/comparison.cpp engine/arithmetic.cpp engine/cross_join.cpp
        engine/equijoin.cpp engine/dimjoin.cpp engine/aggregate.cpp engine/user_defined.cpp engine/create_tars.cpp
        engine/create_tar.cpp engine/create_type.cpp engine/create_dataset.cpp engine/load_subtar.cpp engine/drop_tars.cpp
        engine/drop_type.cpp engine/drop_dataset.cpp engine/save.cpp engine/show.cpp engine/store.cpp engine/drop_tar.cpp
        optimizer/query_cost.cpp optimizer/dependency_checker.cpp optimizer/query_graph.cpp optimizer/operation_splitter.cpp
        engine/split.cpp engine/reorient.cpp engine/predict.cpp engine/predictor.cpp engine/include/predictor.h)

    target_compile_definitions(savime-bin PUBLIC -DCATALYST)

    find_package(OpenMP REQUIRED)
    target_compile_options(savime-bin PRIVATE ${OpenMP_CXX_FLAGS})
    target_link_libraries(savime-bin ${OpenMP_CXX_FLAGS})
    set_target_properties(savime-bin
            PROPERTIES OUTPUT_NAME savime)
    target_link_libraries(savime-bin ${CURL_LIBRARIES})
    target_link_libraries(savime-bin ${VTK_LIB_FILES})
    target_link_libraries(savime-bin -ljsoncpp)



    find_package(OpenMP REQUIRED)
    target_compile_options(savime-bin PRIVATE ${OpenMP_CXX_FLAGS})
    target_link_libraries(savime-bin ${OpenMP_CXX_FLAGS})
    install(TARGETS savime-bin DESTINATION bin)

    add_executable(savime-catalyst engine/catalyst.cpp)
    target_compile_definitions(savime-catalyst PUBLIC -DCATALYST)
    target_link_libraries(savime-catalyst ${VTK_LIB_FILES})
    target_link_libraries(savime-catalyst ${OpenMP_CXX_FLAGS})
    install(TARGETS savime-catalyst DESTINATION bin)


    if(TBB_FOUND)
        message(STATUS "TBB library found")
        target_link_libraries(savime-bin ${TBB_LIBRARIES})
        target_include_directories(savime-bin PRIVATE ${TBB_INCLUDE_DIRS})
        add_definitions( -DTBB_SUPPORT )
    endif(TBB_FOUND)

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
            engine/misc/json.cpp
            engine/misc/include/json.h
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
            engine/default_engine.cpp
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
            storage/template_builder.h core/include/symbols.h
            engine/scan.cpp engine/select.cpp engine/filter.cpp
            engine/subset.cpp engine/logical.cpp engine/comparison.cpp
            engine/arithmetic.cpp engine/cross_join.cpp engine/equijoin.cpp
            engine/dimjoin.cpp engine/aggregate.cpp engine/user_defined.cpp
            engine/create_tars.cpp engine/create_tar.cpp engine/create_type.cpp
            engine/create_dataset.cpp engine/load_subtar.cpp engine/drop_tars.cpp
            engine/drop_type.cpp engine/drop_dataset.cpp engine/save.cpp engine/show.cpp
            engine/store.cpp engine/drop_tar.cpp optimizer/query_cost.cpp optimizer/dependency_checker.cpp
            optimizer/query_graph.cpp optimizer/operation_splitter.cpp engine/split.cpp
            engine/reorient.cpp engine/predict.cpp
            engine/misc/curl.cpp engine/misc/include/curl.h
            engine/predictor.cpp engine/include/predictor.h)

    set_target_properties(savime-bin
            PROPERTIES OUTPUT_NAME savime)

    target_compile_options(savime-bin PRIVATE ${OpenMP_CXX_FLAGS})
    target_link_libraries(savime-bin ${OpenMP_CXX_FLAGS})
    target_link_libraries(savime-bin ${CURL_LIBRARIES})
    target_link_libraries(savime-bin -ljsoncpp)

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
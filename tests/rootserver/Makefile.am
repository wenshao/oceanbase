AM_CPPFLAGS = -I${TBLIB_ROOT}/include/tbsys \
			  -I${TBLIB_ROOT}/include/tbnet \
			  -I${top_srcdir}/include \
			  -I${top_srcdir}/src \
			  -I${top_srcdir}/src/common \
			  -I${top_srcdir}/src/rootserver

LDADD = $(top_builddir)/src/rootserver/librootserver.a \
		$(top_builddir)/src/common/libcommon.a \
		${TBLIB_ROOT}/lib/libtbnet.a \
		${TBLIB_ROOT}/lib/libtbsys.a

AM_LDFLAGS = -lpthread -lc -lm  -lgtest -lgmock -lrt ${GCOV_LIB}
if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

bin_PROGRAMS =  base_main_test \
				root_server_test \
				test_tablet_info_manager \
				root_table2_test \
        test_root_table2_test\
				mock_ups_4_root \
				mock_cs_4_root \
				rt_press_client \
				rt_stats_reader\
				ob_obi_role_test\
        ob_obi_config_test\
        test_batch_create_table\
				chunk_servers_manager_test\
				ob_ups_manager_test\
				ob_root_config_test\
				ob_new_balance_test\
				ob_delete_replicas_test

chunk_servers_manager_test_SOURCES = chunk_servers_manager_test.cpp
base_main_test_SOURCES = base_main_test.cpp test_main.cpp
root_server_test_SOURCES = root_server_test.cpp
#root_server_log_test_SOURCES = root_server_log_test.cpp
test_tablet_info_manager_SOURCES = test_tablet_info_manager.cpp
root_table2_test_SOURCES = test_root_table2.cpp
mock_ups_4_root_SOURCES = mock_server.cpp mock_update_server.cpp test_update_server_main.cpp
mock_cs_4_root_SOURCES = mock_server.cpp mock_chunk_server.cpp test_chunk_server_main.cpp
rt_press_client_SOURCES = rt_press_client.cpp
rt_stats_reader_SOURCES = rt_stats_reader.cpp
test_root_table2_test_SOURCES = test_root_table2_test.cpp
ob_obi_role_test_SOURCES = ob_obi_role_test.cpp
ob_obi_config_test_SOURCES = ob_obi_config_test.cpp
ob_ups_manager_test_SOURCES = ob_ups_manager_test.cpp
ob_root_config_test_SOURCES = ob_root_config_test.cpp
ob_new_balance_test_SOURCES = ob_new_balance_test.cpp
ob_delete_replicas_test_SOURCES = ob_delete_replicas_test.cpp
test_batch_create_table_SOURCES = test_batch_create_table.cpp
EXTRA_DIST = mock_chunk_server.h  mock_server.h  mock_update_server.h  root_server_tester.h  test_main.h mock_root_rpc_stub.h

check_SCRIPTS = $(bin_PROGRAMS)
TESTS = $(check_SCRIPTS)

CLEANFILES = $(check_SCRIPTS)
clean-local:
	-rm -f *.gcov *.gcno *.gcda

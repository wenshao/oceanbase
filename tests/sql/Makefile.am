AM_CPPFLAGS = -I${TBLIB_ROOT}/include/tbsys \
			  -I${TBLIB_ROOT}/include/tbnet \
			  -I${top_srcdir}/include \
			  -I${top_srcdir}/src \
			  -I${top_srcdir}/src/common \
			  -I${top_srcdir}/src/common/hash

LDADD = ${top_builddir}/src/sql/libsql.a \
		${top_srcdir}/src/mergeserver/libmergeserver.a \
		${top_srcdir}/src/sstable/libsstable.a \
		${top_builddir}/src/common/libcommon.a \
		$(top_builddir)/src/common/btree/libbtree.a \
		${TBLIB_ROOT}/lib/libtbnet.a \
		${TBLIB_ROOT}/lib/libtbsys.a

AM_LDFLAGS=-lpthread -lc -lm -lrt -lgtest   ${GCOV_LIB} -lnuma -lcrypt -lreadline -lncurses
CXXFLAGS=-D__STDC_LIMIT_MACROS -D__STDC_CONSTANT_MACROS -g
if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

pub_source= \
          ob_fake_table.cpp \
          ob_file_table.cpp \
					ob_ups_file_table.cpp \
          test_utility.cpp \
					ob_fake_ups_multi_get.cpp \
          ob_fake_ups_scan.cpp \
					ob_fake_ups_rpc_stub.cpp \
					ob_fake_ups_rpc_stub2.cpp \
					ob_fake_sstable_scan.cpp \
					ob_operator_factory_impl.cpp

check_programs= \
            ob_row_desc_test \
            ob_run_file_test \
            ob_in_memory_sort_test \
            ob_merge_sort_test \
            ob_sort_test \
            ob_postfix_expression_test \
            ob_sql_expression_test \
            ob_project_test \
            ob_filter_test \
            ob_limit_test \
            ob_merge_distinct_test \
            ob_aggregate_function_test \
            ob_merge_groupby_test \
            ob_phy_operators_test \
            ob_file_table_test \
            ob_tablet_fuse_test \
            ob_fake_ups_multi_get_test \
            ob_tablet_join_test \
            sql_logical_plan_test \
            sql_physical_plan_test\
            ob_add_project_test \
            ob_single_table_sql_test\
            ob_fake_ups_rpc_stub_test \
            ob_ups_multi_get_test \
            ob_ups_scan_test\
            ob_merge_join_test\
            ob_tablet_scan_test \
            ob_result_set_test

EXTRA_DIST=\
					 *.h
bin_PROGRAMS= ${check_programs}\
            sqlparser \
            sqlplan

ob_row_desc_test_SOURCES=ob_row_desc_test.cpp ${pub_source}
ob_run_file_test_SOURCES=ob_run_file_test.cpp ${pub_source}
ob_in_memory_sort_test_SOURCES=ob_in_memory_sort_test.cpp ${pub_source}
ob_merge_sort_test_SOURCES=ob_merge_sort_test.cpp ${pub_source}
ob_sort_test_SOURCES=ob_sort_test.cpp ${pub_source}
ob_postfix_expression_test_SOURCES=ob_postfix_expression_test.cpp
ob_sql_expression_test_SOURCES=ob_sql_expression_test.cpp
ob_project_test_SOURCES=ob_project_test.cpp ${pub_source}
ob_filter_test_SOURCES=ob_filter_test.cpp ${pub_source}
ob_limit_test_SOURCES=ob_limit_test.cpp ${pub_source}
ob_merge_distinct_test_SOURCES=ob_merge_distinct_test.cpp ${pub_source}
ob_aggregate_function_test_SOURCES=ob_aggregate_function_test.cpp ${pub_source}
ob_merge_groupby_test_SOURCES=ob_merge_groupby_test.cpp ${pub_source}
ob_phy_operators_test_SOURCES=ob_phy_operators_test.cpp ${pub_source}
ob_file_table_test_SOURCES=ob_file_table_test.cpp ${pub_source}
ob_tablet_fuse_test_SOURCES=ob_tablet_fuse_test.cpp ${pub_source}
ob_add_project_test_SOURCES=ob_add_project_test.cpp ${pub_source}
ob_tablet_join_test_SOURCES=ob_tablet_join_test.cpp ${pub_source}
ob_fake_ups_multi_get_test_SOURCES=ob_fake_ups_multi_get_test.cpp ${pub_source}
sqlparser_SOURCES = sql_parser_main.cpp
sqlplan_SOURCES = sql_plan_main.cpp
sql_logical_plan_test_SOURCES = test_sql_parser.cpp
sql_physical_plan_test_SOURCES = test_gen_phy_plan.cpp
ob_single_table_sql_test_SOURCES = ob_single_table_sql_test.cpp ${pub_source}
ob_fake_ups_rpc_stub_test_SOURCES = ob_fake_ups_rpc_stub_test.cpp ${pub_source}
ob_ups_scan_test_SOURCES = ob_ups_scan_test.cpp ${pub_source}
ob_ups_multi_get_test_SOURCES = ob_ups_multi_get_test.cpp ${pub_source}
ob_merge_join_test_SOURCES = ob_merge_join_test.cpp ${pub_source}
ob_tablet_scan_test_SOURCES = ob_tablet_scan_test.cpp ${pub_source}
ob_result_set_test_SOURCES = ob_result_set_test.cpp

check_SCRIPTS = $(check_programs)
TESTS = $(check_SCRIPTS)

CLEANFILES = $(check_SCRIPTS)
clean-local:
	-rm -f *.gcov *.gcno *.gcda

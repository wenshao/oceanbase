AM_CPPFLAGS= -I${TBLIB_ROOT}/include/tbsys \
			 -I${TBLIB_ROOT}/include/tbnet \
			 -I${top_srcdir}/include \
			 -I${top_srcdir}/src

LDADD = ${top_srcdir}/src/mergeserver/libmergeserver.a \
				${top_srcdir}/src/sstable/libsstable.a \
        ${TBLIB_ROOT}/lib/libtbnet.a \
        ${TBLIB_ROOT}/lib/libtbsys.a

#CXXFLAGS+=-O2 -Wno-strict-aliasing
AM_LDFLAGS=-lpthread -lc -lm  -lrt -lnuma -lcrypt -lssl #-pg
if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

CFLAGS=-g -Wall -c

#sql_parser.tab.c sql_parser.tab.h type_name.c: sql_parser.y
#        tools/bison -d $<
#        tools/gen_type_name.sh ob_item_type.h > type_name.c

#sql_parser.lex.c sql_parser.lex.h : sql_parser.l sql_parser.tab.h
#        tools/flex -osql_parser.lex.c $<

sql_parser_sources=parse_malloc.cpp \
                   parse_malloc.h \
                   parse_node.h\
                   parse_node.c \
                   parse_tools.h \
                   ob_item_type.h \
                   type_name.c \
                   sql_parser.lex.c \
                   sql_parser.lex.h \
                   sql_parser.tab.c \
                   sql_parser.tab.h \
                   build_plan.h \
                   build_plan.cpp \
                   ob_bit_set.cpp \
                   ob_bit_set.h \
                   ob_logical_plan.cpp \
                   ob_logical_plan.h \
                   ob_multi_plan.cpp \
                   ob_multi_plan.h \
                   ob_raw_expr.cpp \
                   ob_raw_expr.h \
                   ob_stmt.cpp \
                   ob_stmt.h \
                   ob_schema_checker.cpp \
                   ob_schema_checker.h \
                   ob_select_stmt.cpp \
                   ob_select_stmt.h \
                   ob_delete_stmt.cpp \
                   ob_delete_stmt.h\
                   ob_insert_stmt.cpp \
                   ob_insert_stmt.h \
                   ob_update_stmt.cpp \
                   ob_update_stmt.h \
                   ob_physical_plan.cpp \
                   ob_sys_params_mgr.cpp \
                   ob_sys_params_mgr.h \
                   ob_physical_plan.h \
                   ob_transformer.cpp \
                   ob_transformer.h \
                   MySchema.cpp \
                   MySchema.h

operators_sources= ob_phy_operator.h \
                   ob_single_child_phy_operator.cpp \
                   ob_single_child_phy_operator.h \
                   ob_double_children_phy_operator.cpp \
                   ob_double_children_phy_operator.h \
                   ob_postfix_expression.cpp \
                   ob_postfix_expression.h \
                   ob_sql_expression.cpp \
                   ob_sql_expression.h \
                   ob_filter.cpp \
                   ob_filter.h \
                   ob_limit.cpp \
                   ob_limit.h \
                   ob_project.cpp \
                   ob_project.h \
                   ob_add_project.h\
                   ob_add_project.cpp\
                   ob_set_operator.cpp \
                   ob_set_operator.h \
                   ob_merge_distinct.cpp \
                   ob_merge_distinct.h \
                   ob_merge_except.cpp \
                   ob_merge_except.h \
                   ob_merge_intersect.cpp \
                   ob_merge_intersect.h \
                   ob_merge_union.cpp \
                   ob_merge_union.h \
                   ob_groupby.cpp \
                   ob_groupby.h \
                   ob_aggregate_function.cpp \
                   ob_aggregate_function.h \
                   ob_scalar_aggregate.cpp \
                   ob_scalar_aggregate.h \
                   ob_merge_groupby.cpp \
                   ob_merge_groupby.h \
                   ob_join.cpp \
                   ob_join.h \
                   ob_merge_join.cpp \
                   ob_merge_join.h \
                   ob_run_file.cpp \
                   ob_run_file.h \
                   ob_merge_sort.cpp \
                   ob_merge_sort.h \
                   ob_in_memory_sort.cpp \
                   ob_in_memory_sort.h \
                   ob_sort_helper.h \
                   ob_sort.cpp \
                   ob_sort.h \
                   ob_table_scan.h \
                   ob_rename.cpp \
                   ob_rename.h \
                   ob_table_mem_scan.cpp \
                   ob_table_mem_scan.h \
                   ob_rpc_scan.cpp \
                   ob_rpc_scan.h \
                   ob_table_rpc_scan.cpp \
                   ob_table_rpc_scan.h \
                   ob_tablet_fuse.cpp \
                   ob_tablet_fuse.h \
                   ob_tablet_join.cpp \
                   ob_tablet_join.h \
                   ob_sstable_scan.cpp \
                   ob_sstable_scan.h \
                   ob_ups_multi_get.cpp \
                   ob_ups_multi_get.h \
                   ob_ups_scan.h\
                   ob_ups_scan.cpp \
                   ob_result_set.h\
                   ob_sql.h\
                   ob_sql.cpp\
									 ob_rowkey_phy_operator.h\
									 ob_rowkey_phy_operator.cpp\
									 ob_tablet_scan.h\
									 ob_tablet_scan.cpp\
                   ob_values.h\
                   ob_values.cpp\
                   ob_insert.h\
                   ob_insert.cpp

EXTRA_DIST=\
					 *.h
noinst_LIBRARIES = libsql.a
libsql_a_SOURCES = ${sql_parser_sources} ${operators_sources}

clean-local:
	-rm -f *.gcov *.gcno *.gcda

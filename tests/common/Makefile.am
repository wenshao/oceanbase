AM_CPPFLAGS = -I${TBLIB_ROOT}/include/tbsys \
			  -I${TBLIB_ROOT}/include/tbnet \
			  -I${top_srcdir}/include \
			  -I${top_srcdir}/src \
			  -I${top_srcdir}/src/common \
			  -I${top_srcdir}/src/common/hash

LDADD = ${top_builddir}/src/common/libcommon.a \
		${TBLIB_ROOT}/lib/libtbnet.a \
		${TBLIB_ROOT}/lib/libtbsys.a -lcrypt

AM_LDFLAGS=-lpthread -lc -lm -lrt -lgtest   ${GCOV_LIB} -lcrypt
if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

bin_PROGRAMS = limit_array_test \
			   test_server_ext\
			   test_array_lock \
         test_ob_string \
         test_get_param \
			   test_cell_array \
			   schema_test \
			   serialization_test \
			   test_lrucache \
			   test_mutator \
			   memory_pool_test \
			   test_ob_vector \
			   wait_object_test \
			   test_ob_blacklist \
         test_ob_cond_info \
			   test_ob_string_buf \
			   test_ob_object \
			   test_ob_scanner \
			   test_probability_random \
			   test_ob_single_log_reader \
			   test_ob_scanner \
			   test_ob_prefetch_data \
			   test_ob_log_dir_scanner \
			   test_scan_param \
			   test_ob_stat \
			   test_ob_range \
			   test_ob_log_writer \
			   test_ob_slave_mgr \
			   test_kr \
			   test_slab \
			   test_ob_string_search \
			   test_groupby_param	\
			   test_counter \
			   test_ob_log_reader \
			   test_file \
			   test_row_compaction \
			   test_ob_composite_column_infix	\
			   test_spop_spush_queue\
			   test_login_mgr \
			   test_token \
			   test_meta_cache \
			   test_schema_table \
			   test_merger \
			   test_ring_buffer\
			   test_file \
			   test_row_compaction\
			   ob_flag_test\
			   ob_array_test\
			   test_nb_accessor\
         test_compact_cell\
         test_buffer_helper \
				ob_number_test\
				ob_expr_obj_test\
        ob_ups_row_util_test\
        ob_row_fuse_test\
				test_ob_row_store\
                test_ob_postfix_expression\
                test_ob_composite_column\
				test_row_util \
				test_ob_new_scanner

test_mutator_SOURCES = test_mutator.cpp
test_token_SOURCES = test_token.cpp
test_get_param_SOURCES = test_get_param.cpp
schema_test_SOURCES = schema_test.cpp
limit_array_test_SOURCES = limit_array_test.cpp
serialization_test_SOURCES = serialization_test.cpp
memory_pool_test_SOURCES = memory_pool_test.cpp
test_lrucache_SOURCES = test_lrucache.cpp
test_ob_cond_info_SOURCES = test_ob_cond_info.cpp
test_ob_string_SOURCES = test_ob_string.cpp
test_array_lock_SOURCES = test_array_lock.cpp
test_ob_vector_SOURCES = test_ob_vector.cpp
wait_object_test_SOURCES = wait_object_test.cpp
test_ob_string_buf_SOURCES = test_ob_string_buf.cpp
test_ob_blacklist_SOURCES = test_ob_blacklist.cpp
test_probability_random_SOURCES = test_probability_random.cpp
test_ob_object_SOURCES = test_ob_object.cpp $(top_srcdir)/src/common/ob_object.cpp
test_scan_param_SOURCES=test_scan_param.cpp
test_ob_stat_SOURCES=test_ob_stat.cpp
test_ob_log_dir_scanner_SOURCES=test_ob_log_dir_scanner.cpp
test_ob_single_log_reader_SOURCES= test_ob_single_log_reader.cpp
test_ob_range_SOURCES = test_ob_range.cpp
test_ob_log_reader_SOURCES = test_ob_log_reader.cpp
test_ob_log_writer_SOURCES = test_ob_log_writer.cpp
test_ob_slave_mgr_SOURCES = test_ob_slave_mgr.cpp
test_ob_scanner_SOURCES = test_ob_scanner.cpp
test_ob_prefetch_data_SOURCES = test_ob_prefetch_data.cpp
test_ob_string_search_SOURCES = test_ob_string_search.cpp
test_slab_SOURCES = test_slab.cpp
test_groupby_param_SOURCES =  test_groupby_param.cpp
test_kr_SOURCES = test_kr.cpp
test_file_SOURCES = test_file.cpp
test_row_compaction_SOURCES = test_row_compaction.cpp
test_ob_composite_column_SOURCES = test_ob_composite_column.cpp
test_ob_composite_column_infix_SOURCES = test_ob_composite_column_infix.cpp
test_ob_postfix_expression_SOURCES = test_ob_postfix_expression.cpp
test_spop_spush_queue_SOURCES = test_spop_spush_queue.cpp
test_cell_array_SOURCES = test_cell_array.cpp
test_counter_SOURCES = test_counter.cpp
test_login_mgr_SOURCES = test_login_mgr.cpp
test_meta_cache_SOURCES = test_meta_cache.cpp
test_schema_table_SOURCES = test_schema_table.cpp
test_server_ext_SOURCES = test_server_ext.cpp
ob_flag_test_SOURCES = ob_flag_test.cpp
test_merger_SOURCES = test_merger.cpp
test_ring_buffer_SOURCES = test_ring_buffer.cpp
ob_array_test_SOURCES = ob_array_test.cpp
test_nb_accessor_SOURCES = test_nb_accessor.cpp
test_compact_cell_SOURCES = test_compact_cell.cpp
test_buffer_helper_SOURCES = test_buffer_helper.cpp
ob_number_test_SOURCES = ob_number_test.cpp
ob_ups_row_util_test_SOURCES = ob_ups_row_util_test.cpp
ob_row_fuse_test_SOURCES = ob_row_fuse_test.cpp
test_ob_row_store_SOURCES = test_ob_row_store.cpp
ob_expr_obj_test_SOURCES = ob_expr_obj_test.cpp
test_row_util_SOURCES = test_row_util.cpp
test_ob_new_scanner_SOURCES = test_ob_new_scanner.cpp

SUBDIRS = hash compress

DIST_SUBDIRS = hash compress

EXTRA_DIST = \
	mock_mem_iterator.h \
	test_helper.h\
	slave_mgr_4_test.h

check_SCRIPTS = $(bin_PROGRAMS)
TESTS = $(check_SCRIPTS)

CLEANFILES = $(check_SCRIPTS)
clean-local:
	-rm -f *.gcov *.gcno *.gcda

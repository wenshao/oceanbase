AM_CPPFLAGS= -I${TBLIB_ROOT}/include/tbsys \
			 -I${TBLIB_ROOT}/include/tbnet \
			 -I${top_srcdir}/include \
			 -I${top_srcdir}/src \
			 -fPIC

LDADD = ${TBLIB_ROOT}/lib/libtbnet.a \
				${TBLIB_ROOT}/lib/libtbsys.a
CXXFLAGS+=-fPIC
AM_LDFLAGS=-lpthread -lc -lm  -lrt -lcrypt -lssl #-pg
if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

noinst_LIBRARIES = libcommon.a

libcommon_a_SOURCES = dlist.cpp \
          ob_object.cpp \
					ob_decimal_helper.cpp \
					ob_array_lock.cpp \
          ob_timer.cpp \
		      ob_server.cpp \
		      ob_schema.cpp \
		      file_utils.cpp \
		      file_directory_utils.cpp \
          murmur_hash.cpp \
          ob_merger.cpp \
		      ob_range.cpp \
		      ob_string_buf.cpp \
		      ob_tablet_info.cpp \
		      base_main.cpp \
		      ob_base_server.cpp \
		      ob_single_server.cpp \
		      ob_simple_filter.cpp \
          ob_simple_condition.cpp \
		      ob_cond_info.cpp \
		      ob_blacklist.cpp \
          ob_probability_random.cpp \
		      ob_cell_array.cpp \
		      ob_scanner.cpp \
		      ob_new_scanner.cpp \
					ob_row_desc.cpp \
					ob_raw_row.cpp \
					ob_row.cpp \
					ob_row_store.cpp \
					ob_row_util.cpp \
					ob_scan_param.cpp \
					ob_prefetch_data.cpp \
		      ob_common_param.cpp \
		      ob_get_param.cpp \
		      ob_memory_pool.cpp \
		      ob_malloc.cpp \
		      wait_object.cpp \
		      ob_packet.cpp \
		      thread_buffer.cpp \
		      ob_client_manager.cpp \
		      ob_cache.cpp \
		      ob_trace_log.cpp \
		      ob_record_header.cpp \
		      utility.cpp \
          ob_mutator.cpp \
		      ob_log_replay_runnable.cpp \
		      ob_log_writer.cpp \
		      ob_log_reader.cpp \
		      ob_single_log_reader.cpp \
					ob_direct_log_reader.cpp \
					ob_repeated_log_reader.cpp \
		      ob_log_dir_scanner.cpp \
		      ob_fetch_runnable.cpp \
		      ob_check_runnable.cpp \
		      ob_slave_mgr.cpp \
		      ob_log_entry.cpp \
		      ob_common_rpc_stub.cpp \
		      batch_packet_queue_thread.cpp \
		      ob_mod_define.cpp \
		      ob_crc64.cpp \
		      ob_lease_common.cpp \
		      ob_spop_spush_queue.cpp \
					ob_packet_queue_thread.cpp \
		      ob_thread_mempool.cpp \
		      ob_tsi_factory.cpp \
					ob_counter.cpp \
		      ob_statistics.cpp \
		      ob_slab.cpp \
		      ob_regex.cpp \
		      ob_string_search.cpp \
					ob_groupby.cpp \
					ob_shadow_server.cpp\
					priority_packet_queue_thread.cpp \
					ob_packet_queue.cpp \
					ob_file.cpp \
					ob_obi_role.cpp\
					ob_base_client.cpp\
					ob_obi_config.cpp \
					ob_ups_info.cpp\
					ob_row_compaction.cpp\
					ob_composite_column.cpp \
					ob_postfix_expression.cpp  \
					ob_infix_expression.cpp  \
					ob_groupby_operator.cpp \
					ob_compose_operator.cpp \
					ob_return_operator.cpp \
					ob_session_mgr.cpp \
          ob_server_ext.cpp \
		      ob_update_condition.cpp\
		      ob_login_mgr.cpp \
		      ob_ring_buffer.cpp \
		      ob_token.cpp \
		      ob_encrypt.cpp \
		      ob_perm_components.cpp \
		      ob_meta_cache.cpp\
					ob_rs_ups_message.cpp \
		      ob_client_config.cpp \
			    ob_client_helper.cpp\
			    ob_log_cursor.cpp\
			    ob_server_getter.cpp\
          ob_schema_table.cpp \
          ob_nb_accessor.cpp \
          ob_ms_list.cpp \
          ob_client_proxy.cpp \
          ob_mutex_task.cpp \
          ob_mutator_helper.cpp \
          ob_config.cpp \
          ob_flag.cpp \
          ob_compact_cell_iterator.cpp \
          ob_ups_row.cpp \
          ob_row_fuse.cpp \
          ob_ups_row_util.cpp \
          ob_compact_cell_writer.cpp \
          ob_compact_cell_util.cpp\
				ob_number.h ob_number.cpp\
				ob_expr_obj.h ob_expr_obj.cpp

bin_PROGRAMS = ob_ping
ob_ping_SOURCES = ob_ping.cpp ${libcommon_a_SOURCES}

SUBDIRS=btree compress
DIST_SUBDIRS=btree compress

EXTRA_DIST = \
						 base_main.h \
						 batch_packet_queue_thread.h \
						 bloom_filter.h \
						 check_sum.h \
						 ob_array_lock.h \
             ob_groupby.h \
						 ob_token.h\
						 ob_encrypt.h\
						 data_buffer.h \
						 dlist.h \
						 file_directory_utils.h \
						 file_utils.h \
						 limit_array.h \
						 murmur_hash.h \
						 ob_action_flag.h \
						 ob_array_helper.h \
						 ob_array.h \
						 ob_buffer.h \
						 ob_thread_store.h \
						 ob_base_server.h \
						 ob_cache.h \
						 ob_check_runnable.h \
						 ob_client_manager.h \
						 ob_cell_array.h \
						 ob_simple_condition.h \
						 ob_simple_filter.h \
						 ob_common_param.h \
						 ob_common_rpc_stub.h \
						 ob_define.h \
						 ob_endian.h \
						 ob_fetch_runnable.h \
						 ob_get_param.h \
						 ob_iterator.h \
						 ob_link.h \
						 ob_log_dir_scanner.h \
						 ob_log_entry.h \
						 ob_log_reader.h \
						 ob_log_replay_runnable.h \
						 ob_log_writer.h \
						 ob_lrucache.h \
						 ob_malloc.h \
						 ob_memory_pool.h \
						 ob_merger.h \
						 ob_mutator.h \
						 ob_object.h \
						 ob_decimal_helper.h \
						 ob_operate_result.h \
						 ob_packet.h \
						 ob_packet_factory.h \
						 ob_packet_queue_thread.h \
						 ob_range.h \
						 ob_read_common_data.h \
						 ob_record_header.h \
						 ob_result.h \
						 ob_role_mgr.h \
						 ob_scanner.h \
						 ob_new_scanner.h \
						 ob_row_desc.h \
						 ob_raw_row.h \
						 ob_row.h \
						 ob_row_store.h \
						 ob_row_util.h \
						 ob_scan_param.h \
						 ob_schema.h \
						 ob_server.h \
						 ob_single_log_reader.h \
						 ob_direct_log_reader.h \
						 ob_repeated_log_reader.h \
						 ob_single_server.h \
						 ob_slave_mgr.h \
						 ob_string.h \
						 ob_string_buf.h \
						 ob_tablet_info.h \
						 ob_timer.h \
						 ob_trace_log.h \
						 ob_vector.h \
						 page_arena.h \
						 serialization.h \
						 thread_buffer.h \
						 utility.h \
						 wait_object.h \
						 hash/ob_hashmap.h \
						 hash/ob_hashset.h \
						 hash/ob_hashtable.h \
						 hash/ob_hashutils.h \
						 hash/ob_serialization.h \
						 ob_vector.ipp \
						 ob_statistics.h \
						 ob_mod_define.h \
						 ob_crc64.h \
						 ob_atomic.h \
						 ob_lease_common.h \
						 ob_thread_mempool.h \
						 ob_tsi_factory.h \
						 ob_counter.h \
						 ob_ping.h \
						 ob_bloomfilter.h \
						 ob_slab.h \
						 ob_bloomfilter.h \
						 ob_string_search.h  \
						 ob_update_condition.h \
						 ob_shadow_server.h\
						 ob_probability_random.h \
						 priority_packet_queue_thread.h \
						 ob_spop_spush_queue.h \
						 ob_packet_queue.h \
						 ob_regex.h \
						 ob_spin_rwlock.h \
						 ob_probability_random.h \
						 priority_packet_queue_thread.h \
						 ob_packet_queue.h \
						 ob_base_client.h\
						 ob_ring_buffer.h \
						 ob_regex.h \
						 ob_spin_rwlock.h \
						 ob_fileinfo_manager.h \
						 ob_kv_storecache.h \
						 ob_list.h \
						 ob_thread_objpool.h \
						 ob_bitmap.h \
						 ob_file.h \
						 ob_obi_role.h\
						 ob_composite_column.h \
						 ob_log_cursor.h \
						 ob_postfix_expression.h  \
						 ob_infix_expression.h  \
						 ob_expression.h \
						 ob_groupby_operator.h \
						 ob_compose_operator.h \
						 ob_return_operator.h \
						 ob_server_ext.h \
						 ob_session_mgr.h \
						 ob_thread_objpool.h \
						 ob_file.h \
						 ob_meta_cache.h \
						 ob_rs_ups_message.h\
						 ob_login_mgr.h \
						 ob_perm_components.h \
						 ob_cipher.h \
						 ob_pointer_array.h\
						 ob_bitmap.h \
						 ob_obi_config.h \
						 ob_client_helper.h \
						 ob_client_config.h \
						 ob_ups_info.h \
						 ob_blacklist.h \
             ob_row_compaction.h \
						 ob_schema_table.h \
						 ob_log_cursor.h\
						 ob_flag.h \
						 debug.h \
             ob_cond_info.h \
						 ob_prefetch_data.h\
						 ob_decimal_helper.h \
						 ob_nb_accessor.h \
						 ob_ms_list.h\
						 ob_easy_array.h \
						 ob_server_getter.h\
						 ob_mutator_helper.h \
						 ob_client_proxy.h \
						 ob_task.h \
						 ob_switch.h \
						 ob_mutex_task.h \
             ob_client_proxy.h \
						 ob_config.h \
             ob_compact_cell_writer.h \
             ob_compact_cell_iterator.h \
             ob_buffer_helper.h \
             ob_ups_rpc_proxy.h \
             ob_row_fuse.h \
             ob_ups_row.h \
             ob_ups_row_util.h \
             ob_compact_cell_util.h \
             ob_compact_store_type.h \
						 ob_row_iterator.h \
						 ob_cell_meta.h \
						 ob_simple_right_join_cell.h \
						 ob_fixed_queue.h \
						 ob_atomic_type.h \
						 ob_delay_guard.h \
						 ob_ups_rpc_stub.h \
						 ob_column_filter.h \
						 ob_blacklist.h

clean-local:
	-rm -f *.gcov *.gcno *.gcda

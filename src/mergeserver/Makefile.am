AM_CPPFLAGS = -I${TBLIB_ROOT}/include/tbnet \
			  -I${TBLIB_ROOT}/include/tbsys \
			  -I${top_srcdir}/include \
			  -I${top_srcdir}/src 

LDADD = $(top_builddir)/src/common/libcommon.a \
				$(top_builddir)/src/common/btree/libbtree.a \
				${TBLIB_ROOT}/lib/libtbnet.a \
				${TBLIB_ROOT}/lib/libtbsys.a 

#AM_LDFLAGS = -lpthread -lc -lm -lrt -lprofiler
AM_LDFLAGS = -lpthread -lc -lm -lrt -pg
if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

pub_source = \
			 ob_ms_config_proxy.cpp	\
		 	 ob_ms_config_manager.cpp	\
			 ob_ms_config_task.cpp	\
			 ob_ms_server_counter.cpp \
       ob_ms_sql_operator.cpp \
			 ob_ms_sql_sorted_operator.cpp \
			 ob_ms_sql_rpc_event.cpp \
			 ob_ms_sql_request_event.cpp \
			 ob_ms_sql_sub_scan_request.cpp \
			 ob_ms_sql_scan_event.cpp \
			 ob_sql_rpc_event.cpp \
		   ob_ms_scan_event.cpp \
			 ob_ms_sub_scan_request.cpp\
			 ob_ms_sub_get_request.cpp \
			 ob_ms_get_event_parellel.cpp \
			 ob_ms_request_event.cpp \
			 ob_rpc_event.cpp \
			 ob_ms_rpc_event.cpp \
			 ob_ms_async_rpc.cpp \
			 ob_ms_lease_task.cpp \
			 ob_ms_schema_task.cpp \
			 ob_ms_schema_proxy.cpp \
			 ob_ms_tablet_location_proxy.cpp \
			 ob_ms_monitor_task.cpp \
			 ob_ms_ups_task.cpp \
			 ob_merge_server.cpp \
			 ob_cell_stream.cpp \
			 ob_ms_service_monitor.cpp \
			 ob_merge_server_main.cpp \
			 ob_cell_operator.cpp  \
			 ob_merge_server_params.cpp \
			 ob_ms_server_blacklist.cpp \
			 ob_ms_rpc_proxy.cpp\
			 ob_merge_join_agent_imp.cpp \
			 ob_merge_server_service.cpp \
			 ob_ms_cache_table.cpp \
			 ob_rs_rpc_proxy.cpp \
			 ob_ms_btreemap.cpp	\
			 ob_ms_read_balance.cpp \
			 ob_ms_tablet_location.cpp \
			 ob_ms_schema_manager.cpp \
			 ob_ms_get_cell_stream.cpp \
			 ob_ms_scan_cell_stream.cpp \
			 ob_ms_scanner_encoder.cpp \
			 ob_ms_version_proxy.cpp \
			 ob_ms_prefetch_data.cpp \
			 ob_param_decoder.cpp \
			 ob_mutator_param_decoder.cpp \
			 ob_read_param_modifier.cpp \
			 ob_ms_tablet_location_item.cpp \
			 ob_ms_cache_table.cpp \
			 ob_row_cell_vec.cpp \
			 ob_groupby_operator.cpp \
			 ob_read_param_decoder.cpp \
			 ob_join_cache.cpp \
			 ob_ms_scan_param.cpp \
			 ob_merger_sorted_operator.cpp \
			 ob_merger_reverse_operator.cpp \
			 ob_merger_groupby_operator.cpp\
			 ob_merger_operator.cpp \
			 ob_chunk_server_task_dispatcher.cpp \
			 ob_ms_tablet_iterator.cpp \
			 ob_ms_counter_infos.cpp \
			 ob_ms_cell_stream.cpp \
			 ob_scan_param_loader.cpp	\
			 ob_scanner_loader.cpp \
			 ob_location_list_cache_loader.cpp \
			 ob_query_cache.cpp

# rules to auto generate svn_version.cpp
include $(top_srcdir)/svn_version.mk
noinst_LIBRARIES = libmergeserver.a
bin_PROGRAMS = mergeserver
libmergeserver_a_SOURCES= ${pub_source} ob_ms_rpc_stub.cpp $(top_srcdir)/svn_version.cpp
mergeserver_SOURCES= main.cpp ${pub_source} ob_ms_rpc_stub.cpp
nodist_mergeserver_SOURCES= $(top_srcdir)/svn_version.cpp
EXTRA_DIST = \
				ob_ms_server_counter.h \
        ob_ms_config_proxy.h	\
				ob_ms_config_manager.h	\
				ob_ms_config_task.h	\
			  ob_ms_sql_sorted_operator.h    \
			  ob_ms_sql_operator.h   \
			  ob_ms_sql_rpc_event.h \
			  ob_ms_sql_request_event.h \
			  ob_ms_sql_sub_scan_request.h \
			  ob_ms_sql_scan_event.h \
			  ob_sql_rpc_event.h \
			  ob_ms_async_rpc.h \
			  ob_ms_request_event.h \
			  ob_rpc_event.h \
			 ob_ms_rpc_event.h \
			 ob_rs_rpc_proxy.h \
			 ob_ms_schema_proxy.h \
			 ob_cell_stream.h \
			 ob_cell_operator.h \
			 ob_ms_tablet_location_proxy.h \
			 ob_merge_join_agent_imp.h \
			 ob_merge_server.h \
			 ob_merge_server_main.h \
			 ob_merge_server_params.h \
			 ob_ms_server_blacklist.h \
			 ob_ms_scanner_encoder.h \
			 ob_merge_server_service.h \
			 ob_ms_stlmap.h \
			 ob_ms_btreemap.h \
			 ob_ms_version_proxy.h \
			 ob_ms_service_monitor.h \
			 ob_ms_cache_table.h \
			 ob_ms_cell_stream.h \
			 ob_ms_get_cell_stream.h \
			 ob_ms_rpc_proxy.h \
			 ob_ms_rpc_stub.h \
			 ob_ms_scan_cell_stream.h \
			 ob_ms_schema_manager.h \
			 ob_ms_tablet_location.h \
			 ob_ms_tablet_location_item.h \
			 ob_ms_schema_task.h \
			 ob_ms_lease_task.h \
			 ob_ms_monitor_task.h \
			 ob_read_param_modifier.h \
			 ob_ms_prefetch_data.h \
			 ob_param_decoder.h \
			 ob_mutator_param_decoder.h \
			 ob_ms_ups_task.h \
			 ob_ms_read_balance.h \
			 ob_row_cell_vec.h \
			 ob_join_cache.h \
			 ob_merger_sorted_operator.h\
			 ob_merger_reverse_operator.h\
			 ob_merger_groupby_operator.h\
			 ob_merger_operator.h \
			 ob_groupby_operator.h \
			 ob_ms_define.h \
			 ob_read_param_decoder.h \
			 ob_ms_tsi.h \
			 ob_ms_scan_param.h \
			 ob_ms_sub_scan_request.h \
			 ob_ms_sub_get_request.h \
			 ob_ms_get_event_parellel.h \
			 ob_ms_scan_event.h \
			 ob_chunk_server.h \
			 ob_chunk_server_task_dispatcher.h \
			 ob_ms_tablet_iterator.h \
			 ob_chunk_server.h \
			 ob_ms_counter_infos.h \
			 ob_scan_param_loader.h \
			 ob_scanner_loader.h \
			 ob_location_list_cache_loader.h \
			 ob_query_cache.h \
			 mergeserver.conf.template

pkgdata_data_DATA=mergeserver.conf.template
pkgdata_datadir=${prefix}/etc
clean-local:
	-rm -f *.gcov *.gcno *.gcda

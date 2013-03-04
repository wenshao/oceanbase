SUBDIRS=olap
DIST_SUBDIRS=olap
AM_CPPFLAGS = -I${TBLIB_ROOT}/include/tbsys \
			  -I${TBLIB_ROOT}/include/tbnet \
			  -I${top_srcdir}/include \
			  -I${top_srcdir}/src \
			  -I${top_srcdir}/src/common \
			  -I${top_srcdir}/src/common/btree \
			  -I${top_srcdir}/src/mergeserver 

LDADD = $(top_builddir)/src/mergeserver/libmergeserver.a \
				$(top_builddir)/src/common/libcommon.a \
				$(top_builddir)/src/common/btree/libbtree.a \
				${TBLIB_ROOT}/lib/libtbnet.a \
				${TBLIB_ROOT}/lib/libtbsys.a

CXXFLAGS += -D_MS_MOCK_WHOLE_  -D_BTREE_ENGINE_ -D__UNIT_TEST__
AM_LDFLAGS = -lpthread -lc -lm  -lgtest -lrt ${GCOV_LIB} -ldl
if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

pub_source = mock_server.cpp \
			 mock_root_server.cpp \
			 mock_update_server.cpp \
			 mock_chunk_server.cpp

bin_PROGRAMS = \
			   test_ups_client \
			   monitor_tool \
			   test_mutate_client \
			   test_merger_cache_table \
			   test_merger_stlmap \
			   test_merger_btreemap \
			   test_merger_blacklist \
			   test_merger_scanner_encoder \
			   test_merger_tablet_location_item \
			   test_merger_tablet_location \
			   test_merger_schema_manager \
			   test_merger_schema_proxy \
			   test_merger_timer_task \
			   test_cell_operator \
			   test_merger_rpc_stub \
			   test_merger_root_proxy \
			   test_merger_rpc_proxy \
			   test_merger_version_proxy \
			   test_groupby_operator \
			   test_merger_param_decoder \
			   test_merger_mutator_decoder \
			   test_read_param_decoder \
			   test_read_param_modifier \
			   test_param_modifier \
			   test_ms_counter \
			   test_scan_backward \
			   test_ob_ms_scan_event	\
			   test_ms_scan_param \
			   test_mj_client \
			   test_merger_sorted_operator \
			   test_merger_groupby_operator \
			   test_merger_operator \
			   test_merger_reverse_operator \
			   test_ob_scan_param_loader	\
			   test_ob_scanner_loader \
			   test_ob_location_list_cache_loader \
			   test_merger_scan\
			   test_session_manager\
			   test_ms_sub_get_request\
			   test_get_merger\
			   test_ms_get_event_parellel

test_session_manager_SOURCES = test_session_manager.cpp
test_merger_cache_table_SOURCES = test_merger_cache_table.cpp
test_merger_tablet_location_item_SOURCES = test_merger_tablet_location_item.cpp
monitor_tool_SOURCES = monitor_tool.cpp
test_ups_client_SOURCES = test_ups_client.cpp
test_mutate_client_SOURCES = test_mutate_client.cpp
test_merger_scanner_encoder_SOURCES = test_merger_scanner_encoder.cpp
test_merger_schema_proxy_SOURCES = test_merger_schema_proxy.cpp $(pub_source)
test_merger_timer_task_SOURCES = test_merger_timer_task.cpp ${pub_source}
test_merger_root_proxy_SOURCES = test_merger_root_proxy.cpp ${pub_source}
test_merger_rpc_proxy_SOURCES = test_merger_rpc_proxy.cpp ${pub_source}
test_merger_rpc_stub_SOURCES = test_merger_rpc_stub.cpp ${pub_source}
test_merger_version_proxy_SOURCES = test_merger_version_proxy.cpp ${pub_source}
test_merger_stlmap_SOURCES = test_merger_stlmap.cpp
test_merger_blacklist_SOURCES = test_merger_blacklist.cpp
test_merger_btreemap_SOURCES = test_merger_btreemap.cpp
test_merger_tablet_location_SOURCES = test_merger_tablet_location.cpp
test_merger_schema_manager_SOURCES = test_merger_schema_manager.cpp
test_groupby_operator_SOURCES = test_groupby_operator.cpp
test_merger_param_decoder_SOURCES = test_merger_param_decoder.cpp
test_merger_mutator_decoder_SOURCES = test_merger_mutator_decoder.cpp
test_read_param_decoder_SOURCES = test_read_param_decoder.cpp
test_read_param_modifier_SOURCES = test_read_param_modifier.cpp
test_param_modifier_SOURCES = test_param_modifier.cpp
test_ms_counter_SOURCES = test_ms_counter.cpp
test_scan_backward_SOURCES = test_scan_backward.cpp
test_cell_operator_SOURCES=test_cell_operator.cpp
test_ob_ms_scan_event_SOURCES=test_ob_ms_scan_event.cpp $(pub_source)
test_ms_scan_param_SOURCES=test_ms_scan_param.cpp
test_mj_client_SOURCES = test_mj_client.cpp
test_merger_sorted_operator_SOURCES = test_merger_sorted_operator.cpp
test_merger_reverse_operator_SOURCES = test_merger_reverse_operator.cpp
test_merger_groupby_operator_SOURCES = test_merger_groupby_operator.cpp
test_merger_operator_SOURCES = test_merger_operator.cpp
test_ob_scan_param_loader_SOURCES = test_ob_scan_param_loader.cpp
test_ob_scanner_loader_SOURCES = test_ob_scanner_loader.cpp
test_ob_location_list_cache_loader_SOURCES = test_ob_location_list_cache_loader.cpp
test_merger_scan_SOURCES = test_merger_scan.cpp
test_ms_sub_get_request_SOURCES = test_ms_sub_get_request.cpp
test_get_merger_SOURCES = test_get_merger.cpp
test_ms_get_event_parellel_SOURCES=test_ms_get_event_parellel.cpp

check_SCRIPTS = $(bin_PROGRAMS)
TESTS = $(check_SCRIPTS) 
CLEANFILES = $(check_SCRIPTS)
EXTRA_DIST=mock_chunk_server.h  mock_define.h  mock_root_server.h  mock_server.h  mock_update_server.h mock_client.h
clean-local:
	-rm -f *.gcov *.gcno *.gcda

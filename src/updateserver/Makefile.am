AM_CPPFLAGS = -I${TBLIB_ROOT}/include/tbsys \
			  -I${TBLIB_ROOT}/include/tbnet \
			  -I${top_srcdir}/include \
			  -I${top_srcdir}/src

CXXFLAGS += -D__UNIT_TEST__
LDADD = $(top_builddir)/src/mergeserver/libmergeserver.a\
		$(top_builddir)/src/sstable/libsstable.a \
		${top_builddir}/src/common/compress/libcomp.a \
		${top_builddir}/src/common/libcommon.a \
		${top_builddir}/src/common/btree/libbtree.a \
		${TBLIB_ROOT}/lib/libtbnet.a \
		${TBLIB_ROOT}/lib/libtbsys.a -lcrypt

CXXFLAGS+=-D__VERSION_ID__="\"updateserver 1.0.0.0\""
CXXFLAGS+=-D_BTREE_ENGINE_
#CXXFLAGS+=-pg
#CXXFLAGS+=-O2 -Wno-strict-aliasing
AM_LDFLAGS = -lpthread -lc -lm -lrt -ldl -laio -lcrypt -lssl

if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

noinst_LIBRARIES = libupdateserver.a
libupdateserver_a_SOURCES = ob_ups_slave_mgr.cpp\
			    ob_trans_buffer.cpp \
				    ob_memtable.cpp \
				    ob_ups_mutator.cpp \
				    ob_ups_table_mgr.cpp \
				    ob_update_server.cpp\
				    ob_update_server_param.cpp \
				    ob_ups_utils.cpp \
				    ob_ups_timer_task.cpp\
				    ob_update_server_main.cpp \
				    ob_ups_log_mgr.cpp \
				    ob_btree_engine_alloc.cpp \
				    ob_store_mgr.cpp \
				    ob_sstable_mgr.cpp \
				    ob_table_mgr.cpp \
				    ob_memtable_rowiter.cpp \
				    ob_log_sync_delay_stat.cpp \
				    ob_slave_sync_type.cpp\
				    ob_ups_replay_runnable.cpp \
				    ob_transfer_sstable_query.cpp \
				    ob_ups_fetch_runnable.cpp \
				    ob_multi_file_utils.cpp \
				    ob_schema_mgrv2.cpp \
				    ob_ups_cache.cpp\
				    ob_client_wrapper.cpp\
				    ob_ups_rpc_stub.cpp\
				    ob_ups_rpc_proxy.cpp\
				    ob_commit_log_receiver.cpp \
				    ob_ups_fetch_lsync.cpp \
				    ob_ups_stat.cpp\
				    ob_log_src.cpp\
				    ob_ups_log_utils.cpp\
				    ob_on_disk_log_locator.cpp\
				    ob_located_log_reader.cpp\
				    ob_pos_log_reader.cpp\
				    ob_data_block.cpp\
				    ob_ring_data_buffer.cpp\
				    ob_log_buffer.cpp\
				    ob_prefetch_log_buffer.cpp\
				    ob_fetched_log.cpp\
				    ob_remote_log_src.cpp\
				    ob_cached_pos_log_reader.cpp\
				    ob_replay_log_src.cpp\
				    ob_query_engine.cpp\
				    ob_trans_mgr.cpp\
				    ob_ups_clog_status.cpp\
				    ob_table_engine.cpp\
				    ob_ups_compact_cell_iterator.cpp\
				    ob_ups_compact_cell_writer.cpp\
				    $(top_srcdir)/svn_version.cpp

bin_PROGRAMS = updateserver ups_mon
updateserver_SOURCES = main.cpp ${libupdateserver_a_SOURCES}
#nodist_updateserver_SOURCES = $(top_srcdir)/svn_version.cpp
ups_mon_SOURCES = ups_mon.cpp
EXTRA_DIST = \
			 ob_log_src.h \
			 ob_on_disk_log_locator.h ob_located_log_reader.h ob_pos_log_reader.h ob_data_block.h ob_ring_data_buffer.h \
			 ob_log_buffer.h ob_fetched_log.h ob_remote_log_src.h ob_cached_pos_log_reader.h ob_replay_log_src.h \
			 ob_cellinfo_processor.h \
			 ob_memtable.h \
			 ob_table_engine.h \
			 ob_trans_buffer.h \
			 ob_update_server.h\
			 ob_update_server_param.h \
			 ob_update_server_main.h \
			 ob_ups_log_mgr.h \
			 ob_ups_mutator.h \
			 ob_ups_replay_runnable.h \
			 ob_ups_table_mgr.h \
			 ob_ups_utils.h \
			 ob_ups_stat.h \
			 ob_memtank.h \
			 ob_btree_engine_alloc.h \
			 ups_mon.h \
			 ob_ups_fetch_runnable.h \
			 ob_table_mgr.h \
			 ob_sstable_mgr.h \
			 ob_transfer_sstable_query.h \
			 ob_schema_mgrv2.h \
			 ob_ups_tmps.h \
			 ob_store_mgr.h \
			 ob_memtable_rowiter.h \
			 ob_multi_file_utils.h \
			 ob_ups_cache.h\
			 ob_client_wrapper.h\
			 ob_ups_rpc_stub.h\
			 ob_ups_rpc_proxy.h\
			 ob_client_wrapper_tsi.h \
			 ob_commit_log_receiver.h \
			 ob_ups_fetch_lsync.h \
			 ob_obi_slave_stat.h\
			 ob_ups_slave_mgr.h\
			 ob_log_sync_delay_stat.h \
			 ob_ups_log_utils.h \
			 ob_slave_sync_type.h\
			 ob_ups_role_mgr.h\
			 ob_ups_timer_task.h\
			 ob_prefetch_log_buffer.h\
			 ob_ups_clog_status.h\
			 ob_log_locator.h\
       ob_query_engine.h\
       ob_trans_mgr.h\
       ob_id_map.h\
       ob_lighty_hash.h\
       ob_ups_compact_cell_iterator.h\
       ob_ups_compact_cell_writer.h\
			 ob_recent_cache.h\
			 ob_resource_pool.h\
			 updateserver.conf.template

# rules to auto generate svn_version.cpp
include $(top_srcdir)/svn_version.mk

pkgdata_data_DATA=updateserver.conf.template
pkgdata_datadir=${prefix}/etc
clean-local:
	-rm -f *.gcov *.gcno *.gcda

AM_CPPFLAGS = -I${TBLIB_ROOT}/include/tbsys \
			  -I${TBLIB_ROOT}/include/tbnet \
			  -I${top_srcdir}/include \
			  -I${top_srcdir}/src

LDADD = $(top_builddir)/src/sstable/libsstable.a \
	$(top_builddir)/src/compactsstable/libcompactsstable.a \
		$(top_builddir)/src/common/compress/libcomp.a \
		$(top_builddir)/src/common/btree/libbtree.a \
		$(top_builddir)/src/common/libcommon.a \
		${TBLIB_ROOT}/lib/libtbnet.a \
		${TBLIB_ROOT}/lib/libtbsys.a


AM_LDFLAGS = -lpthread -lc -lm -lrt -ldl -laio 
if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

noinst_LIBRARIES = libchunkserver.a
libchunkserver_a_SOURCES = ob_chunk_server.cpp \
						   ob_chunk_server_main.cpp \
						   ob_chunk_server_param.cpp \
						   ob_chunk_service.cpp \
						   ob_fileinfo_cache.cpp \
						   ob_tablet_manager.cpp \
						   ob_root_server_rpc.cpp \
						   ob_tablet.cpp \
						   ob_tablet_image.cpp \
						   ob_merge_reader.cpp \
						   ob_disk_manager.cpp \
						   ob_file_recycle.cpp \
						   ob_chunk_merge.cpp \
						   ob_chunk_server_merger_proxy.cpp \
						   ob_block_cache_reader.cpp \
						   ob_block_cache_loader.cpp \
						   ob_switch_cache_utility.cpp \
						   ob_get_cell_stream_wrapper.cpp \
						   ob_row_cell_vec.cpp \
						   ob_read_param_modifier.cpp \
						   ob_schema_task.cpp \
						   ob_rpc_proxy.cpp \
						   ob_join_cache.cpp \
						   ob_cell_stream.cpp \
						   ob_schema_manager.cpp \
						   ob_cell_array_helper.cpp \
						   ob_rpc_stub.cpp \
						   ob_get_cell_stream.cpp \
						   ob_join_get_cell_stream.cpp \
						   ob_scan_cell_stream.cpp \
						   ob_query_agent.cpp \
						   ob_get_scan_proxy.cpp \
						   ob_query_service.cpp \
						   ob_merge_operator.cpp \
						   ob_join_operator.cpp \
						   ob_merge_join_operator.cpp \
						   ob_get_param_cell_iterator.cpp \
						   ob_read_ups_balance.cpp \
						   ob_ups_blacklist.cpp \
						   ob_import_sstable.cpp \
						   ob_tablet_merge_filter.cpp \
						   ob_compactsstable_cache.cpp \
						   ob_multi_tablet_merger.cpp \
						   $(top_srcdir)/svn_version.cpp

bin_PROGRAMS = chunkserver 
chunkserver_SOURCES = ob_disk_path.cpp main.cpp ${libchunkserver_a_SOURCES}

EXTRA_DIST = \
	ob_chunk_server.h \
	ob_chunk_server_main.h \
	ob_chunk_server_merger_proxy.h \
	ob_chunk_server_param.h \
	ob_chunk_service.h \
	ob_disk_manager.h \
	ob_fileinfo_cache.h \
	ob_file_recycle.h \
	ob_merge_reader.h \
	ob_root_server_rpc.h \
	ob_tablet.h \
	ob_tablet_image.h \
	ob_tablet_manager.h \
	ob_chunk_server_stat.h \
	chunkserver.conf.template \
	ob_chunk_merge.h \
	ob_block_cache_reader.h \
	ob_block_cache_loader.h \
	ob_switch_cache_utility.h \
	ob_read_param_modifier.h \
	ob_cell_array_helper.h \
	ob_rpc_proxy.h \
	ob_scan_cell_stream.h \
	ob_rpc_stub.h \
	ob_schema_task.h \
	ob_get_cell_stream.h \
	ob_join_get_cell_stream.h \
	ob_query_agent.h \
	ob_join_cache.h \
	ob_get_cell_stream_wrapper.h \
	ob_cell_stream.h \
	ob_schema_manager.h \
	ob_row_cell_vec.h \
	ob_get_scan_proxy.h \
	ob_query_service.h \
	ob_merge_operator.h \
	ob_join_operator.h \
	ob_merge_join_operator.h \
	ob_get_param_cell_iterator.h \
	ob_read_ups_balance.h \
	ob_ups_blacklist.h \
	ob_import_sstable.h \
	ob_tablet_merge_filter.h \
	ob_compactsstable_cache.h \
	ob_multi_tablet_merger.h

# rules to auto generate svn_version.cpp
include $(top_srcdir)/svn_version.mk

pkgdata_data_DATA=chunkserver.conf.template
pkgdata_datadir=${prefix}/etc
clean-local:
	-rm -f *.gcov *.gcno *.gcda

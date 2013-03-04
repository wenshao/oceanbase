AM_CPPFLAGS = -I${TBLIB_ROOT}/include/tbsys \
			  -I${TBLIB_ROOT}/include/tbnet \
			  -I${top_srcdir}/include \
			  -I${top_srcdir}/src

LDADD = \
		$(top_builddir)/src/chunkserver/libchunkserver.a\
		$(top_builddir)/src/updateserver/libupdateserver.a \
		$(top_builddir)/src/mergeserver/libmergeserver.a\
		$(top_builddir)/src/sstable/libsstable.a \
		$(top_builddir)/src/common/btree/libbtree.a \
		$(top_builddir)/src/common/libcommon.a \
		$(top_builddir)/src/common/compress/libcomp.a\
		${TBLIB_ROOT}/lib/libtbnet.a \
		${TBLIB_ROOT}/lib/libtbsys.a -lcrypt 

AM_LDFLAGS=-lgtest -lpthread -lc -lm  -lrt ${GCOV_LIB} -ldl -laio -lcrypt -lssl
CXXFLAGS+=-D__UNIT_TEST__ -D_BTREE_ENGINE_ #-DNO_STAT
if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

bin_PROGRAMS = test_ups_mutator \
               test_scan \
               test_get \
               test_storemgr \
               test_sstablemgr \
               test_rowiter \
               test_tablemgr \
               test_transfer_sstable_query \
               test_memtable \
               test_replay_local_log \
               test_ob_pos_log_reader \
               test_ob_log_buffer \
               test_ob_prefetch_log_buffer \
               test_ob_fetch_log \
               test_trans_mgr \
               test_async_rw_log

test_ups_mutator_SOURCES = test_ups_mutator.cpp
test_scan_SOURCES = test_scan.cpp test_utils.cpp $(top_builddir)/src/updateserver/ob_ups_stat.cpp
test_get_SOURCES = test_get.cpp test_utils.cpp $(top_builddir)/src/updateserver/ob_ups_stat.cpp
test_memtable_SOURCES = test_memtable.cpp test_utils.cpp $(top_builddir)/src/updateserver/ob_ups_stat.cpp
test_storemgr_SOURCES = test_storemgr.cpp
test_sstablemgr_SOURCES = test_sstablemgr.cpp $(top_builddir)/src/updateserver/ob_ups_stat.cpp
test_rowiter_SOURCES = test_rowiter.cpp test_utils.cpp $(top_builddir)/src/updateserver/ob_ups_stat.cpp
test_tablemgr_SOURCES = test_tablemgr.cpp test_utils.cpp $(top_builddir)/src/updateserver/ob_ups_stat.cpp
test_transfer_sstable_query_SOURCES = test_transfer_sstable_query.cpp test_disk_path.cpp
test_trans_mgr_SOURCES = test_trans_mgr.cpp

test_replay_local_log_SOURCES = test_replay_local_log.cpp ob_log_generator.cpp ob_log_writer2.cpp test_utils2.cpp $(top_builddir)/src/updateserver/ob_ups_stat.cpp
test_ob_pos_log_reader_SOURCES = test_ob_pos_log_reader.cpp ob_log_writer2.cpp ob_log_generator.cpp test_utils2.cpp $(top_builddir)/src/updateserver/ob_ups_stat.cpp
#test_ob_log_sync_SOURCES = test_ob_log_sync.cpp
test_ob_log_buffer_SOURCES = test_ob_log_buffer.cpp ob_log_generator.cpp ob_log_writer2.cpp $(top_builddir)/src/updateserver/ob_ups_stat.cpp
test_ob_prefetch_log_buffer_SOURCES = test_ob_prefetch_log_buffer.cpp ob_log_generator.cpp ob_log_writer2.cpp $(top_builddir)/src/updateserver/ob_ups_stat.cpp
#test_ob_fetch_log_LDADD = $(LDADD) utils/libutils.a
test_ob_fetch_log_SOURCES = test_ob_fetch_log.cpp ob_log_generator.cpp ob_log_writer2.cpp test_utils2.cpp $(top_builddir)/src/updateserver/ob_ups_stat.cpp
test_async_rw_log_SOURCES = test_async_rw_log.cpp ob_log_generator.cpp ob_log_writer2.cpp test_utils2.cpp $(top_builddir)/src/updateserver/ob_ups_stat.cpp

EXTRA_DIST = \
	mock_client.h \
	mock_mem_iterator.h \
	mock_root_server.h \
	mock_server.h \
	ob_final_data.h \
	test_helper.h \
	test_init.h \
	test_ups_table_mgr_helper.h \
	test_utils.h \
	ob_log_generator.h \
	ob_log_writer2.h \
	my_ob_server.h \
	log_utils.h \
	test_utils2.h

DIST_SUBDIRS= stress_test

check_SCRIPTS = $(bin_PROGRAMS)
TESTS = $(check_SCRIPTS) 

CLEANFILES = $(check_SCRIPTS)
clean-local:
	-rm -f *.gcov *.gcno *.gcda

AM_CPPFLAGS = -I${TBLIB_ROOT}/include/tbsys \
			  -I${TBLIB_ROOT}/include/tbnet \
			  -I${top_srcdir}/include \
			  -I${top_srcdir}/tests/updateserver \
			  -I${top_srcdir}/src \
			  -fPIC

LDADD = $(top_builddir)/src/updateserver/libupdateserver.a \
		$(top_builddir)/src/rootserver/librootserver.a \
		$(top_builddir)/src/chunkserver/libchunkserver.a  \
		$(top_builddir)/src/mergeserver/libmergeserver.a \
		$(top_builddir)/src/compactsstable/libcompactsstable.a \
		$(top_builddir)/src/sstable/libsstable.a \
		$(top_builddir)/src/common/libcommon.a \
		$(top_builddir)/src/common/compress/libcomp.a \
		$(top_builddir)/src/common/btree/libbtree.a \
		${TBLIB_ROOT}/lib/libtbnet.a \
		${TBLIB_ROOT}/lib/libtbsys.a -lcrypt

AM_LDFLAGS = -lpthread -lc -lm -lrt -ldl -laio -lreadline -lncurses -lcrypt -lssl  ${GCOV_LIB}
CXXFLAGS += -D_BTREE_ENGINE_
if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

#bin_PROGRAMS = sstable_checker test_client mergemeta gen_sstable cs_admin cs_admin2 merge_meta_new cs_info_reader ups_admin gen_meta databuilder dumpsst gen_data_test gen_data_testV3 log_reader
bin_PROGRAMS = sstable_checker test_client mergemeta gen_sstable merge_meta_new cs_info_reader ups_admin gen_meta databuilder gen_data_test gen_data_testV3 log_reader dumpsst cs_admin

sstable_checker_SOURCES = ob_sstable_checker.cpp
test_client_SOURCES = test_client.cpp  $(top_builddir)/src/updateserver/ob_ups_stat.cpp
databuilder_SOURCES = ob_databuilder.cpp feak_disk_path.cpp
#databuilder_CPPFLAGS= -DORDER_SERIALIZE ${AM_CPPFLAGS} #-DBUILDER_DEBUG
#test_manager_SOURCES = test_manager.cpp ob_disk_path.cpp
mergemeta_SOURCES = ob_merge_meta.cpp ob_tablet_meta.cpp
gen_sstable_SOURCES=gen_sstable.cpp feak_disk_path.cpp
gen_data_test_SOURCES=gen_data_test.cpp ob_disk_path.cpp
gen_data_testV3_SOURCES=gen_data_testV3.cpp ob_disk_path.cpp
#cs_press_SOURCES = base_client.cpp cs_press.cpp client_rpc.cpp common_func.cpp $(top_builddir)/src/updateserver/ob_ups_stat.cpp
cs_admin_SOURCES = base_client.cpp cs_admin.cpp client_rpc.cpp common_func.cpp stats.cpp ob_server_stats.cpp ob_cluster_stats.cpp ob_disk_path.cpp 
cs_admin2_SOURCES = base_client.cpp cs_admin.cpp client_rpc.cpp common_func.cpp stats.cpp ob_server_stats.cpp ob_cluster_stats.cpp ob_disk_path.cpp $(top_builddir)/src/updateserver/ob_ups_stat.cpp
dumpsst_SOURCES = dumpsst.cpp feak_disk_path.cpp common_func.cpp
gen_data_test_CPPFLAGS= -DGEN_SSTABLE_DEBUG ${AM_CPPFLAGS}
merge_meta_new_SOURCES = merge_meta_new.cpp ob_disk_path.cpp

log_reader_SOURCES = log_reader.cpp $(top_builddir)/src/updateserver/ob_ups_stat.cpp
#root_table_reader_SOURCES = rt_root_table_reader.cpp
cs_info_reader_SOURCES = rt_cs_info_reader.cpp
ups_admin_SOURCES = ups_admin.cpp $(top_builddir)/tests/updateserver/test_utils.cpp $(top_builddir)/src/updateserver/ob_ups_stat.cpp
gen_meta_SOURCES = gen_meta.cpp common_func.cpp feak_disk_path.cpp #$(top_builddir)/src/updateserver/ob_ups_stat.cpp
log_locator_SOURCES = log_locator.cpp
#authority_admin_SOURCES = ob_authority_manager_main.cpp ob_authority_manager.cpp

EXTRA_DIST = \
			 data_syntax.h \
			 ob_databuilder.h \
			 ob_sstable_checker.h \
			 ob_tablet_meta.h \
			 gen_sstable.h \
			 base_client.h \
			 cs_press.h \
			 common_func.h \
			 stats.h \
			 client_rpc.h \
			 ob_cluster_stats.h \
			 ob_server_stats.h \
			 cs_admin.h \
			 gen_data_test.h \
			 gen_data_testV3.h \
			 dumpsst.h \
			 oceanbase.sh \
			 sysctl.conf \
			 cs_init.sh \
			 snmpd.conf \
			 ups_init.sh

pkgdata_data_DATA=oceanbase.sh sysctl.conf cs_init.sh snmpd.conf ups_init.sh
pkgdata_datadir=${prefix}
SUBDIRS=syschecker msync dump olapdrive sstable_builder mixed_test obadmin sqltest bigquerytest log_tool

clean-local:
	-rm -f *.gcov *.gcno *.gcda

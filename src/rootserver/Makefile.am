AM_CPPFLAGS = -I${TBLIB_ROOT}/include/tbnet \
			  -I${TBLIB_ROOT}/include/tbsys \
			  -I${top_srcdir}/include \
			  -I${top_srcdir}/src \
				-fPIC

LDADD = $(top_builddir)/src/common/libcommon.a \
	${TBLIB_ROOT}/lib/libtbnet.a \
	${TBLIB_ROOT}/lib/libtbsys.a

AM_LDFLAGS = -lpthread -lc -lm -lrt
if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

pub_source = ob_chunk_server_manager.cpp\
ob_root_server2.cpp \
ob_root_worker.cpp \
ob_root_log_worker.cpp \
ob_tablet_info_manager.cpp \
ob_root_meta2.cpp \
ob_root_table2.cpp \
ob_root_log_replay.cpp \
ob_root_log_manager.cpp \
ob_root_rpc_stub.cpp \
ob_root_fetch_thread.cpp\
ob_root_stat_key.cpp\
ob_chunk_server_manager.h \
ob_root_log_worker.h \
ob_root_main.h \
ob_root_meta2.h \
ob_root_server2.h \
ob_root_table2.h \
ob_root_worker.h \
ob_tablet_info_manager.h \
ob_root_log_manager.h \
ob_root_rpc_stub.h \
ob_root_log_replay.h \
ob_root_stat.h \
ob_root_fetch_thread.h \
rootserver.conf.template\
ob_root_stat_key.h\
ob_root_admin_cmd.h\
ob_root_admin2.h\
ob_server_balance_info.h ob_server_balance_info.cpp\
ob_ups_manager.h ob_ups_manager.cpp\
ob_root_config.h ob_root_config.cpp\
ob_ups_heartbeat_runnable.h ob_ups_heartbeat_runnable.cpp\
ob_ups_check_runnable.h ob_ups_check_runnable.cpp\
ob_migrate_info.h ob_migrate_info.cpp\
ob_root_balancer.h ob_root_balancer.cpp\
ob_root_balancer_runnable.h ob_root_balancer_runnable.cpp\
ob_root_util.h ob_root_util.cpp\
ob_restart_server.h ob_restart_server.cpp

noinst_LIBRARIES = librootserver.a
bin_PROGRAMS = rootserver schema_reader checkpoint2str str2checkpoint rs_admin rs_stress clist2str

librootserver_a_SOURCES = ${pub_source}
rootserver_SOURCES = ob_root_main.cpp ob_rt_main.cpp ${pub_source}
nodist_rootserver_SOURCES = $(top_srcdir)/svn_version.cpp
schema_reader_SOURCES = ob_schema_reader.cpp ${pub_source}
checkpoint2str_SOURCES = ob_check_point2str.cpp ${pub_source}
str2checkpoint_SOURCES = ob_str2check_point.cpp ${pub_source}
rs_admin_SOURCES = ob_root_admin2.cpp ob_root_admin2_main.cpp ${pub_source}
nodist_rs_admin_SOURCES = $(top_srcdir)/svn_version.cpp
rs_stress_SOURCES = ob_rs_stress.cpp ${pub_source}
nodist_rs_stress_SOURCES = $(top_srcdir)/svn_version.cpp
clist2str_SOURCES = ob_clist2str.cpp ${pub_source}

pkgdata_data_DATA=rootserver.conf.template
pkgdata_datadir=${prefix}/etc

include Flymake.mk

# rules to auto generate svn_version.cpp
include $(top_srcdir)/svn_version.mk
clean-local:
	-rm -f *.gcov *.gcno *.gcda

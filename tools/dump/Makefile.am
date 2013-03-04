AM_CPPFLAGS = -I${TBLIB_ROOT}/include/tbsys \
	      -I${TBLIB_ROOT}/include/tbnet \
	      -I${top_srcdir}/include \
	      -I${top_srcdir}/src \
	      -I./include

LDADD = $(top_builddir)/src/updateserver/libupdateserver.a \
	$(top_builddir)/src/rootserver/librootserver.a \
	$(top_builddir)/src/chunkserver/libchunkserver.a \
	$(top_builddir)/src/mergeserver/libmergeserver.a \
	$(top_builddir)/src/sstable/libsstable.a \
	$(top_builddir)/src/common/compress/libcomp.a \
	$(top_builddir)/src/common/libcommon.a \
	$(top_builddir)/src/common/btree/libbtree.a \
	${TBLIB_ROOT}/lib/libtbnet.a \
	${TBLIB_ROOT}/lib/libtbsys.a -lcrypt

AM_LDFLAGS = -lpthread -lc -lm -lrt -ldl ${GCOV_LIB} -laio -lcrypt -lssl
CXXFLAGS += -I${JAVA_HOME}/include -DDUMP_DEBUG

if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

EXTRA_DIST= db_dumper_checkpoint.h db_file_utils.h db_queue.h \
	    db_table_info.h ob_api_util.h oceanbase_db.h \
	    db_dumper_config.h db_import.h db_record_filter.h \
	    db_thread_mgr.h ob_import_comsumer.h slice.h \
	    db_dumper.h db_log_monitor.h db_record_formator.h db_utils.h \
	    ob_import.h tokenizer.h db_dumper_mgr.h db_msg_report.h db_record.h \
	    file_appender.h ob_import_param.h db_dumper_writer.h db_parse_log.h db_record_set.h \
	    file_reader.h ob_import_producer.h \
	    scripts/obdump.sh scripts/dumper.ini scripts/ob_check_done.sh \
	    scripts/dumper.ini_master ob_data_set.h

obdump_SOURCES=db_record_set.cpp oceanbase_db.cpp db_table_info.cpp db_record.cpp db_dumper.cpp \
	       db_dumper_config.cpp db_parse_log.cpp main.cpp db_dumper_writer.cpp \
	       db_dumper_mgr.cpp db_log_monitor.cpp db_thread_mgr.cpp db_msg_report.cpp \
	       db_record_filter.cpp db_dumper_checkpoint.cpp db_utils.cpp db_record_formator.cpp \
	       file_appender.cpp $(top_builddir)/src/updateserver/ob_ups_stat.cpp

tablet_stats_SOURCES=db_record_set.cpp oceanbase_db.cpp db_table_info.cpp db_record.cpp db_dumper.cpp \
		     db_dumper_config.cpp db_parse_log.cpp db_dumper_writer.cpp \
		     db_dumper_mgr.cpp db_log_monitor.cpp db_thread_mgr.cpp db_msg_report.cpp \
		     db_record_filter.cpp db_dumper_checkpoint.cpp db_utils.cpp db_record_formator.cpp \
		     tablet_stats.cpp file_appender.cpp $(top_builddir)/src/updateserver/ob_ups_stat.cpp

ob_import_SOURCES =  tokenizer.cpp ob_import.cpp ob_import_producer.cpp \
		     db_utils.cpp file_reader.cpp oceanbase_db.cpp db_record_set.cpp \
		     ob_import_comsumer.cpp ob_import_param.cpp ob_import_main.cpp \
		     db_table_info.cpp db_record.cpp $(top_builddir)/src/updateserver/ob_ups_stat.cpp \
				 file_appender.cpp

ob_export_SOURCES = oceanbase_db.cpp db_record_set.cpp db_utils.cpp \
										 db_table_info.cpp db_record.cpp ob_data_set.cpp tokenizer.cpp \
										 $(top_builddir)/src/updateserver/ob_ups_stat.cpp ob_export_main.cpp
obbinlog_SOURCES = obbinlog.cpp $(top_builddir)/src/updateserver/ob_ups_stat.cpp

bin_PROGRAMS = obdump ob_import tablet_stats obbinlog ob_export
SUBDIRS=scripts
DIST_SUBDIRS=scripts
clean-local:
	-rm -f *.gcov *.gcno *.gcda

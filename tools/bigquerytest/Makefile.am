AM_CPPFLAGS = -I${TBLIB_ROOT}/include/tbsys \
			  -I${TBLIB_ROOT}/include/tbnet \
			  -I${top_srcdir}/include \
				-I${top_srcdir}/tools/bigquerytest/include/mysql \
				-I${top_srcdir}/tools/bigquerytest/include/postgre\
			  -I${top_srcdir}/src\
				-I${top_srcdir}/src/client/cpp

LDADD = $(top_builddir)/src/chunkserver/libchunkserver.a \
		$(top_builddir)/src/client/cpp/.libs/libobapi.a \
		$(top_builddir)/src/mergeserver/libmergeserver.a \
		$(top_builddir)/tools/bigquerytest/lib/libpq.a \
	        $(top_builddir)/tools/bigquerytest/lib/libmysqlclient_r.a\
		$(top_builddir)/src/common/compress/libcomp.a \
		$(top_builddir)/src/common/libcommon.a \
		$(top_builddir)/src/common/btree/libbtree.a \
		${TBLIB_ROOT}/lib/libtbnet.a \
		${TBLIB_ROOT}/lib/libtbsys.a

AM_LDFLAGS = -lpq -lmysqlclient_r -lpthread -lc -lm -lrt -ldl -lnuma -Llib -L$(top_builddir)/src/client/cpp/
#AM_LDFLAGS = -lpq -lmysqlclient -lpthread -lc -lm -lrt -ldl -lnuma
CXXFLAGS = -D__STDC_LIMIT_MACROS -g -Wall -DORDER_SERIALIZE #-O2 -fno-strict-aliasing
if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

noinst_LIBRARIES = libbigquery.a
bin_PROGRAMS = bigquery

libbigquery_a_SOURCES = \
									obsql_client.cpp \
									util.cpp \
									prefix_info.cpp \
									key_generator.cpp \
									value_generator.cpp \
									bigquery.cpp \
									bigquery_reader.cpp \
									bigquery_writer.cpp \
									read_worker.cpp \
									write_worker.cpp\
									bigquerytest_param.cpp \
									bigquerytest.cpp \
									bigquerytest_main.cpp

bigquery_SOURCES = $(libbigquery_a_SOURCES) main.cpp

EXTRA_DIST = obsql_client.h \
						 util.h \
						 prefix_info.h \
						 key_generator.h \
						 value_generator.h \
						 bigquery.h \
						 bigquery_reader.h \
						 bigquery_writer.h \
						 read_worker.h \
						 write_worker.h \
						 bigquerytest_param.h \
						 bigquerytest.h \
						 bigquerytest_main.h\
						 include\
						 lib\
						 data
clean-local:
	-rm -f *.gcov *.gcno *.gcda

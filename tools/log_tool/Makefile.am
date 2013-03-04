AM_CPPFLAGS = -I${TBLIB_ROOT}/include/tbsys \
			  -I${TBLIB_ROOT}/include/tbnet \
			  -I${top_srcdir}/include \
        -I${top_srcdir}/tests/updateserver \
			  -I${top_srcdir}/src 

LDADD = $(top_builddir)/src/updateserver/libupdateserver.a \
	$(top_builddir)/src/rootserver/librootserver.a \
	$(top_builddir)/src/chunkserver/libchunkserver.a  \
	$(top_builddir)/src/mergeserver/libmergeserver.a \
	$(top_builddir)/src/sstable/libsstable.a \
	$(top_builddir)/src/common/libcommon.a \
		$(top_builddir)/src/common/compress/libcomp.a \
		$(top_builddir)/src/common/btree/libbtree.a \
		${TBLIB_ROOT}/lib/libtbnet.a \
		${TBLIB_ROOT}/lib/libtbsys.a -lcrypt

AM_LDFLAGS = -O2 -lpthread -lc -lm -lrt -ldl -laio -lcrypt -lssl ${GCOV_LIB}
CXXFLAGS += -g -O2
if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

bin_PROGRAMS = log_tool client
log_tool_SOURCES = log_tool.cpp $(top_builddir)/src/updateserver/ob_ups_stat.cpp
client_SOURCES = ob_client2.cpp ob_utils.cpp $(top_builddir)/src/updateserver/ob_ups_stat.cpp

EXTRA_DIST = cmd_args_parser.h  file_utils.h  ob_client2.h  ob_utils.h

clean-local:
	-rm -f *.gcov *.gcno *.gcda

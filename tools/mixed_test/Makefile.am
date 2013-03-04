AM_CPPFLAGS = -I${TBLIB_ROOT}/include/tbsys \
			  -I${TBLIB_ROOT}/include/tbnet \
			  -I${top_srcdir}/include \
			  -I${top_srcdir}/src \
			  -I${top_srcdir}/tests/updateserver

LDADD = $(top_builddir)/src/updateserver/libupdateserver.a \
    $(top_builddir)/src/chunkserver/libchunkserver.a \
	  $(top_builddir)/src/mergeserver/libmergeserver.a \
    $(top_builddir)/src/sstable/libsstable.a \
    $(top_builddir)/src/common/btree/libbtree.a \
    $(top_builddir)/src/common/libcommon.a \
    $(top_builddir)/src/common/compress/libcomp.a \
    $(top_builddir)/src/client/cpp/.libs/libobapi.a \
	  $(top_builddir)/src/rootserver/librootserver.a \
		${TBLIB_ROOT}/lib/libtbnet.a \
		${TBLIB_ROOT}/lib/libtbsys.a -lcrypt

AM_LDFLAGS=-lpthread -lc -lm -lrt -ldl -laio -lcrypt -lssl
CXXFLAGS+=-D_BTREE_ENGINE_

if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif


bin_PROGRAMS = multi_write total_scan random_read launcher

EXTRA_DIST = \
						 mutator_builder.h \
						 rowkey_builder.h\
						 cellinfo_builder.h\
						 client_wrapper.h\
						 row_checker.h\
						 schema_compatible.h\
						 utils.h
libupdateserver_a_SOURCES = mutator_builder.cpp \
                            rowkey_builder.cpp \
                            utils.cpp \
                            cellinfo_builder.cpp \
                            client_wrapper.cpp \
                            row_checker.cpp \
                            $(top_builddir)/tests/updateserver/test_utils.cpp \
                            $(top_builddir)/src/updateserver/ob_ups_stat.cpp

multi_write_SOURCES = multi_write.cpp ${libupdateserver_a_SOURCES}

total_scan_SOURCES = total_scan.cpp ${libupdateserver_a_SOURCES}

random_read_SOURCES = random_read.cpp ${libupdateserver_a_SOURCES}

launcher_SOURCES = launcher.cpp


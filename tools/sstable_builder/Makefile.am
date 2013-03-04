AM_LDFLAGS=-lpthread -lc -lm -lrt
if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

lib_LTLIBRARIES = libmrsstable.la
libmrsstable_la_SOURCES = sstable_builder.cpp \
						  sstable_writer.cpp

libmrsstable_la_CPPFLAGS = -I${TBLIB_ROOT}/include/tbsys \
						   -I${TBLIB_ROOT}/include/tbnet \
						   -I${top_srcdir}/src \
						   -I${JAVA_HOME}/include \
						   -I${JAVA_HOME}/include/linux \
						   -fPIC

libmrsstable_la_LDFLAGS = -L${top_builddir}/src/sstable/ \
						  -L${top_builddir}/src/common/ \
						  -L${top_builddir}/src/common/compress/ \
						  -L${TBLIB_ROOT}/lib/ \
						  -lsstable -lcommon -lcomp \
						  -Wl,-static,-ltbnet \
						  -Wl,-static,-ltbsys \
						  -lpthread -lc -lm -lrt -ldl
											
bin_PROGRAMS = test_sstable_builder
test_sstable_builder_SOURCES = test_sstable_builder.cpp ${libmrsstable_la_SOURCES}
test_sstable_builder_LDADD = $(top_builddir)/src/sstable/libsstable.a \
							 $(top_builddir)/src/common/libcommon.a \
							 $(top_builddir)/src/common/compress/libcomp.a \
							 ${TBLIB_ROOT}/lib/libtbnet.a \
							 ${TBLIB_ROOT}/lib/libtbsys.a \
							 -lpthread -lc -lm -lrt -ldl

test_sstable_builder_CPPFLAGS = -I${TBLIB_ROOT}/include/tbsys \
								-I${TBLIB_ROOT}/include/tbnet \
								-I${JAVA_HOME}/include \
								-I${JAVA_HOME}/include/linux \
								-I${top_srcdir}/src 

EXTRA_DIST = sstable_builder.h \
			 sstable_writer.h \
			 data_syntax.h \
			 com_taobao_mrsstable_SSTableBuilder.h

AM_CPPFLAGS= -I${TBLIB_ROOT}/include/tbsys \
			 -I${top_srcdir}/include \
			 -I${top_srcdir}/src \
			 -I${top_srcdir}/src/common \
			 -I${top_srcdir}/src/common/hash

LDADD=$(top_builddir)/src/common/libcommon.a \
	  ${TBLIB_ROOT}/lib/libtbsys.a 

AM_LDFLAGS=-lpthread -lc -lm  -lgtest ${GCOV_LIB}
if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

bin_PROGRAMS=hash_benz \
			 test_hashmap \
       test_hashset \
			 test_simpleallocer

hash_benz_SOURCES = hash_benz.cpp
test_hashmap_SOURCES = test_hashmap.cpp
test_simpleallocer_SOURCES = test_simpleallocer.cpp
test_hashset_SOURCES = test_hashset.cpp

check_SCRIPTS = $(bin_PROGRAMS)
TESTS = $(check_SCRIPTS) 

CLEANFILES = $(check_SCRIPTS)
clean-local:
	-rm -f *.gcov *.gcno *.gcda

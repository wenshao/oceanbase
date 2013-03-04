AM_LDFLAGS = -L/usr/lib64/mysql/ -lmysqlclient -lgtest -lpthread

if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

bin_PROGRAMS=newsqltest
newsqltest_SOURCES=ob_new_sql_test.cpp

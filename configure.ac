    AC_INIT([OceanBase],
            [0.3.1.2],
            [qushan@taobao.com],
            [oceanbase])

    obapi_version="1.0.0"
    AC_SUBST(obapi_version)

    AC_PROG_LIBTOOL
    AM_INIT_AUTOMAKE(oceanbase, 0.3.1.2)
    ac_default_prefix=${HOME}/oceanbase_bin
    AC_PROG_CXX
    AC_LANG([C++])
    AC_ARG_WITH([tblib-root],
           AS_HELP_STRING([--with-tblib-root],
           [tbnet installation path]),
           TBLIB_ROOT=${withval})

    if  ! test -d "${TBLIB_ROOT}/include"; then
       AC_MSG_ERROR([TBLIB_ROOT have not been set])
       exit 1;
    fi
    dnl tbnet include path supplied
    AC_SUBST([TBLIB_ROOT])

    AC_ARG_WITH([release],
           AS_HELP_STRING([--with-release],
                           [use optimize (default is NO)]),
                           [
                            if test "$withval" = "yes"; then
                                    CXXFLAGS="-g -O2 -D_NO_EXCEPTION -DCOMPATIBLE -finline-functions \
									-D__STDC_LIMIT_MACROS -D__STDC_CONSTANT_MACROS -DNDEBUG\
				          -Wall -Werror -Wextra -Wunused-parameter -Wformat -fno-strict-aliasing \
					      -Wconversion -Wno-deprecated -D__USE_LARGEFILE64 -D_FILE_OFFSET_BITS=64 \
					      -D_LARGE_FILE -D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE" #-lprofiler
                            fi
                            ],
                            [ CXXFLAGS="-D__STDC_LIMIT_MACROS -D__STDC_CONSTANT_MACROS -D_NO_EXCEPTION  -DCOMPATIBLE \
							-Wall -Werror -Wextra -Wunused-parameter \
			    			-Wformat -Wconversion -Wno-deprecated -fno-strict-aliasing -g"]
                            )

    #check gcc version, add -Wno-ignored-qualifiers flag for gcc version greater than 4.3.0
    GCC_VERSION=`$CC -dumpversion`
    if test $? -eq 0; then
        major=`echo $GCC_VERSION | cut -d. -f1`
        minor=`echo $GCC_VERSION | cut -d. -f2`
    fi
    if test $major -eq 4 -a $minor -ge 3 ; then
        CXXFLAGS="${CXXFLAGS} -Wno-ignored-qualifiers"
    fi

    AC_ARG_WITH([test_case],
           AS_HELP_STRING([--with-test-case],
		          [with test case (default is YES)]),
	   		  [
			    if test "$withval" = "no"; then
			    	test_case=no
			    fi
			  ],
			  [ test_case=yes ]
	   )

    AC_ARG_WITH([coverage],
           AS_HELP_STRING([--with-coverage],
		          [with coverage (default is NO)]),
	   		  [
			    if test "$withval" = "yes"; then
			    	coverage=yes
			    fi
			  ],
			  [coverage=no]
	   )

	AC_ARG_VAR([RELEASEID], [release number when building in Taobao ABS])
	AC_DEFINE_UNQUOTED([RELEASEID], "$RELEASEID")

    AM_CONDITIONAL([WITH_TEST_CASE], test x$test_case = xyes )

    AM_CONDITIONAL([COVERAGE], test x$coverage = xyes )

    dnl see whether subversion is installed
    AC_PATH_PROG(svnversioncommand, svnversion)

    dnl use svnversion to record the current repository revision only if
    dnl subversion is installed and we are in a working copy
    if test "X$svnversioncommand" = "X" || test `$svnversioncommand -n '.'` = "exported"; then
        mysvnversion="cat \$(top_srcdir)/svn_dist_version"
        AM_CONDITIONAL([HAVESVNWC], false)
    else
        mysvnversion="svnversion -n '.'"
        AM_CONDITIONAL([HAVESVNWC], true)
    fi

    AC_SUBST(mysvnversion)

    AC_CONFIG_FILES([\
                     Makefile \
                     tools/Makefile \
                     tools/olapdrive/Makefile \
		                 tools/syschecker/Makefile \
		                 tools/syschecker/client/Makefile \
		                 tools/msync/Makefile \
                     tools/sqltest/Makefile \
                     tools/bigquerytest/Makefile \
                     tools/dump/Makefile \
                     tools/dump/scripts/Makefile \
		                 tools/obadmin/Makefile \
                     src/Makefile \
                     src/common/Makefile \
                     src/common/compress/Makefile \
                     src/common/btree/Makefile \
                     src/common/btree/test/Makefile \
                     src/rootserver/Makefile \
                     src/chunkserver/Makefile \
                     src/updateserver/Makefile \
                     src/mergeserver/Makefile \
                     src/sstable/Makefile \
                     src/clustermanager/Makefile \
                     src/mms/Makefile \
                     src/lsync/Makefile \
                     src/client/Makefile \
                     src/client/cpp/Makefile \
                     src/compactsstable/Makefile \
                     src/sql/Makefile \
                     src/importserver/Makefile \
                     tests/Makefile \
                     tests/chunkserver/Makefile \
                     tests/common/Makefile \
                     tests/common/compress/Makefile \
                     tests/common/hash/Makefile \
                     tests/rootserver/Makefile \
                     tests/updateserver/Makefile \
                     tests/updateserver/stress_test/Makefile \
                     tests/mergeserver/Makefile \
                     tests/mergeserver/olap/Makefile \
                     tests/clustermanager/Makefile \
                     tests/mms/Makefile \
                     tests/sql/Makefile \
                     tests/sstable/Makefile \
                     tests/lsync/Makefile \
                     tests/compactsstable/Makefile \
                     tools/mixed_test/Makefile \
                     tools/log_tool/Makefile \
                     tools/sstable_builder/Makefile
                     ])
    AC_OUTPUT

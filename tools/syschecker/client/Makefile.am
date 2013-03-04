AM_CPPFLAGS = -I${TBLIB_ROOT}/include/tbsys \
			  -I${TBLIB_ROOT}/include/tbnet \
			  -I${top_srcdir}/include \
			  -I${top_srcdir}/src 

LDADD = $(top_builddir)/src/chunkserver/libchunkserver.a \
	$(top_builddir)/src/mergeserver/libmergeserver.a \
		$(top_builddir)/src/common/compress/libcomp.a \
		$(top_builddir)/src/common/libcommon.a \
		$(top_builddir)/src/common/btree/libbtree.a \
		${TBLIB_ROOT}/lib/libtbnet.a \
		${TBLIB_ROOT}/lib/libtbsys.a 

AM_LDFLAGS = -lpthread -lc -lm -lrt -ldl

noinst_LIBRARIES = libclient.a
libclient_a_SOURCES = ob_client.cpp \
					  ob_base_client.cpp \
					  ob_server_manager.cpp \
					  ob_server_rpc.cpp

EXTRA_DIST = ob_client.h \
			 ob_base_client.h \
			 ob_server_manager.h \
			 ob_server_rpc.h

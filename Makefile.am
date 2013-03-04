EXTRA_DIST=build.sh rpm/oceanbase.spec svn_dist_version

if WITH_TEST_CASE
TEST_DIR:=tests
endif
SUBDIRS=src $(TEST_DIR) tools
DIST_SUBDIRS=src tests tools

utest: check

.PHONY: utest

ABS_PATH=$(shell pwd)
TMP_DIR := /$(ABS_PATH)/oceanbase-tmp.$(shell echo $$$$)
PREFIX := $(shell [ x$(PREFIX) = x ] && PREFIX=/usr/local/oceanbase-$(VERSION) && echo "$${PREFIX}")
RELEASE := $(shell [ x$(RELEASE) = x ] && RELEASE="0" && echo "$${RELEASE}")
rpms: 
	make dist-gzip rpm/oceanbase.spec
	mkdir -p $(TMP_DIR)
	mkdir -p $(TMP_DIR)/BUILD
	mkdir -p $(TMP_DIR)/RPMS
	mkdir -p $(TMP_DIR)/SOURCES
	mkdir -p $(TMP_DIR)/SRPMS
	cp oceanbase-${VERSION}.tar.gz $(TMP_DIR)/SOURCES
	echo $(PREFIX)
	echo "RELEASE:$(RELEASE)"
	cd $(TMP_DIR)/BUILD; \
	tar xfz $(TMP_DIR)/SOURCES/oceanbase-${VERSION}.tar.gz \
	        oceanbase-$(VERSION)/rpm/oceanbase.spec
	rpmbuild --define "_topdir $(TMP_DIR)" \
	         --define "NAME $(PACKAGE)" \
	         --define "VERSION $(VERSION)" \
	         --define "_prefix $(PREFIX)" \
	         --define "RELEASE $(RELEASE)" \
	        -ba $(TMP_DIR)/BUILD/oceanbase-$(VERSION)/rpm/oceanbase.spec
	find $(TMP_DIR)/RPMS/ -name "*.rpm" -exec mv '{}' . \;
	rm -rf $(TMP_DIR)

# rules to auto generate svn_version.cpp
include $(top_srcdir)/svn_version.mk

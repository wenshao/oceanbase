#
# (C) 2007-2010 TaoBao Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License version 2 as
# published by the Free Software Foundation.
#
# oceanbase.spec is for what ...
#
# Version: $id$
#
# Authors:
#   MaoQi maoqi@taobao.com
#

Name: %NAME
Version: %VERSION
Release: %{RELEASE}
#Release: 1
Summary: TaoBao distributed database
Group: Application
URL: http:://yum.corp.alimama.com
Packager: taobao
License: GPL
Vendor: TaoBao
Prefix:%{_prefix}
Source:%{NAME}-%{VERSION}.tar.gz
BuildRoot: %(pwd)/%{name}-root
BuildRequires: t-csrd-tbnet-devel >= 1.0.7 lzo >= 2.06 snappy >= 1.0.2 numactl-devel >= 0.9.8 libaio-devel >= 0.3 openssl-devel >= 0.9.8e

%package -n oceanbase-dump
summary: Oceanbase dump tools
group: Development/Libraries
Version: %VERSION
Release: %{RELEASE}
#Release: 1

%define dump_prefix /home/admin/obdump

%description
OceanBase is a distributed database

%description -n oceanbase-dump
Oceanbase dumper 

%define _unpackaged_files_terminate_build 0

%prep
%setup

%build
chmod u+x build.sh
./build.sh init
./configure RELEASEID=%{RELEASE} --prefix=%{_prefix} --with-test-case=no --with-release=yes --with-tblib-root=/opt/csr/common
#./configure --prefix=%{_prefix} --with-test-case=no --with-release=yes
make %{?_smp_mflags}
#make

%install
rm -rf $RPM_BUILD_ROOT

#build dir for obdumper
mkdir -p $RPM_BUILD_ROOT%{dump_prefix}/bin
mkdir -p $RPM_BUILD_ROOT%{dump_prefix}/conf
mkdir -p $RPM_BUILD_ROOT%{dump_prefix}/data
mkdir -p $RPM_BUILD_ROOT%{dump_prefix}/tmp_log

make DESTDIR=$RPM_BUILD_ROOT install

mv $RPM_BUILD_ROOT%{_prefix}/etc/obdump.sh         $RPM_BUILD_ROOT%{dump_prefix}
mv $RPM_BUILD_ROOT%{_prefix}/bin/obdump            $RPM_BUILD_ROOT%{dump_prefix}/bin
mv $RPM_BUILD_ROOT%{_prefix}/etc/ob_check_done.sh  $RPM_BUILD_ROOT%{dump_prefix}/bin
mv $RPM_BUILD_ROOT%{_prefix}/etc/dumper.ini        $RPM_BUILD_ROOT%{dump_prefix}/conf
mv $RPM_BUILD_ROOT%{_prefix}/etc/dumper.ini_master $RPM_BUILD_ROOT%{dump_prefix}/conf

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(0755, admin, admin)
%{_prefix}/oceanbase.sh
%{_prefix}/cs_init.sh
%{_prefix}/ups_init.sh
%{_prefix}/sysctl.conf
%{_prefix}/snmpd.conf
%{_prefix}/bin/rootserver
%{_prefix}/bin/updateserver
%{_prefix}/bin/mergeserver
%{_prefix}/bin/chunkserver
%{_prefix}/bin/rs_admin
%{_prefix}/bin/ups_admin
#%{_prefix}/bin/cs_admin
%{_prefix}/bin/ob_ping
%{_prefix}/bin/ups_mon
%{_prefix}/bin/str2checkpoint
%{_prefix}/bin/checkpoint2str
%{_prefix}/bin/schema_reader
%{_prefix}/bin/lsyncserver
%{_prefix}/bin/msyncclient
%{_prefix}/bin/ob_import
%{_prefix}/bin/log_reader
%{_prefix}/bin/importserver.py
%{_prefix}/bin/importcli.py
%{_prefix}/bin/copy_sstable.py
%{_prefix}/bin/mrsstable.jar
%{_prefix}/bin/dispatch.sh
%{_prefix}/lib
%{_prefix}/etc

%files -n oceanbase-dump
# files for oceanbase-dump
%defattr(0755, admin, admin)
%{dump_prefix}/obdump.sh
%{dump_prefix}/bin/obdump
%{dump_prefix}/bin/ob_check_done.sh
%{dump_prefix}/conf/dumper.ini
%{dump_prefix}/conf/dumper.ini_master
%{dump_prefix}/data
%{dump_prefix}/tmp_log

%post
chown -R admin:admin /home/admin/oceanbase
mv %{_prefix}/cs_init.sh %{_prefix}/bin
mv %{_prefix}/ups_init.sh %{_prefix}/bin


#!/bin/bash
#for taobao abs
export temppath=$1
temppath=$1
cd $temppath/rpm
if [ `cat /etc/redhat-release|cut -d " " -f 7|cut -d "." -f 1` = 4 ]
then
	RELEASE=$4.el4
else
	RELEASE=$4.el5
fi
VERSION=$3

cd $temppath
chmod +x build.sh
./build.sh init
export TBLIB_ROOT=/opt/csr/common
./configure  --with-test-case=no --with-release=yes --with-tblib-root=/opt/csr/common
make PREFIX=/home/admin/oceanbase RELEASE=$RELEASE VERSION=$VERSION rpms
mv *.rpm rpm/

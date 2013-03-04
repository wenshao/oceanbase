export CC=gcc;
export CXX=g++;
pwd=${PWD}
update="yes"
url=http://svn.app.taobao.net/repos/oceanbase/branches/rev_0_3_0_ii_dev/oceanbase
while getopts "a:n" arg
do
  case $arg in
    a)
      url=$OPTARG
      echo "address:$url"
      ;;
    n)
      echo "do not checkout"
      update="no"
      ;;
  esac
done

if [ x$update = x"yes" ]; then
  mkdir -p auto_run;
  cd auto_run; svn co $url
  cd oceanbase;
  ./build.sh init; ./configure; 
  cd src;
  make -j;
  cd ../;
  cd $pwd; 
  ./copy.sh $pwd/auto_run/oceanbase;
fi

#deploy bigquery
./deploy.py ob3.random_test check 5 >check_err;
basic_err=1;
if grep "not pass basic check before timeout" check_err >basic_err; then
  basic_err=0;
fi
result_err=1;
if grep "ERROR" check_err >result_err; then
  result_err=0;
fi

echo "basic_err="$basic_err",result_err="$result_err;

if [ $basic_err -eq 1 ] && [ $result_err -eq 1 ]; then
  echo "re-deploy";
  ./deploy.py ob3.stop; ./deploy.py ob3.[conf,rsync];./deploy.py ob3.restart;./deploy.py ob3.start;
  sleep 240
  ./deploy.py ob3.ct.reboot;
fi

#clean
#/bin/rm -rf auto_run


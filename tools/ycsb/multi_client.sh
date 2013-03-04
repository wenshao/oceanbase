if [ $# -lt 1 ]
then
   echo "param should be >= 1"
   exit
fi
command='export M3_HOME=/home/rizhao.ych/tools/apache-maven-3.0.4/;export PATH=$PATH:$HOME/bin:$HOME/tools/bin:$M3_HOME/bin; export JAVA_HOME=/usr/java/jdk1.6.0_21; cd ~/tools/ycsb; ./batch_run.sh log/result_get_20121031_'$1' '$1' 2>&1 &'
echo $command
pgm -f server_list $command

if [ $# -lt 2 ]
then
   echo "param should be >= 2"
   exit
fi
for (( i=1; i <= $2; i++ ));
do
  ./run.sh >$1_$i 2>&1 &
done


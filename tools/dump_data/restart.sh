#!/bin/bash

killall task_server
killall task_worker

mkdir -p log
mkdir -p data

rm -fr log/*.log
rm -fr data/*

#./task_server -f server.conf -t collect_item -t collect_info &
./task_server -f server.conf -t test9 &

sleep 3

#./task_worker -a 127.0.0.1 -p 10234 -l log/client1.log 1>1.data 2>&1 &
#exit

./task_worker -a 10.232.35.40 -p 10234 -f ./data/ -c worker.conf -l log/client1.log &
exit

./task_worker -a 10.232.35.40 -p 10234 -f ./data/ -l log/client2.log &
./task_worker -a 10.232.35.40 -p 10234 -f ./data/ -l log/client3.log &

exit

./task_worker -a 127.0.0.1 -p 10234 -f ./data/ -l log/client4.log &
./task_worker -a 127.0.0.1 -p 10234 -f ./data/ -l log/client5.log &
./task_worker -a 127.0.0.1 -p 10234 -f ./data/ -l log/client6.log &


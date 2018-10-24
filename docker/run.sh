#!/bin/bash

# Start mysql
chown -R mysql:mysql /var/lib/mysql /var/run/mysqld
service mysql start
status=$?
if [ $status -ne 0 ]; then
  echo "Failed to start mysql: $status"
  exit $status
fi

# Start mycat
cd /usr/local/mycat2/bin
./mycat start
status=$?
if [ $status -ne 0 ]; then
  echo "Failed to start mycat: $status"
  exit $status
fi

while sleep 60; do
  ps aux |grep mysql |grep -q -v grep
  PROCESS_1_STATUS=$?
  ps aux |grep mycat |grep -q -v grep
  PROCESS_2_STATUS=$?
  # If the greps above find anything, they exit with 0 status
  # If they are not both 0, then something is wrong
  if [ $PROCESS_1_STATUS -ne 0 -o $PROCESS_2_STATUS -ne 0 ]; then
    echo "One of the service has already exited."
    exit 1
  fi
done
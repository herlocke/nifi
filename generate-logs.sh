#!/usr/bin/env bash
#
STRING="Sep  6 00:14:21 Jons-MacBook-Pro syslogd[54]: ASL Sender Statistics"
LOGFILE=/tmp/uselesslog.log
while true
do
    echo ${STRING} >> ${LOGFILE}
    sleep 1;
done

#!/bin/bash

ROOT_DIR=/home/dungvc2/deploy/radius
LOG_FILE=$ROOT_DIR/logs/radius-streaming.log
NAME_APP=$1

cd $ROOT_DIR

export JAVA_HOME=/opt/jdk
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop

date=$(date +%y-%m-%d-%H:%M:%S)

echo "===> LOG $date ================" >> $LOG_FILE
echo "===> BEGIN CHECK ================" >> $LOG_FILE

resubmit() {
  state=$(/opt/hadoop/bin/yarn application -list -appStates RUNNING | grep "$1")
  if [ -z "$state" -a "$state" != " " ]; then
    /usr/bin/screen -S streaming-$1 -Q select . ;
    screen_state=$?
    if [ "$screen_state" == 0 ]; then
      return 2 # DELAY GET STATUS BUT APP STILL RUNNING
    else
      return 0
    fi
  else
    return 1 # SEE RUNNING ON WEB GUI
  fi
}


if [ ! -f "RUNNING_$NAME_APP" ]; then
    echo "===> Don't exist $ROOT_DIR/RUNNING_$NAME_APP file in here." >> $LOG_FILE
else
    flag=$(/opt/hadoop/bin/yarn application -list -appStates RUNNING | grep "_$NAME_APP")
    if [ -z "$flag" -a "$flag" != " " ]; then
       echo "===> $NAME_APP is not running on YARN." >> $LOG_FILE
       sleep 1m
       resubmit $NAME_APP
       status=$?
       #flag=$(/build/hadoop/bin/yarn application -list -appStates RUNNING | grep "$NAME_APP")
       #if [ -z "$flag" -a "$flag" != " " ]; then
       if [ "$status" == 0 ]; then
         echo "===> Remove $ROOT_DIR/RUNNING_$NAME_APP file." >> $LOG_FILE
         rm -f $ROOT_DIR/RUNNING_$NAME_APP
       else
         echo "===> Check again and $NAME_APP is running on YARN." >> $LOG_FILE
       fi
    else
       echo "===> $NAME_APP is running on YARN." >> $LOG_FILE
    fi
fi

echo "===> END CHECK ================" >> $LOG_FILE
echo "===> BEGIN RESUBMIT ================" >> $LOG_FILE

date=$(date +%y-%m-%d-%H:%M:%S)

echo "===> LOG $date " >> $LOG_FILE

if [ ! -f "RUNNING_$NAME_APP" ]; then
    echo "===> Resubmit $1 " >> $LOG_FILE
    /usr/bin/screen -dmS streaming-$NAME_APP $ROOT_DIR/submit-streaming.sh $NAME_APP
else
    #flag=$(/build/hadoop/bin/yarn application -list -appStates RUNNING | grep "$NAME_APP")
    #if [ -z "$flag" -a "$flag" != " " ]; then
    #   echo "===> $NAME_APP is not running on YARN." >> logs/resubmit-kafka.log
    #else
       echo "===> $NAME_APP is running on YARN." >> logs/resubmit-kafka.log
    #fi
fi

echo "===> END RESUBMIT ================" >> $LOG_FILE
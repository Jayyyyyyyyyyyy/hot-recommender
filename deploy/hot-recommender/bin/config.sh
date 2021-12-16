#!/bin/bash
# -*- coding:utf-8 -*-

#######################
# Template config
# DO NOT modify
PATH=".:$PATH" 
TBINDIR=$(cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
TBASEDIR=$(dirname $TBINDIR)
TBASENAME=$(basename $TBASEDIR)
hostip=`hostname -i|cut -d' ' -f2`
#######################
# programe name
# Replace s
PROG="$JAVA_HOME/bin/java"

PROGNAME="hotvideo"
PROGBASE=${PROGNAME%%.*}

#######################
# runtime options
memTotal=`free -g|sed -n 2p|awk '{print $2}'`
memHeap=$((memTotal*80/100))
OPTS="-Xmx${memHeap}g
-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=17068
-Dcom.sun.management.jmxremote.ssl=false
-Dcom.sun.management.jmxremote.authenticate=false
-Dcom.sun.management.jmxremote.port=9997
-Djava.rmi.server.hostname=$hostip
-XX:+PrintGCDetails
-Xloggc:../logs/gc.log
-XX:+PrintGCTimeStamps
-XX:+PrintGCDateStamps
-XX:+UseG1GC
-XX:MaxGCPauseMillis=150
-DSW_AGENT_NAME=hot-recommender
-DSW_AGENT_COLLECTOR_BACKEND_SERVICES=10.42.184.142:11800,10.42.26.219:11800,10.42.15.139:11800
-DSW_AGENT_SPAN_LIMIT=2000
-DSW_AGENT_SAMPLE=10
-javaagent:../skywalking/skywalking-agent.jar
-Dfile.encoding=UTF8
-jar hot-recommender-1.0-SNAPSHOT.jar --server.port=8080"

#######################
#
DAEMONIZE=yes

#######################
# pidfile absolute path
# Default: 
#	${TBASEDIR}/var/${PROGBASE}.pid
PIDFILE=${PIDFILE:-${TBASEDIR}/var/${TBASENAME}.pid}

#######################
# nohup stdout file
# set STDOUT=/dev/null if no need
# Default:
#	${TBASEDIR}/logs/${PROGBASE}.stdout.log
#STDOUT=${STDOUT:-${TBASEDIR}/logs/${PROGBASE}.stdout.log.`date +%Y%m%d%H%M%S`}
STDOUT=/dev/null
#######################
# ulimit -c 
# Default:
#	0
DAEMON_COREFILE_LIMIT=${DAEMON_COREFILE_LIMIT:-0}

TIMEWAIT=${TIMEWAIT:-10}

#######################
# use nohup to daemonize
# if PROG daemonized by itself, set START_COMMAND="${PROG} ${OPTS} > /dev/null 2>&1"
if [ ${DAEMONIZE} == 'yes' -o ${DAEMONIZE} == 'YES' -o ${DAEMONIZE} == 1 ];then 
  START_COMMAND="${TBINDIR}/daemonize.sh"
else
  START_COMMAND="${PROG} ${OPTS}"
fi

#######################
# monit config
MONIT_NAME="hotvideo"

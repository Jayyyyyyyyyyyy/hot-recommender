#!/bin/bash
#######################
# Template config
# DO NOT modify
PATH=".:$PATH"
TBINDIR=$(cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
TBASEDIR=$(dirname $TBINDIR)

source ${TBINDIR}/config.sh

nohup ${PROG} ${JAVA_OPTS} ${OPTS} >> ${STDOUT} 2>&1 &
SUBPID=$!

[ -n ${PIDFILE} -a ! -f ${PIDFILE} ] && echo $SUBPID > ${PIDFILE}

#!/bin/bash

PATH=".:$PATH"
TBINDIR=$(cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
TBASEDIR=$(dirname $TBINDIR)
source ${TBINDIR}/functions
source ${TBINDIR}/config.sh

####### main ######

echo -n "Stoping ${PROGNAME} ..."
if [ -n ${PIDFILE} -a -s ${PIDFILE} ]; then
   checkpid `cat ${PIDFILE}`
   RETVAL=$?
   if [ $RETVAL -ne 0 ];then
     echo -e "$PROGNAME is not running.\npidfile=\"$PIDFILE\"" 
     rm ${PIDFILE}
     exit 0
   fi
fi

KILLPROC_OPTS=
if [ -n ${PIDFILE} -a -s ${PIDFILE} ];then
  KILLPROC_OPTS="-p ${PIDFILE}" 
  killproc ${KILLPROC_OPTS}
  RETVAL=$?
  echo
else
  failure $"$base shutdown"
  echo
  echo -e "RETVAL\tno pidfile Found"
#  tmppidfile=$(mktemp /tmp/pidfile.XXXXXXXXXX)
#  ps -ef | grep "$PROG" | grep -v "grep" | grep "$OPTS" 
#  ps -ef | grep "$PROG" | grep -v "grep" | grep "$OPTS" | awk '{print $2}' >> ${tmppidfile}
#  if [ -s ${tmppidfile} ];then
#    killproc -p ${tmppidfile} && RETVAL=$?
#  else
#    failure $'shut dwon'
#    echo -e "RETVAL Not Existes!"
#  fi
#  test -f ${tmppidfile} && rm ${tmppidfile}
fi

exit $RETVAL

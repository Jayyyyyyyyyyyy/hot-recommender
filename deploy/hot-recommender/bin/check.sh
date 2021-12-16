#!/bin/bash
#######################
# Template config
# DO NOT modify
#liujikun
PATH=".:$PATH"
TBINDIR=$(cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
TBASEDIR=$(dirname $TBINDIR)

source ${TBINDIR}/functions
source ${TBINDIR}/config.sh

STATUS_OPTS=
[ -n ${PIDFILE} ] && STATUS_OPTS="${STATUS_OPTS} -p ${PIDFILE}"
status ${STATUS_OPTS} ${PROG}

RETVAL=$?
[ "$RETVAL" -eq 0 ] && success $"$base startup" || failure $"$base startup"
echo

exit $RETVAL

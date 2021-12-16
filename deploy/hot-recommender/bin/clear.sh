#!/bin/bash
######################
# TODO
# clear logs

PATH=".:$PATH"
TBINDIR=$(cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
TBASEDIR=$(dirname $TBINDIR)
source ${TBINDIR}/config.sh

###
# Warning:
#   DELELE files in logs-dir last-modified 7 days ago
find ${TBASEDIR}/logs/ -type f -mtime 7 | while read logfile tail
do
  echo "clearing $logfile..."
  rm ${logfile}
done

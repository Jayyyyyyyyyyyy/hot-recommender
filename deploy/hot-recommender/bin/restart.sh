#!/bin/bash
#######################
# Template config
# DO NOT modify
PATH=".:$PATH"
TBINDIR=$(cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
TBASEDIR=$(dirname $TBINDIR)

$TBINDIR/stop.sh
$TBINDIR/start.sh

#!/bin/bash

echo $$ $PPID
echo $$ > ../var/package_template.pid
while :
do
    sleep 1
done

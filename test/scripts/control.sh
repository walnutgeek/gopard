#!/bin/bash
P=`pwd`
echo print: Hello $P!!!
sleep 2
cat << EOM
exec:`dirname $0`/ls.sh s x
print: Good morning!!
EOM
sleep 1
echo print: I am done
sleep 10
echo print: Bye


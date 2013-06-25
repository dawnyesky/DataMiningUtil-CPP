#!/bin/sh

export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:./lib

rm -f ./log/*.log && rm -f ./log/*.log.*

./debug/yskdmu

#rm -f ./log/*.log

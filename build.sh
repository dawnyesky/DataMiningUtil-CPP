#! /bin/sh

export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:./lib
cd ./debug && make clean && make

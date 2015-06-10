#!/bin/bash

trap "exit" INT
for file in data/*.in
do
    base=${file%.*}
    echo $base
    for i in {1..10}
    do
        echo $i; mpirun -np 8 par/gpm-par.exe $file out
        sort out > out2
        diff out2 ${base}.out
    done
done
rm out out2

#!/bin/bash
counter=1
for file in *.png; do
    #echo Renaming $file to ${counter}_market.png
    n=$(printf "%04d_market.png" $counter)
    #echo $counter + $n
    mv $file $n
    counter=$((counter+1))
done

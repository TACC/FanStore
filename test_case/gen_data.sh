#!/bin/bash

mkdir data &>/dev/null


for i in `seq 0 9`;
do
   mkdir data/${i} &>/dev/null
   
   for j in {a..z};
   do
       dd if=/dev/urandom of=data/${i}/${j}.rnd bs=1K count=16 &>/dev/null
   done
done    

cd data

find . > list
../../prep 12 list

grep rnd list | sed 's/\.\///g' > flist


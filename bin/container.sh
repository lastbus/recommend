#!/bin/bash
# 从 ResourceManager log 文件
# appId application id
# output file name

if [ $# -eq 0 ]; then
   echo "usage: shell -i appId -o outfile"
   exit -1
fi

while getopts "i:o:" arg
do
  case $arg in
        i)
           appId=$OPTARG
           ;;
        o)
           output=$OPTARG
           ;;
        ?)
           echo "unkown argument"
           exit -1
           ;;
    esac
done

echo "application id:  $appId"
echo "output file name: $output"

for i in 10 9 8 7 6 5 4 3 2 1 0
do
   if [ $i -eq 0 ]
   then
        grep $appId hadoop-cmf-yarn-RESOURCEMANAGER-master27.bl.bigdata.log.out >> $output
   else
        grep $appId hadoop-cmf-yarn-RESOURCEMANAGER-master27.bl.bigdata.log.out.$i >> $output
   fi
done

echo "## successed! ##"
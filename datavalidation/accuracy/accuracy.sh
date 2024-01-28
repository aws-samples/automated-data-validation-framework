#!/bin/bash
#script to check the Match and Mismatch records
set -e
set -x
path=/mnt/griffin
sudo su -l root -c "chmod -R 757 $path"
for i in `cat $path/accuracy/accuracy_input.txt`
do
  dbname=$(echo $i |cut -d "," -f1)
  tabname=$(echo $i |cut -d "," -f2)
  tgtdbname=$(echo $i |cut -d "," -f3)
  tgttabname=$(echo $i |cut -d "," -f4)
  #condition1=$(echo $i |cut -d "," -f5)
  bundlename=$(echo $i |cut -d "," -f5)
  env_code=$(echo $i |cut -d "," -f6)
  echo "RUNNING for srcDB Name: $dbname and srcTable Name:$tabname && tgtDB Name: $tgtdbname and tgtTable Name:$tgttabname"
        echo "$bundlename"

#        if [ "$bundlename" == "gss_raw" ];
#        then
#        echo "$bundlename picking"
#        #condition2=$(date '+%Y-%m-%d' -d "-2 day")
#         condition2=$(date --date="-60 days" +%s)
#        condition="$condition1>='$condition2'"
#
#        fi

	condition="1=1"
  echo "condition: $condition"

  hive --hiveconf hive.root.logger=OFF -S -e "USE $tgtdbname;describe $tgttabname" > $path/tabstruct.txt


       if [ $(grep -w Partition $path/tabstruct.txt)==" "  ]
       then
                echo    "it is not partition table"
        else
                echo "doing msck repair for $dbname.$tabname"
                hive -S -e "USE $dbname;msck repair table $tabname"


       fi

  cat $path/tabstruct.txt |grep -v  "#"|sort -u > $path/columnnames.txt

            if [ "$tabname" == "gss_raw" ];
            then
            echo "it is  parquet table"
            cat $path/columnnames.txt |grep -v "timestamp\|boolean\|binary\|map\|array" |awk -F " " '{ print $1 }' > $path/nblncn.txt
            else
            echo "it is not parquet table"
            cat $path/columnnames.txt |grep -v "boolean\|binary\|map\|array" |awk -F " " '{ print $1 }' > $path/nblncn.txt
            fi

  for cln in `cat $path/nblncn.txt`
  do
          echo "src.$cln = tgt.$cln AND" >> $path/clmnames.txt
  done

  cat $path/columnnames.txt |grep "boolean"|awk -F " " '{ print $1 }' > $path/blncn.txt

  for cln in `cat $path/blncn.txt`
  do
          echo "coalesce(src.$cln,false) = coalesce(tgt.$cln,false) AND" >> $path/clmnames.txt
  done

  cat $path/columnnames.txt |grep "binary"|awk -F " " '{ print $1 }' > $path/bincn.txt

  for cln in `cat $path/bincn.txt`
  do
          echo "coalesce(src.$cln,null) = coalesce(tgt.$cln,null) AND" >> $path/clmnames.txt
  done

  cat $path/columnnames.txt |grep "array"|awk -F " " '{ print $1 }' > $path/arrcn.txt

  for cln in `cat $path/arrcn.txt`
  do
          echo "coalesce(src.$cln[0]) = coalesce(tgt.$cln[0]) AND" >> $path/clmnames.txt
  done

  sed -ie '$s/ AND//' $path/clmnames.txt

  cat $path/clmnames.txt |tr '\n' ' ' > $path/clmnnames.txt

  clmn=`cat $path/clmnnames.txt`

  cp $path/default_accuracy_dq.json $path/dq_${tgtdbname}-${tgttabname}_accuracy_${tgttabname}.json

  sed -i "s/dynamicrule/${clmn}/" $path/dq_${tgtdbname}-${tgttabname}_accuracy_${tgttabname}.json
  sed -i "s/srcdb/${dbname}/" $path/dq_${tgtdbname}-${tgttabname}_accuracy_${tgttabname}.json
  sed -i "s/srctbl/${tabname}/" $path/dq_${tgtdbname}-${tgttabname}_accuracy_${tgttabname}.json
  sed -i "s/tgtdb/${tgtdbname}/" $path/dq_${tgtdbname}-${tgttabname}_accuracy_${tgttabname}.json
  sed -i "s/tgttbl/${tgttabname}/" $path/dq_${tgtdbname}-${tgttabname}_accuracy_${tgttabname}.json
  sed -i "s/condition/${condition}/" $path/dq_${tgtdbname}-${tgttabname}_accuracy_${tgttabname}.json
  sed -i "s/batch_accuracy/${tgtdbname}.${tgttabname}/" $path/dq_${tgtdbname}-${tgttabname}_accuracy_${tgttabname}.json

  rm $path/clmnnames.txt $path/columnnames.txt $path/clmnames.txte $path/clmnames.txt $path/tabstruct.txt $path/nblncn.txt $path/blncn.txt $path/bincn.txt $path/arrcn.txt

spark-submit --class org.apache.griffin.measure.Application --master yarn --deploy-mode client --queue default \
--driver-memory 2g --executor-memory 2g --num-executors 2 \
$path/griffin-measure.jar \
$path/env/env_accuracy.json $path/dq_${tgtdbname}-${tgttabname}_accuracy_${tgttabname}.json

current_time=$(date +'%m-%d-%Y-%H-%M-%S')

cd $path/persist/accuracy/${tgtdbname}.${tgttabname}
ls -t -1 --directory 1* | head -n 1 > $path/folder.txt
var=`cat $path/folder.txt`


sudo su -l root -c "aws s3 cp $path/persist/accuracy/${tgtdbname}.${tgttabname}/$var/METRICS.json $env_code/accuracy/${tgtdbname}.${tgttabname}/$current_time/"
sudo su -l root -c "aws s3 cp $path/persist/accuracy/${tgtdbname}.${tgttabname}/$var/__missRecords $env_code/mismatched_records/${tgtdbname}.${tgttabname}/$current_time/"

rm $path/folder.txt
rm $path/dq_${tgtdbname}-${tgttabname}_accuracy_${tgttabname}.json

done


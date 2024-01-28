#!/bin/bash
#script to check the Total Count
set -e
set -x
path=/mnt/griffin
sudo su -l root -c "chmod -R 757 $path"
for i in `cat $path/totalcount/totalcount_input.txt`
do
  srcdb=$(echo $i |cut -d "," -f1)
  srctab=$(echo $i |cut -d "," -f2)
  tgtdb=$(echo $i |cut -d "," -f3)
  tgttab=$(echo $i |cut -d "," -f4)
# condition1=$(echo $i |cut -d "," -f5)
  bundlename=$(echo $i |cut -d "," -f5)
  env_code=$(echo $i |cut -d "," -f6)
  echo "RUNNING for srcDB Name: $srcdb and srcTable Name:$srctab && tgtDB Name: $tgtdb and tgtTable Name:$tgttab"
        echo "$bundlename"

 #       if [ "$bundlename" == "gss_raw" ];
  #      then
   #     echo "$bundlename picking"
    #    condition2=$(date --date="-60 days" +%s)
     #   condition="$condition1>='$condition2'"

      #  fi
	 condition="1=1"
         echo "condition: $condition"

	hive --hiveconf hive.root.logger=OFF -S -e "USE $tgtdb;describe $tgttab" > $path/tabstruct1.txt


       if [ $(grep -w Partition $path/tabstruct1.txt)==" "  ]
       then
                echo    "it is not partition table"
        else
                echo "doing msck repair for $srcdb.$srctab"
                hive -S -e "USE $srcdb;msck repair table $srctab"


       fi

cp $path/default_count_dq.json $path/dq_${tgtdb}-${tgttab}_count_${tgttab}.json

sed -i "s/srcdb/${srcdb}/" $path/dq_${tgtdb}-${tgttab}_count_${tgttab}.json
sed -i "s/srctab/${srctab}/" $path/dq_${tgtdb}-${tgttab}_count_${tgttab}.json
sed -i "s/tgtdb/${tgtdb}/" $path/dq_${tgtdb}-${tgttab}_count_${tgttab}.json
sed -i "s/tgttab/${tgttab}/" $path/dq_${tgtdb}-${tgttab}_count_${tgttab}.json
sed -i "s/condition/${condition}/" $path/dq_${tgtdb}-${tgttab}_count_${tgttab}.json
sed -i "s/total_count_records/${tgtdb}.${tgttab}/" $path/dq_${tgtdb}-${tgttab}_count_${tgttab}.json

rm $path/tabstruct1.txt

spark-submit --class org.apache.griffin.measure.Application --master yarn --deploy-mode client --queue default \
--driver-memory 2g --executor-memory 2g --num-executors 2 \
$path/griffin-measure.jar \
$path/env/env_totalcount.json $path/dq_${tgtdb}-${tgttab}_count_${tgttab}.json

current_time=$(date +'%m-%d-%Y-%H-%M-%S')

cd $path/persist/count_accuracy/${tgtdb}.${tgttab}
ls -t -1 --directory 1* | head -n 1 > $path/folder1.txt
var1=`cat $path/folder1.txt`
echo $var1

sudo su -l root -c "aws s3 cp $path/persist/count_accuracy/${tgtdb}.${tgttab}/$var1/METRICS.json $env_code/total_count/${tgtdb}.${tgttab}/$current_time/"

rm $path/dq_${tgtdb}-${tgttab}_count_${tgttab}.json
rm $path/folder1.txt

done


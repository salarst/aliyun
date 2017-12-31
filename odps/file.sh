#!/bin/bash
tmpfile='tmp.txt'
preString=pangu://localcluster/product/aliyun/odps/
echo $preString
/apsara/deploy/pu ls -l $preString > $tmpfile
echo list pangu dir sortby file num
#get all projects but (meta/.backup/adsmr).
cat $tmpfile |tr "\n" " "  |sed 's#pangu://localcluster#\n#g' | awk '{print " pangu://localcluster" $1 "    " $2,$3,$4,$5,$6,$7,$8,$9,$10 }' | sort -k 7 -rn  | awk -F' ' '{print $1}'  > allProjects
sed -i '/meta\|adsmr\|\./d' allProjects
rm -rf allTables
#get all tables.
#
for i in `cat allProjects`;do
    /apsara/deploy/pu ls -l ${i}data > $tmpfile
    cat $tmpfile |tr "\n" " "  |sed 's#pangu://localcluster#\n#g' | awk '{print " pangu://localcluster" $1 " " $2,$3,$4,$5,$6,$7,$8,$9,$10 }' | sort -k 7 -rn   | awk '{if ($4/10000000<$7) print $1}'>> allTables
done
rm -rf allPartitions
#get all Partitions
for i in `cat allTables`;do
    /apsara/deploy/pu ls -l $i > $tmpfile
    cat $tmpfile |tr "\n" " "  |sed 's#pangu://localcluster#\n#g' | awk '{print " pangu://localcluster" $1 "    " $2,$3,$4,$5,$6,$7,$8,$9,$10 }' | sort -k 7 -rn  | grep '[[:digit:]]$'>> allPartitions
done
#files, need to merge
cat allPartitions | sort -k 7 -rn | grep '[[:digit:]]$' |awk '{print $1,$2,$3,$4,$5,$6,$7}' | egrep -v '0$|adsmr|\.' | awk '{print $1}' | grep pangu://localcluster/product/aliyun/odps/ > finally

rm -f $tmpfile

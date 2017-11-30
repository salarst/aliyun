#!/bin/bash
tmpfile='/home/admin/tmp.txt'
preString=pangu://localcluster/product/aliyun/odps/
access_id=a3Es0X662vmZuW9Y
access_key=kkIBst7vQDvt2lPW5a72bAJdnqMrrD
echo $preString
/apsara/deploy/pu ls -l $preString > $tmpfile
echo list pangu dir sortby file num
#sort by file num
cat $tmpfile |tr "\n" " "  |sed 's#pangu://localcluster#\n#g' | awk '{print " pangu://localcluster" $1 "    " $2,$3,$4,$5,$6,$7,$8,$9,$10 }' | sort -k 7 -rn  | head -n10 |awk -F' ' '{print $1}'  > abcdefg
rm -rf /tmp/mergefile
for i in `cat /tmp/abcdefg`;do
#project_name=`echo $i | awk -F'/' '{print $(NF-1)}'`
    /apsara/deploy/pu ls -l ${i}data > $tmpfile
    cat $tmpfile |tr "\n" " "  |sed 's#pangu://localcluster#\n#g' | awk '{print " pangu://localcluster" $1 "    " $2,$3,$4,$5,$6,$7,$8,$9,$10 }' | sort -k 7 -rn  | head -n20 | awk -F'    ' '{print $1}'>> mergefile
done
rm -rf mergePart
for i in `cat /tmp/mergefile`;do
    /apsara/deploy/pu ls -l $i > $tmpfile
    cat $tmpfile |tr "\n" " "  |sed 's#pangu://localcluster#\n#g' | awk '{print " pangu://localcluster" $1 "    " $2,$3,$4,$5,$6,$7,$8,$9,$10 }' | sort -k 7 -rn  | head -n30 | grep '[[:digit:]]$'>> mergePart
done

cat mergePart | sort -k 7 -rn | grep '[[:digit:]]$' |awk '{print $1,$2,$3,$4,$5,$6,$7}' | egrep -v '0$|adsmr|\.' | awk '{print $1}' | grep pangu://localcluster/product/aliyun/odps/ > finally
#sort by file length
#echo list pangu dir sortby file length 
#cat $tmpfile |tr "\n" " "  |sed 's#pangu://localcluster#\n#g' | awk '{print " pangu://localcluster" $1 "  " $2,$3,$4,$5,$6,$7,$8,$9,$10 }' | sort -k 4 -rn  | head -n3700

rm -f $tmpfile

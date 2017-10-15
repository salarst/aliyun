#!/bin/sh
#****************************************************************#
# ScriptName: batchSnapFormat.sh
# Author: salarst
# Create Date: 2017-07-24 18:58
# Modify Author: 
# Modify Date: 2017-07-25 09:20
# Function: 自动输出浙江省公安厅部门的批量快照格式
#***************************************************************#

#读取环境ecsdriver别名
. /etc/bashrc

#manage.yun.zj替换成实际的dtcenter域名，后面不变
#autoSanpShotPolicyId为dtcenter上自动快照ID
#departmentId为要做快照的一级部门ID
manageUrl=http://manage.yun.zj/manage/control/cloudSnapShot/use/applyPolicyEx?autoSnapshotPolicyId=sp-88gzv046u\&departmentId=2\&regionId=cn-hangzhou-zjga-d01

#获取未做快照的磁盘信息
#需要先建立一个diskIds的并在行首写入需要从什么时间开始做快照。格式为linux：date '+%F %T'
function getNewDisk() {
    if [ -e diskIds ];then
        lastTime=`sed -n 1p diskIds`
        echo `date "+%F %T"` > diskIds
        ecsdriver -e "select disk_id from ecs_device where status in ('In_use','Available') and ali_uid='your ali_uid' and gmt_created>'$lastTime'" >> diskIds
    else
        echo `date "+%F %T"` > diskIds
        ecsdriver -e "select disk_id from ecs_device where status in ('In_use','Available') and ali_uid='your ali_uid' >> diskIds
    fi
    sed -i 2d diskIds
}

#组合调用dtcenterApi的URL
function snapFormat() {
    if [ `cat diskIds | wc -l` == 1 ] ;then
        echo "无增加磁盘，不用挂载"
        exit 0
    fi
    for i in `cat diskIds | sed -n '1!p'`;do
        manageUrl=$manageUrl\&diskIds=$i
    done
    echo $manageUrl > autosnap
}

function main() {
    getNewDisk
    snapFormat
}

main

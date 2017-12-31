#!/home/tops/bin/python
#coding=utf-8
import os,sys,commands
import MySQLdb as mysql
import json
import logging

#通过odpscmd获取aliyun账号添加到ak里
def getAliyunId():
    logging.info('[INFO] run function getAliyunId')
    projectName = projectClass
    for i in projectName.keys():
        project_name=i
        if ak.has_key(project_name):
            continue
        os.system("sed -i 's#project_name=.*#project_name=odps_smoke_test#g' %s"%(odpscmd_conf))
        os.system("sed -i 's#access_id=.*#access_id=%s#g' %s"%(ak['odps_smoke_test']['access_id'],odpscmd_conf))
        os.system("sed -i 's#access_key=.*#access_key=%s#g' %s"%(ak['odps_smoke_test']['access_key'],odpscmd_conf))
        aliyunId = commands.getoutput('''%s "%s'%s'" 2>/dev/null | awk -F'$' '{print $2}' | awk '{print $1}' | grep -v ^$ '''%(ODPSCMD,sql,project_name))
        ak[project_name]={'aliyunId':aliyunId}
        ak[project_name]={'aliyunId':aliyunId}

#通过ak里的aliyun账号，到uim和ummak库当中获取ak
def getAk():
    logging.info('[INFO] run function getAK')
    uim = mysql.connect(host='xxx',port=3306,user=xxx,passwd=xxx,db=xxx)
    uimcur = uim.cursor()
    ummak = mysql.connect(host='xxx',port=3402,user=xxx,passwd=xxx,db=xxx)
    ummakcur = ummak.cursor()
    for k,v in ak.items():
        if k == 'odps_smoke_test' or v.has_key('access_key') :
            print('@DEBUG ak has already exists in ak.txt ')
            continue
        print('@DEBUG %s'%k)
        print(ak[k])
        aliyunId = v['aliyunId']
        uimcur.execute('select primarykey from aliyunuser where aliyunid="%s"'%(aliyunId))
        try:
            primarykey=uimcur.fetchone()[0]
        except:
            logging.error('[ERROR] can not get ak--%s'%k)
            continue
        ummakcur.execute('select access_id,access_key from accesskey_table where ower_id="%s" and expire_time=0'%(primarykey))
        result=ummakcur.fetchone()
        ak[k]['access_id']=result[0]
        ak[k]['access_key']=result[1]
    uimcur.close()
    uim.close()
    ummakcur.close()
    ummak.close()

#保存ak到文件，解决odpscmd执行慢的问题
def saveAK():
    logging.info('[INFO] run function saveAK')
    fd = open('ak.txt','wb+')
    json.dump(ak,fd)
    fd.close()

def getProject():
#example
#projectClass={'projectName':[['tableName','Part'],['tableName','Part']]}
    logging.info('[INFO] run function getProject')
    #rCode = 256
    #while rCode/256 == 1:
    #    print rCode
    #    rCode,temp = commands.getstatusoutput('bash file.sh &> /dev/null')
    os.system('bash file.sh &>/dev/null')
    fd = open('finally','r')
    for i in fd.readlines():
        tmp = []
        a = []
        projectInfo = i.split('/')
        projectName = projectInfo[6]
        table = projectInfo[8]
        part = projectInfo[9]
        a = part.split('=')
        try:
            part = "%s=\'%s\'"%(a[0],a[1])
        except Exception:
            part = 'noPartition'
        tmp.append(table)
        tmp.append(part)
        if projectClass.has_key(projectName):
            projectClass[projectName].append(tmp)
        else:
            projectClass[projectName] = [tmp]
    fd.close()

def merge():
    logging.info('[INFO] run function merge')
    #logging.basicConfig(level=logging.DEBUG,format='%(asctime)s %(message)s',datefmt='%x %X',filename='merge.log',filemode='a')
    odps_setting = 'set odps.merge.cross.paths=true; \
            set odps.merge.smallfile.filesize.threshold=64; \
            set odps.merge.maxmerged.filesize.threshold=1024; \
            set odps.merge.max.filenumber.per.instance=10000; \
            set odps.merge.max.filenumber.per.job=5000000; \
            set odps.task.merge.wait.for.fuxi.timeout=6000; \
            set odps.merge.max.partition.count=200;'
    for k,v in projectClass.items():
        access_id = ak[k]['access_id']
        access_key = ak[k]['access_key']
        for i in v:
            os.system("sed -i 's#project_name=.*#project_name=%s#g' %s"%(k,odpscmd_conf))
            os.system("sed -i 's#access_id=.*#access_id=%s#g' %s"%(access_id,odpscmd_conf))
            os.system("sed -i 's#access_key=.*#access_key=%s#g' %s"%(access_key,odpscmd_conf))
            if i[1] == 'noPartition':
                try:
                    logging.debug(commands.getoutput('%s "%s alter table %s merge smallfiles"'%(ODPSCMD,odps_setting,i[0])))
                except Exception:
                    logging.error('[ERROR] %s "alter table %s merge smallfiles"'%(ODPSCMD,i[0]))
            else:
                try:
                    logging.debug(commands.getoutput('''%s "%s alter table %s partition(%s) merge smallfiles"'''%(ODPSCMD,odps_setting,i[0],i[1])))
                except Exception:
                    logging.error('''[ERROR] %s "%s alter table %s partition(%s) merge smallfiles"'''%(ODPSCMD,odps_setting,i[0],i[1]))
    
if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG,format='%(asctime)s %(message)s',datefmt='%x %X',filename='merge.log',filemode='a')
    ODPSCMD = '/apsara/odps_tools/clt/bin/odpscmd -e'
    PU = '/apsara/deploy/pu ls -l'
    tmpfile = 'tmp.txt'
    preString = 'pangu://localcluster/product/aliyun/odps/'
    odpscmd_conf = '/apsara/odps_tools/clt/conf/odps_config.ini'
    sql='use meta;select distinct owner_name from m_project where project_name='
    projectClass = {}
    if os.path.exists('ak.txt'):
        fd = open('ak.txt','r')
        ak = json.load(fd)
        fd.close()
    else:
        ak = {'odps_smoke_test':{'aliyunId':99999999999999999999999,'access_id':xxxx,'access_key':xxxx}}
    getProject()
    getAliyunId()
    getAk()
    saveAK()
    merge()

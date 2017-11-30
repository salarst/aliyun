#!/home/tops/bin/python
#coding=utf-8
import os,sys,commands
import MySQLdb as mysql
import json
import logging

#通过odpscmd获取aliyun账号添加到ak里
def getAliyunId():
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
    uim = mysql.connect(host='uim.mysql.rds.yun.zj',port=3306,user='uim',passwd='v1xzEt9kbEliwdub',db='uim')
    uimcur = uim.cursor()
    ummak = mysql.connect(host='ummak.mysql.rds.yun.zj',port=3402,user='ummak',passwd='b5hgljnngCTu8wvc',db='ummak')
    ummakcur = ummak.cursor()
    for k,v in ak.items():
        if k == 'odps_smoke_test':
            continue
        aliyunId = v['aliyunId']
        uimcur.execute('select primarykey from aliyunuser where aliyunid="%s"'%(aliyunId))
        primarykey=uimcur.fetchone()[0]
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
    fd = open('ak.txt','wb+')
    json.dump(ak,fd)
    fd.close()

def getProject():
#example
#projectClass={'projectName':[['tableName','Part'],['tableName','Part']]}
#	os.system('sh file.sh')  //先执行file.sh生成finally文件
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
    logging.basicConfig(level=logging.DEBUG,format='%(asctime)s %(message)s',datefmt='%x %X',filename='merge.log',filemode='a')
    for k,v in projectClass.items():
        access_id = ak[k]['access_id']
        access_key = ak[k]['access_key']
        for i in v:
            os.system("sed -i 's#project_name=.*#project_name=%s#g' %s"%(k,odpscmd_conf))
            os.system("sed -i 's#access_id=.*#access_id=%s#g' %s"%(access_id,odpscmd_conf))
            os.system("sed -i 's#access_key=.*#access_key=%s#g' %s"%(access_key,odpscmd_conf))
            if i[1] == 'noPartition':
                try:
                    logging.debug(commands.getoutput('%s "alter table %s merge smallfiles"'%(ODPSCMD,i[0])))
                except Exception:
                    logging.error('[ERROR] %s "alter table %s merge smallfiles"'%(ODPSCMD,i[0]))
            else:
                try:
                    logging.debug(commands.getoutput('''%s "alter table %s partition(%s) merge smallfiles"'''%(ODPSCMD,i[0],i[1])))
                except Exception:
                    logging.error('''[ERROR] %s "alter table %s partition(%s) merge smallfiles"'''%(ODPSCMD,i[0],i[1]))
    
#    print tables
#partitions = '/tmp/partition'
#for i in tables:
#        if  "not a partitioned table" in commands.getoutput('%s "show partitions %s"'%(ODPSCMD,i[0])):
#		os.system('%s "alter table %s merge smallfiles"'%(ODPSCMD,i[0]))
#                continue
#        print('@DEBUG %s'%i[0])
        #os.system('''%s "show partitions %s" 2>/dev/null | grep -v '^$' > %s '''%(ODPSCMD,i,partitions))
        #tmp = os.popen('cat %s | cut -d"=" -f2'%partitions).read().split('\n')
        #for j in tmp:
        #    os.system('''sed -i "s#%s#\'%s\'#g" %s 2> /dev/null'''%(j,j,partitions))
	#for k in os.popen('cat %s'%partitions).read().split('\n'): 
#        print('''-------------%s "alter table %s partition(%s) merge smallfiles"'''%(ODPSCMD,i[0],i[1]))
#	os.system('''%s "alter table %s partition(%s) merge smallfiles"'''%(ODPSCMD,i[0],i[1]))
if __name__ == '__main__':
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
        ak = {'odps_smoke_test':{'aliyunId':99999999999999999999999,'access_id':'a3Es0X662vmZuW9Y','access_key':'kkIBst7vQDvt2lPW5a72bAJdnqMrrD'}}
    getProject()
    getAliyunId()
    getAk()
    saveAK()
    merge()

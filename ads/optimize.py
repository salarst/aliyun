#!/home/tops/bin/python
#coding:utf8
#****************************************************************#
# ScriptName: optimize.py
# Author: salarst
# Create Date: 2017-09-08 10:28
# Modify Author: 
# Modify Date: 2017-10-10 10:16
# Function: optimize ads tables
#***************************************************************#
#################################################################
#run the scrip on dmsag
#
#################################################################
import MySQLdb as mysql
import sys,os,datetime,time
def cleanlog():
    path = os.path.join(os.path.dirname(os.path.realpath(__file__)),'optimize.log')
    if os.path.exists(path):
        size = os.path.getsize(path)/1024/1024
        if size >= 100:
            os.remove(path)

#接受二元数据组Msg
def log(Msg):
   fd = open(os.path.join(os.path.dirname(os.path.realpath(__file__)),'optimize.log'),'ab+')
   Time = datetime.datetime.now().strftime("%Y-%m-%d %X")
   info = ' '.join(Msg[0])
   fd.write('\n==' + Time + ' '  + info + '\n')
   fd.close()

def errlog(e):
    fd = open(os.path.join(os.path.dirname(os.path.realpath(__file__)),'optimize_err.log'),'ab+')
    Time = datetime.datetime.now().strftime("%Y-%m-%d %X")
    fd.write('\n==' + Time + ' ' + e + '\n')
    fd.close()
#no used
def getTables(schema_name):
    db =  mysql.connect("your db connectors","your db_user","your db_pass","your db_name")
    cur = db.cursor()
    cur.execute("select table_name from tables where table_schema='%s' and update_type='realtime'"%(schema_name))
    tables = cur.fetchall()
    cur.close()
    db.close()
    return tables

#optimize table
def optimize():
    schemaInfo=getSchemaInfo()
    for schema_name,v in schemaInfo.items():
        #tables = getTables(schema_name)
        host = v['host']
        port = int(v['port'])
        user = v['access_id']
        password = v['access_key']
        schema_id = v['schema_id']
        db_name = schema_name
        db = mysql.connect(host=host,port=port,user=user,passwd=password,db=db_name)
        cur = db.cursor()
        cur.execute('select table_name,insert_record_count from information_schema.realtime_table_traffic_cross order by insert_record_count desc limit 20')
        tables = cur.fetchall()
        flag = 0
        for i in tables:
            count=i[1]
            if count == 0:
                break
            else:
                sql ="optimize table " + i[0]
                try:
                    cur.execute(sql)
                    result = cur.fetchall()
                    log(result)
                except mysql.Error as e:
                    errlog(e)
            if result[0][len(result[0])-1].endswith('ZK') and flag == 0:
                restartBufferNode(schema_id,schema_name)
                flag = 1
        cur.close()
        db.close()


#restart buffernode when the result of optimize action include 'ZK'
def restartBufferNode(id,schema):
    appname = schema + '_' + str(id) + '-BUFFERNODE-2'
    stop = 'sh group.sh stop ' + appname + ' group'
    start = 'sh group.sh start ' + appname + ' group'
    ads_tools_dir = '/home/admin/ads_tools/gallardo_cmd'
    os.popen('su - admin -c  "cd %s;echo y | %s" &>/dev/null' %(ads_tools_dir,stop))
    os.popen('su - admin -c  "cd %s;echo y | %s" &>/dev/null' %(ads_tools_dir,start))
    result = (('restart ' + appname,'sleep 180s'))
    log(result)
    time.sleep(180)
    

#get ads connectors/schema_id from ads_meta.schemata
#get the ak from db(ummak) with user_id
def getSchemaInfo():
    adsmeta =  mysql.connect("your db connectors","your db_user","your db_pass","your db_name")
    adscur =adsmeta.cursor()
    adscur.execute("select vip,port,creator_id,schema_name,id from schemata where schema_name in (select table_schema from db_resource WHERE resource_type not in ('c1','c2'))")
    result=adscur.fetchall()
    schemaInfo = {}
    for i in result:
        schemaInfo[i[3]]={'host':i[0],'port':i[1],'user_id':i[2].split('$')[1],'schema_id':i[4]}
    adscur.close()
    adsmeta.close()
    #print(json.dumps(b,indent=4)) 
    ummak = mysql.connect(host='ummak connectors',port='db port',user='your username',passwd='your passwd',db='ummak')
    ummakcur = ummak.cursor()
    for v in schemaInfo.values():
        timelist = []
        user_id=int(v['user_id'])
        ummakcur.execute("select create_time from accesskey_table where ower_id='%d'" %(user_id)) 
        result = ummakcur.fetchall()
        for i in result:
            timelist.append(i[0])
        AKTime = str(sorted(timelist)[0])
        ummakcur.execute("select access_id,access_key from accesskey_table where create_time='%s'"%(AKTime))
        ak = ummakcur.fetchall()
        v['access_id']=ak[0][0]
        v['access_key']=ak[0][1]
    ummakcur.close()
    ummak.close()
    return schemaInfo


if __name__ == '__main__':
    cleanlog()
    optimize()

#!/home/tops/bin/python
#****************************************************************#
# ScriptName: /tmp/getTaskName.py
# Author: salarst
# Create Date: 2017-09-18 22:52
# Modify Author: 
# Modify Date: 2017-09-23 15:33
# Function: list all cdp task on cdp nodes
#***************************************************************#
#################################################################
#run the scrip on dmsag
#require sshkey login
#################################################################
import json,time
import os,urllib2
def getTaskInfo():
    urllist = {}
    cdpnode=['cdp_node_ip']
    for i in cdpnode:
        tmp = os.popen('''ssh %s  ps -ef | grep taskexec | grep -v grep | awk '{print $2"-"$5"-"$16}' '''%i).read().split()
        urllist[i]=tmp
    return urllist

def getName():
    result = {}
    urllist = getTaskInfo()
    for k,i in urllist.items():
        task = {}
        for m in i:
            j = m.split('-')
            response = urllib2.urlopen(j[2]).read()
            tmp = json.loads(response)
            if tmp['job']['content'][0]['reader']['parameter'].has_key('project'): 
                srcproject=tmp['job']['content'][0]['reader']['parameter']['project']
                srctable=tmp['job']['content'][0]['reader']['parameter']['table']
            elif tmp['job']['content'][0]['reader']['parameter'].has_key('schema'):
                srcproject=tmp['job']['content'][0]['reader']['parameter']['schema']
                srctable=tmp['job']['content'][0]['reader']['parameter']['table']
            elif tmp['job']['content'][0]['reader']['parameter'].has_key('connection'):
                srcproject=tmp['job']['content'][0]['reader']['parameter']['connection'][0]['jdbcUrl'][0]
                srctable=tmp['job']['content'][0]['reader']['parameter']['connection'][0]['table'][0]
            else:
                continue
        

    
            if tmp['job']['content'][0]['writer']['parameter'].has_key('project'): 
                dstproject=tmp['job']['content'][0]['writer']['parameter']['project']
                dsttable=tmp['job']['content'][0]['writer']['parameter']['table']
            elif tmp['job']['content'][0]['writer']['parameter'].has_key('schema'):
                dstproject=tmp['job']['content'][0]['writer']['parameter']['schema']
                dsttable=tmp['job']['content'][0]['writer']['parameter']['table']
            elif tmp['job']['content'][0]['writer']['parameter'].has_key('connection'):
                dstproject=tmp['job']['content'][0]['writer']['parameter']['connection'][0]['jdbcUrl'][0]
                dsttable=tmp['job']['content'][0]['writer']['parameter']['connection'][0]['table'][0]
            speed = tmp['job']['setting']['speed']['byte']/1024/1024
            task[j[0]]={'startTime':j[1],'direction':srcproject + ':' + srctable + '-->' + dstproject + ':' + dsttable,'speed':str(speed) + 'M'}
        result[k]=task
    print json.dumps(result,indent=4)

if '__name__' == '__name__':
    getName()

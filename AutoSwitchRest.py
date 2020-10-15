#-*- coding:utf-8 -*-
import pymongo
import getopt
import sys
import simplejson as json
import subprocess
import argparse
import socket
import time
from pymongo.read_preferences import ReadPreference

class MongoCluster:

    # pymongo connection
    conn = None

    # connection string
    url = ""

    def __init__(self, url):
        self.url = url

    def connect(self):
        self.conn = pymongo.MongoClient(self.url,read_preference=ReadPreference.SECONDARY)

    def close(self):
        self.conn.close()


def run_cmd(cmd):
    '''
	    执行shell脚本
	'''
    p = subprocess.Popen(cmd,stdout = subprocess.PIPE,stderr=subprocess.PIPE, shell=True)
    out, err = p.communicate()
   
    return out, err



def port_status(ip,port):
    '''
	    判断端口状态
	'''
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.settimeout(2)
    result=server.connect_ex((ip,int(port)))
    return result
    

class MongoRest:
    def __init__(self,mongolists=None,username=None,password=None):
        self.mongolists= mongolists if mongolists else  ['10.249.218.7:27017', '10.249.218.22:27017','10.249.218.10:27017']
        self.username= username if username else 'xxx'
        self.password= password if password else 'xxxx'
        self.mongourl='mongodb://{}:{}@{}/?authSource=admin'.format(self.username,self.password,','.join(self.mongolists))
        
    ##副本集状态
    def replSetGetStatus(self):
        ##正常的副本集列表
        AcitveLists=[]
        ##异常的副本集列表
        FailedLists=[]
        for i in self.mongolists:
            ip = i.split(':')[0]
            port  = i.split(':')[1]
            #print('检测{}:{}是否存活'.format(ip,port))
            isclose = port_status(ip,port)        
            if not isclose:
                AcitveLists.append(i)
            else:
                FailedLists.append(i)
        ##最多可以删除的副本集个数
        delnum = len(self.mongolists)-int(len(self.mongolists)/2)
        print("最多宕机{}个节点集群可用".format(delnum-1))
        print("目前存活的节点为:{}".format(AcitveLists))
        ##异常的副本集列表
        print("目前失败的节点为:{}".format(FailedLists))
        ##
        self.replSetGetConfig(AcitveLists,FailedLists)

    def replSetGetConfig(self,AcitveLists,FailedLists):
        try:
            ##获取正常的副本集列表
            #Acitvelist=replSetGetStatus(mongoc)
            with open('/tmp/mongo.js','w') as f:
                c="""
                var cnf=rs.conf()
                var ac = %s
                var numbers = cnf.members

                for (var i = numbers.length - 1; i >= 0; i--) {
                    if(ac.indexOf(numbers[i].host)>=0){
                        numbers.splice(i, 1)
                    }

                }
                cnf.members=numbers
                rs.reconfig(cnf,{force:true})
                """%(FailedLists)
    
                f.write(c)
            cmd='mongo {}/admin -u {} -p {} /tmp/mongo.js'.format(AcitveLists[0],self.username,self.password)
            out,err = run_cmd(cmd)
            print(err)
        except Exception as err:
            print(err)
    
    def AddreplSetList(self):
        
        try:
            mongoc=MongoCluster(self.mongourl)
            mongoc.connect()
            #srcDbNamessrcDbNames = mongoc.conn.list_database_names()
            ##副本集状态
            replSetGetStatus=mongoc.conn.admin.command('replSetGetStatus')
            #副本集个数
            Shardmembers = replSetGetStatus['members']

            ##需要添加到集群的节点：
            #   MongoClusters=list(map(lambda x: x+':'+str(mongoport),mongolists))
            #addList=list(set(MongoClusters) ^ set([r['name'] for r in  Shardmembers]))
            print([r['name'] for r in  Shardmembers])
            addList=list(set(self.mongolists) ^ set([r['name'] for r in  Shardmembers]))
            print('需要添加到集群的节点：{}'.format(addList))


            ##检测失败节点，端口是否存活
            for lists in addList:
                ip = lists.split(':')[0]
                port  = lists.split(':')[1]
                #print('检测{}:{}是否存活'.format(ip,port))
                isclose = port_status(ip,port)
                ##获取主节点IP
                masterIp = mongoc.conn.admin.command('isMaster')['primary']
                print("集群主节点IP：{}".format(masterIp))
                ##服务正常添加到集群
                if not isclose:
                    cmd='mongo {}/admin -u {} -p {} --eval "rs.add(\'{}\')"'.format(masterIp,self.username,self.password,lists)
                    out,err = run_cmd(cmd)
            
                    if err:
                        print(err.decode('utf8'))
                    else:
                        print(out)
                        print('添加节点{}到集群'.format(lists))
                else:
                    print('{}节点异常'.format(lists))
        
        except Exception as e:
            print(e)
            print("create mongo connection failed %s" % (self.mongourl))
            exit()
        
    
    
if __name__=='__main__':
    rst=MongoRest()
    ##集群异常删除失败节点
    rst.replSetGetStatus()
    time.sleep(5)
    ##增加节点
    rst.AddreplSetList()


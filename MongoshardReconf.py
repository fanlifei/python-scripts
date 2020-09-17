import pymongo
import getopt
import sys
import json

def usage():

	print(
	'''        |-----------------------------------------------------------------------------------------------------------------------------------------------|
        | Usage : ./checkmongo.py --host="localhost" --username=xxx  --port=1000  --password="aaaa"  |')
        |-----------------------------------------------------------------------------------------------------------------------------------------------|
    ''')
	exit(0)


class MongoCluster:

    # pymongo connection
    conn = None

    # connection string
    url = ""

    def __init__(self, url):
        self.url = url

    def connect(self):
        self.conn = pymongo.MongoClient(self.url)

    def close(self):
        self.conn.close()



def replSetGetConfig(mongoc):
	memlist=replSetGetStatus(mongoc)

	cnf=mongoc.conn.admin.command({"replSetGetConfig":1})
	cnf['config']['members'] = [ cnf['config']['members'][id['memberid']] for id in memlist]
	cnf['config']['version']+=1
	recnf=mongoc.conn.admin.command('replSetReconfig',cnf['config'],force=True)
	print(recnf)


def replSetGetStatus(mongoc):
	srcDbNames = mongoc.conn.list_database_names()
	replSetGetStatus=mongoc.conn.admin.command('replSetGetStatus')
	memberslen = len(replSetGetStatus['members'])
	delnum = memberslen-int(memberslen/2)
	
	print("最多删除{}个节点,才能保证集群可用".format(memberslen-int(memberslen/2)-1))
	healthlist=[r['name'] for r in  replSetGetStatus['members'] if  r['health']==1.0]
	print("目前存活的节点为:{}".format(healthlist))
	nohealthlist=[r['name'] for r in  replSetGetStatus['members'] if  r['health']==0.0]
	print("目前失败的节点为:{}".format(nohealthlist))
	res=[]
	for index,r in enumerate(replSetGetStatus['members']):
		if r['name'] not in nohealthlist[:delnum]:
			_dict=dict()
			_dict['memberid'] = index
		#_dict['id']=r['_id']
			_dict['name'] = r['name']
		#_dict['health'] = r['health']
		#_dict['stateStr'] = r['stateStr']
			res.append(_dict)

	return res

if __name__=='__main__':
    opts, args = getopt.getopt(sys.argv[1:], "Hh:u:p:P:", ["help", "host=", "username =", "password=", "port="])
    host,username,password,port,delete='127.0.0.1','','',27017,0
    for key, value in opts:
        if key in ("-H", "--help"):
            usage()
        if key in ("-h", "--host"):
            host = value
        if key in ("-u", "--username"):
            username = value
        if key in ("-p", "--password"):
            password = value
        if key in ("-P", "--port"):
            port = value
    # params verify
    if len(host)==0 or len(username)==0:
    	usage()
    try:
    	mongourl='mongodb://{}:{}@{}:{}/?authSource=admin'.format(username,password,host,port)
    	mongoc=MongoCluster(mongourl)
    	mongoc.connect()
    except Exception as e:
        print("create mongo connection failed %s" % (mongourl))
        exit()
    
    #aa=replSetGetStatus(mongoc)
    #print('ccc',aa)
    replSetGetConfig(mongoc)



    mongoc.close()

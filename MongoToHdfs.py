#-*- coding:utf-8 -*-
#---------------------------------------
#   程序：获取Mongo数据压缩并传到hdfs
#   版本：0.1
#   作者：fanlifei
#   日期：2018-05-23
#   语言：Python 2.7
#   注意: pyhdfs 上传hdfs需要在本地hosts文件添加 hdfs datanode ip 
#---------------------------------------
import pymongo
from bson.objectid import ObjectId
from datetime import datetime,timedelta
import pprint
import os
import subprocess
import dateutil
import gzip
import sys
import time
import shutil
import pyhdfs
import logging
from pymongo import MongoClient
from bson import Code
reload(sys)
sys.setdefaultencoding('utf-8')
def logger(log_message):
	log_file=os.path.join('/tmp/',os.path.splitext(sys.argv[0])[0]+'.log')
	logger = logging.getLogger(os.path.splitext(sys.argv[0])[0])
	logger.setLevel(logging.INFO)
	handler = logging.FileHandler(log_file)
	fmt = '%(asctime)s - %(filename)s:%(lineno)s - %(name)s - %(message)s' 
	formatter = logging.Formatter(fmt)
	handler.setFormatter(formatter)
	logger.addHandler(handler) 
	logger.info(log_message)  
	
	
def conn_hdfs(conn_hdfs):
	fs = pyhdfs.HdfsClient(**conn_hdfs)
	return fs
	
	
def local_to_hdfs(hdfs_info,local_file,dest,**kwargs):
	print local_file,dest	
	fs = conn_hdfs(hdfs_info)
	if not fs.exists(dest):
		fs.mkdir(dest)
	hdfs_file = os.path.join(dest,os.path.split(local_file)[-1])
	if fs.exists(hdfs_file):
		fs.delete(hdfs_file,recursive='false')
	fs.copy_from_local(local_file,hdfs_file)
	if os.path.exists(local_file):
		os.remove(local_file)
#	if os.path.exists(os.path.splitext(local_file)[0]+'.csv'):
#		os.remove(os.path.splitext(local_file)[0]+'.csv')
			
	
		
def run_cmd(cmd):
	'''
	执行shell脚本
	'''
	p = subprocess.Popen(cmd,stdout = subprocess.PIPE,stderr=subprocess.PIPE, shell=True)
	out, err = p.communicate()
	return out, err
def get_filesize(filename):
	'''
	判断文件大小
	'''
	try:
		size = os.path.getsize(filename)
		return size
	except Exception as err:
		print(err)
def filename(pfefix,dirname):	
	'''
	生成文件名
	'''
	#today = datetime.today()
	#formats = '%Y%m%d'
	#filename = 'ott_final_'+today.strftime(formats)+'.csv'
	filename = str(pfefix)+'.csv'
	full_filename = os.path.join(dirname,filename)
	#full_filename = os.path.join('/tmp/',filename)
	return full_filename
def copyfile(filename):
	'''
	scp 拷贝文件
	'''
	dest_host='10.9.133.239'
	dest_path='/data/mongodb'
	cmd = "scp {0} {1}:{2}".format(filename,dest_host,dest_path)
	out,err = run_cmd(cmd)
	
	
 
def writefile(full_filename,result):
	'''
	获取Mongo 数据写入文件
	'''
	#full_filename = filename()
	try:
		with open(full_filename,'at') as f:
			f.write(result)
	except:
		raise SystemExit(1)
def get_keys(db, collection):
    client = MongoClient()
    db = client[db]
    map = Code("function() { for (var key in this) { emit(key, null); } }")
    reduce = Code("function(key, stuff) { return null; }")
    result = db[collection].map_reduce(map, reduce, "myresults")
    return result.distinct('_id')
	
def conn_mongo(mongo_url):
	client = pymongo.MongoClient(mongo_url)
	return client
	
	
##mongo查询
def query_mongo(conn,database,coll,queryArgs=None):
	logger('begin conn Mongo')
	db=conn[database]
	db_coll = db[coll]
	
	#map = Code("function() { for (var key in this) { emit(key, null); } }")
	#reduce = Code("function(key, stuff) { return null; }")
	#result = db_coll.map_reduce(map, reduce, "myresults")
	#projectionFields=result.distinct('_id')[1:]
	projectionFields = ['noticeid','预约日期','身份证明号码','学员姓名','requestid','ykrqend','date','考试车型','ykrqstart','kskm','add_time','index','kscx','考试科目','add_date','学习驾驶证明编号','考试场次','sid','约考日期','type','考试场地','考试日期','考试成绩']
	if queryArgs ==None:
		search_res = db_coll.find(projection = projectionFields)
	else:
		search_res = db_coll.find(queryArgs,projection = projectionFields)
	file_mess=''	
	for record in search_res:
		fileds=''
		for x in range(len(projectionFields)):
			filed = projectionFields[x].decode('utf-8')
			if record.has_key(filed):
				if type(record[filed]) == ObjectId:
					record[filed] = str(record[filed]) 
				fileds += str(record[filed]).decode('utf-8') + '\t'	
			else:
				fileds += '' + '\t'
		yield  fileds+'\n'
	
	#with open(full_filename, 'rt') as f_in, gzip.open(os.path.splitext(full_filename)[0]+'.gz', 'wt') as f_out:
	#	shutil.copyfileobj(f_in, f_out)
def main():
	mongo_url='10.9.93.137:27017'
	mongo_database='ott'
	mongo_coll='ott_final'
	local_dir='/tmp'
	hdfs_dir='/data/mongodb/'
	#连接mongo
	conn = conn_mongo(mongo_url)
	#需要传输的文件名
	full_filename=filename(mongo_coll,local_dir)
	#today = datetime.today()
	#yesterday = today-timedelta(days=1)
	#start_time = datetime(yesterday.year,yesterday.month,yesterday.day,0,0,0)
	#end_time = datetime(today.year, today.month, today.day, 0, 0, 0)
	#queryArgs = {"add_date":{"$gte":start_time,"$lt":end_time}}
	##mongo返回数据
	results=query_mongo(conn,mongo_database,mongo_coll)
	for result in results:
		#写入文件
		writefile(full_filename,result)
	#判断文件大小
	if get_filesize(full_filename):	
		conn_hdfs={'hosts':'10.9.28.44:50070,10.9.45.20:50070','max_tries':10,'timeout':10,'user_name':'hadoop'}
		local_to_hdfs(conn_hdfs,full_filename,hdfs_dir)
	else:
		 print("%s is empaty!" % full_filename)
	
	
if __name__=='__main__':
	main()

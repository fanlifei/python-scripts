#!/usr/bin/env python3
#-*- coding:utf-8 -*-

###目的:下载阿里云rds数据库到本地存储
###创建时间:2018年7月20日
###创建人:范立飞
###联系方式:fannnlf@gmail.com
###

###pip install aliyun-python-sdk-core-v3 # 安装阿里云SDK核心库
###pip install aliyun-python-sdk-ecs # 安装管理ECS的库
###pip install aliyun-python-sdk-rds # 安装管理RDS的库

try:
    from aliyunsdkcore import client
except Exception as e:
    print(e)
try:
    from aliyunsdkrds.request.v20140815 import DescribeBackupsRequest,DescribeBinlogFilesRequest
except Exception as e:
    print(e)

import json
import requests
import datetime
from aliyunsdkcore.profile import region_provider
import logging
import time
import os
import re





##记录日志
def logg(logpath='.'):
    # 第一步，创建一个logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)  # Log等级总开关
    # 第二步，创建一个handler，用于写入日志文件
    logfile = os.path.join(logpath,'rds.log')
    fh = logging.FileHandler(logfile, mode='a')
    fh.setLevel(logging.DEBUG)
    # 第三步，定义handler的输出格式
    formatter = logging.Formatter("%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s")
    fh.setFormatter(formatter)
    # 第四步，将logger添加到handler里面
    if not logger.handlers:
        logger.addHandler(fh)
    #logger.removeHandler(fh)
    return logger

##删除过期日志
def clean(day=7, defaultpath=''):
    ##获取指定目录文件
    for eachfile in os.listdir(defaultpath):
        ##生成文件名
        filename = os.path.join(defaultpath,eachfile)
        ##获取文件的修改时间
        ltime= int(os.stat(filename).st_mtime)
        ##获取删除时间
        beftime = int(time.time())-24*3600*day
        if ltime<beftime:
            try:
                ##判断是否为文件
                if os.path.isfile(filename):
                    os.remove(filename)
                    logger.info("History files %s have been deleted" % filename)
                    print(filename)

                elif os.path.isidr(filename):
                    os.removedirs(filename)
                    logger.info("History files %s have been deleted" % filename)
                    print(filename)
                else:
                    os.remove(filename)
                    logger.info("History files %s have been deleted" % filename)
                    print(filename)

            except Exception as err:
                print(error)
                print("%s remove faild." % filename)
                logger.error( "%s remove faild." % filename)
# 下载链接
def downLink(url,filename):
    if url is None:
        print('url is None')
    else:
        downFile = requests.get(url)
        with open(filename,'wb') as backup_file:
           backup_file.write(downFile.content)

#获取备份日期
def getdate():
    _ISO8601_DATE_FORMAT = "%Y-%m-%dT00:00Z"
    today=datetime.date.today().strftime(_ISO8601_DATE_FORMAT)
    yesterday=(datetime.date.today() - datetime.timedelta(1)).strftime(_ISO8601_DATE_FORMAT)
    return today,yesterday

##获取binlog 备份日期
def binlogback_getdate():
    _ISO8601_DATE_FORMAT = "%Y-%m-%dT00:00:00Z"
    today=datetime.date.today().strftime(_ISO8601_DATE_FORMAT)
    yesterday=(datetime.date.today() - datetime.timedelta(1)).strftime(_ISO8601_DATE_FORMAT)
    return today,yesterday


###下载数据备份文件
def downfullbackupfile(DBInstanceId,dbname):
    logger = logg(backpath)
    clt = client.AcsClient('','','cn-beijing') #这里的地区ID非必须的
    request = DescribeBackupsRequest.DescribeBackupsRequest()
    today,yesterday = getdate()
    ## 以下请求的参数都是必须的 尤其实例名和查询区间
    request.set_accept_format('json')
    request.set_action_name('DescribeBackups')
    request.set_DBInstanceId(DBInstanceId) # 你的实例ID
    request.set_StartTime(yesterday)
    request.set_EndTime(today)

    try:
        result = clt.do_action_with_exception(request)
        if result:
            json_rds = json.loads(result)
            lists = json_rds['Items']['Backup']
            if len(lists)==0:
                logger.error(u"%s %s 数据库没有备份文件"%(yesterday,dbname))
            for l in lists:
                BackupDownloadURL=l['BackupDownloadURL']
                BackupStatus=l['BackupStatus']
                BackupSize=l['BackupSize']
                BackupType=l['BackupType']
                DBInstanceId = l['DBInstanceId']
                BackupEndTime = l['BackupEndTime']
                BackupStartTime = l['BackupStartTime']
                BackupStartTime = datetime.datetime.strptime(BackupStartTime, "%Y-%m-%dT%H:%M:%SZ").strftime("%Y%m%d%H%M%S")
                BackupEndTime = datetime.datetime.strptime(BackupEndTime, "%Y-%m-%dT%H:%M:%SZ").strftime("%Y%m%d%H%M%S")
                filename = dbname+'_'+DBInstanceId+'_'+BackupStartTime+'_'+BackupEndTime+'_'+BackupType
                #print(json_rds)
                if BackupStatus=='Success':
                    filename = '%s.tar.gz'%(filename)
                    backfullpath=os.path.join(backpath,filename)
                    downLink(BackupDownloadURL,backfullpath)
                    print(u"%s%s 数据库备份完成"%(yesterday,dbname))
                    logger.info(u"%s %s 数据库备份完成,文件大小%sM"%(yesterday,dbname,round(BackupSize/1024/1024,2)))
                else:
                    print(u"%s%s 数据库备份失败"%(yesterday,dbname))
                    logger.error(u"%s %s 数据库备份失败"%(yesterday,dbname))

    except Exception as e:
        logger.error(e)
        print(e)

##下载binlog 文件


def downbinlogfile(DBInstanceId,dbname):
    logger = logg(backpath)
    clt = client.AcsClient('','','cn-beijing') #这里的地区ID非必须的
    request = DescribeBinlogFilesRequest.DescribeBinlogFilesRequest()
    today,yesterday = binlogback_getdate()
    request.set_accept_format('json')
    request.set_action_name('DescribeBinlogFiles')
    request.set_DBInstanceId(DBInstanceId) # 你的实例ID
    request.set_StartTime(yesterday)
    request.set_EndTime(today)
    try:
        result = clt.do_action_with_exception(request)
        if result:
            json_rds = json.loads(result)
            lists = json_rds['Items']['BinLogFile']
            if len(lists)==0:
                logger.error(u"%s %s 数据库没有binlog备份文件"%(yesterday,dbname))
            for l in lists:
                DownloadLink=l['DownloadLink']
                HostInstanceID=l['HostInstanceID']
                LogBeginTime=l['LogBeginTime']
                LogEndTime=l['LogEndTime']
                FileSize=l['FileSize']
                ##正则匹配binlog文件
                pat='/hostins%s/(.*)\?OSSAccessKeyId'%(str(HostInstanceID))
                binfile=re.findall(pat,DownloadLink)
                filename=''.join(binfile)
                filename=os.path.join(backpath,'binlog',dbname,filename)
                if not os.path.exists(os.path.dirname(filename)):
                    os.makedirs(os.path.dirname(filename))
                downLink(DownloadLink,filename)
                print(u"%s%s binlog备份完成"%(yesterday,dbname))
                logger.info(u"%s  %s binlog备份完成,文件大小%sM"%(yesterday,filename,round(FileSize/1024/1024,2)))
    except Exception as e:
        logger.error(e)
        print(e)                
        
        

##主函数
def main():
    global backpath
    backpath='/data/backup/rds/'
    logfile=os.path.join(backpath,'rds.log')
    DBDIC=[
        {'DBInstanceId':'rm-2zevtk2agv1d1e6in','dbname':'新mdm'},
        {'DBInstanceId':'rm-2zea0im1wyh2r2m43','dbname':'催收master'}
    ]
    for dbs in DBDIC:
        #备份数据库
       downfullbackupfile(**dbs)
       #备份binlog
       downbinlogfile(**dbs)
    ##删除过期备份
    clean(defaultpath=backpath)
    ##获取
    clean(defaultpath=backpath)


if __name__=='__main__':
    main()



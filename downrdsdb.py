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
    from aliyunsdkrds.request.v20140815 import DescribeBackupsRequest
except Exception as e:
    print(e)

import json
import requests
import datetime
from aliyunsdkcore.profile import region_provider

# 下载链接
def downLink(url,filename):
    saveRoot='/data/backup/rds/' #设置备份根目录
    downFile = requests.get(url)
    # 设置备份目录
    savePath = '%s%s.tar.gz'%(saveRoot,filename)
    with open(savePath,'wb') as backup_file:
       backup_file.write(downFile.content)

def getdate():
    _ISO8601_DATE_FORMAT = "%Y-%m-%dT00:00Z"
    today=datetime.date.today().strftime(_ISO8601_DATE_FORMAT)
    yesterday=(datetime.date.today() - datetime.timedelta(1)).strftime(_ISO8601_DATE_FORMAT)
    return today,yesterday

#region_provider.modify_point('Rds', 'cn-beijing', 'rds.aliyuncs.com')
#region_provider.user_config_endpoints

def downfullbackupfile(DBInstanceId,dbname):
    clt = client.AcsClient('LTAIdXvY80BT2B0S','7Taui0vs9rGTqc45gP5x1wqyooEjgX','cn-beijing') #这里的地区ID非必须的
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
            for l in lists:
                BackupDownloadURL=l['BackupDownloadURL']
                BackupStatus=l['BackupStatus']
                BackupType=l['BackupType']
                DBInstanceId = l['DBInstanceId']
                BackupEndTime = l['BackupEndTime']
                BackupStartTime = l['BackupStartTime']
                BackupStartTime = datetime.datetime.strptime(BackupStartTime, "%Y-%m-%dT%H:%M:%SZ").strftime("%Y%m%d%H%M%S")
                BackupEndTime = datetime.datetime.strptime(BackupEndTime, "%Y-%m-%dT%H:%M:%SZ").strftime("%Y%m%d%H%M%S")
                filename = dbname+'_'+DBInstanceId+'_'+BackupStartTime+'_'+BackupStartTime+'_'+BackupType
                #print(json_rds)
                if BackupStatus=='Success':
                    downLink(BackupDownloadURL,filename)

        print(u"%s 数据库备份完成"%(dbname))
    except Exception as e:
        print(e.get_http_status())
        print(e.get_error_code())
        print(e.get_error_msg())
def main():
    DBDIC=[
        {'DBInstanceId':'rm-2zex2l38184h3485b','dbname':'订单系统'},
        {'DBInstanceId':'rm-2zez49dhs9h4se1bu','dbname':'用户中心usercenter'},
        {'DBInstanceId':'rm-2zea0im1wyh2r2m43','dbname':'催收master'},
        {'DBInstanceId':'rm-2zev5ms2yu860e292','dbname':'hshc-log'},
        {'DBInstanceId':'rm-2zed7m4gs6qz090g6','dbname':'优惠券'},
        {'DBInstanceId':'rm-2zex2xy15vq04832c','dbname':'支付中心'},
        {'DBInstanceId':'rm-2ze18n7pyi1j900oh','dbname':'结算master'},
        {'DBInstanceId':'rm-2zeyjt47i3rw7t7g1','dbname':'花生代理人'},
        {'DBInstanceId':'rm-2ze16067kx75zkn5l','dbname':'风控'},
        {'DBInstanceId':'rm-2zevtk2agv1d1e6in','dbname':'新mdm'},
        {'DBInstanceId':'rm-2zehh217415y797mt','dbname':'客户关系系统'},
        {'DBInstanceId':'rm-2zenl3sn56ij5u7kn','dbname':'花生好车APP'},
    ]
    for dbs in DBDIC:
       downfullbackupfile(**dbs)

if __name__=='__main__':
    main()

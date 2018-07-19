#!/usr/bin/env python
# encoding: utf-8
'''
读取收件箱里包含jira主题的邮件，并获取issue_id
根据issue_id来返回相应的信息

'''

from jira import JIRA
import sys
import os
import json
import re
import email,imaplib
reload(sys)
sys.setdefaultencoding('utf8')


class jbase():
     ##初始化jira用户密码
     def __init__(self,issue_id):
         self.server='http://jira.hche.work/'
         self.basic_auth=('fanlifei','Cw31')
         self.issue=issue_id

     def fmt(self,summary,msg):
         if summary ==None:
             summary=''
         if msg==None:
             msg=''
         print('\033[36m {}{:#>10}\n{:>3}{} \033[0m').format(summary,'','',msg)
         #print('{}{:#>10}\n{:>3}{}').format(summary,'','',msg)

     #链接jira
     def conn(self):
         jira=JIRA(server=self.server,basic_auth=self.basic_auth)
         return jira
         
         
     #issue
     def _issue(self):
         jira = self.conn()
         issues=jira.issue(self.issue)
         return issues

     def key(self):
         key = self._issue().key
         self.fmt('key',key)
     #解决方案
     def resolution(self):
         res =self._issue().fields.resolution
         desc =''
         name=''
         if res is not None:
             desc =self._issue().fields.resolution.description

             name = self._issue().fields.resolution.name

         resole={'解决方案':desc,'解决结果':name}
         dicts=json.dumps(resole, encoding="UTF-8", ensure_ascii=False, sort_keys=False, indent=4)
         self.fmt('解决方案',dicts)

         #print self._issue().fields.issuetype


     ##模块
     def components(self):
         for component in self._issue().fields.components:
             self.fmt('模块',component.name)

     ##描述
     def description(self):
         desc=self._issue().fields.description
         self.fmt('描述',desc)

     ##附件
     def attachment(self):
         for attachment in self._issue().fields.attachment:
             att=("Name: '{filename}', size: {size}".format(
                 filename=attachment.filename.encode('utf-8'), size=attachment.size))
             if os.path.splitext(attachment.filename.encode('utf-8'))[1]=='.sql':
                 content=("Content: '{}'".format(attachment.get()))
                 self.fmt('附件',att)
                 self.fmt('附件',content)

     ##备注
     def comment(self):
         for comment in self._issue().raw['fields']['comment']['comments']:
             dicts={}
             dicts={'姓名':comment['author']['displayName'] ,'内容':comment['body'],'创建时间':comment['created'],'更新时间':comment['updated']}
             #c.append(dicts)
             dicts=json.dumps(dicts, encoding="UTF-8", ensure_ascii=False, sort_keys=False, indent=4)
             self.fmt('备注',dicts)
     ##摘要
     def summary(self):
         summary=self._issue().fields.summary
         self.fmt('摘要',summary.encode('utf-8'))
         
##获取未读邮件主题
def getMail(host, username, password, port=993):
      try:
          serv = imaplib.IMAP4_SSL(host, port)
      except Exception, e:
          print e.message
      serv.login(username, password)
      serv.select()
      # 搜索邮件内容
      typ, data = serv.search('UNSEEN','ALL')
      sub=[]
      for num in data[0].split()[::-1]:
          typ, data = serv.fetch(num, '(RFC822)')
          text = data[0][1].decode('utf-8')
          message = email.message_from_string(text)   # 转换为email.message对象
          subject = message.get('subject')
          h = email.Header.Header(subject)
          dh = email.Header.decode_header(h)
          subject = unicode(dh[0][0], dh[0][1]).encode('utf-8')
          sub.append(subject)
          #邮件设置为已读
          serv.store(num,'+FLAGS','\Seen')
      serv.close()
      serv.logout()
      return sub
  
  
def main():
     host = "imap.exmail.qq.com"
     username = "lifeifan@h.com"
     password = "123"
     subs=getMail(host, username, password)
     for sub in list(set(subs)):
         if 'JIRA' in sub:
             issue_id = re.findall(r'[(](.*?)[)]', sub)

             j = jbase(issue_id[0])
             j.key()
             j.summary()
             j.components()
             j.description()
             j.attachment()
             j.comment()
             j.resolution()

if __name__=='__main__':
     main()
      

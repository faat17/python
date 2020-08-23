#-*- coding: UTF-8 -*-


import paramiko
import os
import base64
import pandas as pd
import numpy as np
import time, datetime


#---登录210.104，运行shell脚本---
client = paramiko.SSHClient() 
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())  # 允许连接不在know_hosts文件中的主机
passwords = base64.b64decode(b'QmRVKmZSTjMwYTV2')    #base64解码
passwords = str(passwords,encoding ="utf-8")
client.connect('host ip',port,'username',passwords)   
execmd = 'pwd' #需要输入的命令
stdin, stdout, stderr = client.exec_command ('pwd') 
print(stdout.read()) 
stdin, stdout, stderr = client.exec_command ('cd /home/wgdata/shell/alarm/; ls')
print(stdout.readlines())
stdin, stdout, stderr = client.exec_command ('python /home/wgdata/shell/alarm/hc_alarm.py')
print(stdout.readlines())
client.close()

#---把告警的两个脚本拉到240.90这台机器---
alarm_time=time.strftime('%m%d',time.localtime(time.time()))
socket_4G_filename=alarm_time+'_4G_socket.csv'
socket_5G_filename=alarm_time+'_5G_socket.csv'
corba_4G_filename=alarm_time+'_4G_corba.csv'
corba_5G_filename=alarm_time+'_5G_corba.csv'

sf= paramiko.Transport(('host ip',port))
sf.connect(username='wgdata', password=passwords)
sftp=paramiko.SFTPClient.from_transport(sf) 
sftp.get('/home/wgdata/shell/alarm/'+socket_4G_filename, '/home/wgdata/pg/'+socket_4G_filename) 
sftp.get('/home/wgdata/shell/alarm/'+socket_5G_filename, '/home/wgdata/pg/'+socket_5G_filename)
sftp.get('/home/wgdata/shell/alarm/'+corba_4G_filename, '/home/wgdata/pg/'+corba_4G_filename)
sftp.get('/home/wgdata/shell/alarm/'+corba_5G_filename, '/home/wgdata/pg/'+corba_5G_filename)
# 上传文件到远程主机，也可能会抛出异常
#sftp.put('/home/test.sh', 'test.sh')
sf.close() 












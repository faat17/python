# 利用python实现自动化数据统计的脚本
## 项目介绍
由于项目需求，每天需要跑不同的SQL语句来统计数据并对比差异，耗费大量的时间精力做重复的事情，严重影响工作效率，故提出一种基于面向对象的编程语言Python构建的自动化数据日报统计脚本，利用python语言的高效、灵活、简介等特点，结合os、psycopg2、pandas、smtplib等模块提供的强大功能，构建了一个可以自动进行提取、清洗、统计的python脚本，并通过linux服务器设置定时任务，完全解放人力，大量节约时间成本。
## 主要职责
1. 与业务部门深度结合，并根据部门需求编写SQL语句从底层数据库中提取数据 
2. 利用pandas对数据进行清洗和处理，对比今天与昨天的数据差异并输出为结果表，使用SMTP服务将结果发送至邮箱 
3. 将python脚本固化至linux服务器中，并设置定时任务，实现日报的自动化统计功能，提升80%工作效率
## 代码展示
### 导入python依赖包
```python
import os
os.environ['NLS_LANG'] = 'AMERICAN_AMERICA.UTF8'
import psycopg2
import csv
import base64
import pandas as pd
import time, datetime
from datetime import datetime,timedelta

if __name__ == "__main__":
```
### 登录数据库
```python
    tmp_file1 = open('00TABLENMAE.csv', mode='w', newline='')
    writer1 = csv.writer(tmp_file1)
    PGDatabase = 'databasename'
    DPGUser = 'username'
    PGPassword = base64.b64decode(b'TzQtd2dkYXRhJTIwMTk=')    #base64解码
    PGPassword = str(PGPassword,encoding ="utf-8")
    PGHost = 'host ip'
    PGPort = 'port'
    conn = psycopg2.connect(database=PGDatabase, user=DPGUser, password=PGPassword, host=PGHost, port=PGPort)
    cursor = conn.cursor()
```

### 读取schema中的表名称
```python
    cursor.execute("SELECT tablename FROM pg_tables where schemaname='public'")  # 使用连接对象获得一个cursor对象
    TABLENMAE_ONE = []
    rows = cursor.fetchall()  # 收size条返回结果行
    n = 0
    for row in rows:  # 取rows中的每行第一个元素
        TABLENMAE_ONE .append(row[0])
        n += 1
        #writer1.writerow([TABLENMAE[n]])

    print(len(TABLENMAE_ONE))
```
### 打开文件写入表头
```python
    m=0
    if m < len(TABLENMAE_ONE):
        sqll = 'ftp_task.csv'
        tmp_file3 = open(sqll, mode='w', newline='')
        writer3 = csv.writer(tmp_file3)
        writer3.writerow(['omc_id','ftp_type','province_code','province_name','omc_name','port','ip'])  #表的格式
        s = 'NULL'
```
### 使用sql语句统计所以在采集的流程并进行数据处理
```python
#-----ftp/db---
        sql_ftp_task = 'select a.id,a.omc_id,a.type as ftp_type,b.profession_code,b.province_code,c.name as province_name,b.name as omc_name,a.port,a.host as ip,b.network_type from ftp_task a join omc b on a.omc_id=b.id join province c on c.code=b.province_code where a.is_del=\'f\''
        cursor.execute(sql_ftp_task)
        ftp_task_rows = cursor.fetchall()
        ftp_task=pd.DataFrame(list(ftp_task_rows),columns=['id','omc_id','ftp_type','profession_code','province_code','province_name','omc_name','port','ip','network_type'])
        ftp_task['net_type']='4G'

        sql_gsm_ftp_task = 'select a.id,a.omc_id,a.type as ftp_type,b.profession_code,b.province_code,c.name as province_name,b.name as omc_name,a.port,a.host as ip,b.network_type from gsm_file_task a join omc b on a.omc_id=b.id join province c on c.code=b.province_code where a.is_del=\'f\''
        cursor.execute(sql_gsm_ftp_task)
        gsm_ftp_task_rows = cursor.fetchall()
        gsm_ftp_task=pd.DataFrame(list(gsm_ftp_task_rows),columns=['id','omc_id','ftp_type','profession_code','province_code','province_name','omc_name','port','ip','network_type'])
        gsm_ftp_task['net_type']='2G'

        sql_gsm_db_task = 'select a.id,a.omc_id,a.type as ftp_type,b.profession_code,b.province_code,c.name as province_name,b.name as omc_name,a.port,a.host as ip ,b.network_type from gsm_database_task a join omc b on a.omc_id=b.id join province c on c.code=b.province_code where a.is_del=\'f\''
        cursor.execute(sql_gsm_db_task)
        sql_gsm_db_task_rows = cursor.fetchall()
        gsm_db_task=pd.DataFrame(list(sql_gsm_db_task_rows),columns=['id','omc_id','ftp_type','profession_code','province_code','province_name','omc_name','port','ip','network_type'])
        gsm_db_task['net_type']='2G'

        ftp_db_task=pd.concat([ftp_task,gsm_ftp_task,gsm_db_task])


#-----corba/socket---
        sql_corba_task_4G = 'select a.id,a.omc_id,b.province_code,c.name as province_name,b.profession_code,b.name as omc_name,a.host as ip from corba_task a join omc b on a.omc_id=b.id and a.tag != \'NR\' join province c on c.code=b.province_code where a.is_del=\'f\''
        cursor.execute(sql_corba_task_4G)
        corba_task_rows = cursor.fetchall()
        corba_task_4G=pd.DataFrame(list(corba_task_rows),columns=['id','omc_id','province_code','province_name','profession_code','omc_name','ip'])
        corba_task_4G = corba_task_4G.reindex(columns = ['id','omc_id','province_code','province_name','profession_code','omc_name','ip'])
        corba_task_4G['alarm_type']='4G_CORBA'
        corba_task_4G['network_type']='4G'

        sql_corba_task_5G = 'select a.id,a.omc_id,b.province_code,c.name as province_name,b.profession_code,b.name as omc_name,a.host as ip from corba_task a join omc b on a.omc_id=b.id and a.tag = \'NR\' join province c on c.code=b.province_code where a.is_del=\'f\''
        cursor.execute(sql_corba_task_5G)
        corba_task_rows = cursor.fetchall()
        corba_task_5G=pd.DataFrame(list(corba_task_rows),columns=['id','omc_id','province_code','province_name','profession_code','omc_name','ip'])
        corba_task_5G = corba_task_5G.reindex(columns = ['id','omc_id','province_code','province_name','profession_code','omc_name','ip'])
        corba_task_5G['alarm_type']='5G_CORBA'
        corba_task_5G['network_type']='5G'


        sql_socket_task_4G = 'select a.id,a.omc_id,b.province_code,c.name as province_name,b.profession_code,b.name as omc_name,a.socket_host as ip from socket_task a join omc b on a.omc_id=b.id and a.tag != \'NR\' join province c on c.code=b.province_code where a.is_del=\'f\''
        cursor.execute(sql_socket_task_4G)
        socket_task_4G_rows = cursor.fetchall()
        socket_task_4G=pd.DataFrame(list(socket_task_4G_rows),columns=['id','omc_id','province_code','province_name','profession_code','omc_name','ip'])
        socket_task_4G = socket_task_4G.reindex(columns = ['id','omc_id','province_code','province_name','profession_code','omc_name','ip'])
        socket_task_4G['alarm_type']='4G_SOCKET'
        socket_task_4G['network_type']='4G'

        sql_socket_task_5G = 'select a.id,a.omc_id,b.province_code,c.name as province_name,b.profession_code,b.name as omc_name,a.socket_host as ip from socket_task a join omc b on a.omc_id=b.id and a.tag = \'NR\' join province c on c.code=b.province_code where a.is_del=\'f\''
        cursor.execute(sql_socket_task_5G)
        socket_task_5G_rows = cursor.fetchall()
        socket_task_5G=pd.DataFrame(list(socket_task_5G_rows),columns=['id','omc_id','province_code','province_name','profession_code','omc_name','ip'])
        socket_task_5G = socket_task_5G.reindex(columns = ['id','omc_id','province_code','province_name','profession_code','omc_name','ip'])
        socket_task_5G['alarm_type']='5G_SOCKET'
        socket_task_5G['network_type']='5G'

        socket_task_4G = socket_task_4G.append(socket_task_5G)
        socket_task_4G = socket_task_4G.append(socket_task_5G)
        socket_task_4G = socket_task_4G.drop_duplicates(subset=['id'],keep=False)

        sql_gsm_alarm_task = 'select a.id,a.omc_id,b.province_code,c.name as province_name,b.profession_code,b.name as omc_name,a.host as ip,a.protocol as alarm_type from gsm_alarm_task a join omc b on a.omc_id=b.id join province c on c.code=b.province_code where a.is_del=\'f\''
        cursor.execute(sql_gsm_alarm_task)
        gsm_alarm_task_rows = cursor.fetchall()
        gsm_alarm_task=pd.DataFrame(list(gsm_alarm_task_rows),columns=['id','omc_id','province_code','province_name','profession_code','omc_name','ip','alarm_type'])
        gsm_alarm_task = gsm_alarm_task.reindex(columns = ['id','omc_id','province_code','province_name','profession_code','omc_name','ip'])
        gsm_alarm_task['alarm_type']='2G_SOCKET'
        gsm_alarm_task['network_type']='2G'

        socket_task=pd.concat([socket_task_5G,socket_task_4G])
        socket_task=socket_task.reset_index()
        corba_task=pd.concat([corba_task_5G,corba_task_4G])
        corba_task=corba_task.reset_index()
        alarm_task=pd.concat([corba_task,socket_task,gsm_alarm_task])
```
### 使用sql语句统计今天和昨天所有采集成功的流程
```python
#-----进展---
#-----4G-ftp-进展---
        check_time=time.strftime('%Y%m%d',time.localtime(time.time()))
	    
        sql_ftp_real = 'select b.ftask_id as task_id,a.id as omc_id,a.network_type,type as ftp_type,a.profession_code,a.province_code,d.name as province_name,a.name as omc_name,b.port, b.host as ip,b.max_file_size, b.file_status from omc a join ( select  f.omc_id, f.host, f.id as ftask_id, f.type , f.port, f.province_code ,g.file_status, max(g.file_size) as max_file_size   from  ftp_task_detail g join ftp_task f on g.task_id = f.id and f.is_del = \'f\' where g.data_time_in_file_name like \'%{}%\'  group by (f.omc_id, f.host, f.id, f.type, f.port, f.province_code,g.file_status)  ) b on a.id=b.omc_id join task_status_trace_event c on \'E/SCANNER/\'||b.ftask_id=c.full_path join province d on a.province_code=d.code'.format(check_time)
        cursor.execute(sql_ftp_real)
        ftp_real_rows = cursor.fetchall()
        ftp_real_today=pd.DataFrame(list(ftp_real_rows),columns=['task_id','omc_id','network_type','ftp_type','profession_code','province_code','province_name','omc_name','port','ip','max_file_size','file_status'])
        ftp_real=pd.DataFrame(list(ftp_real_rows),columns=['task_id','omc_id','network_type','ftp_type','profession_code','province_code','province_name','omc_name','port','ip','max_file_size','file_status'])
        ftp_real['file_status']=ftp_real['file_status'].map(lambda x:str(x) )
        ftp_real_shallow=ftp_real.copy(deep=True)
        #ftp_real_shallow['task_id']=ftp_real_shallow['task_id'].map(lambda x:str(x) )
        def ab(df):
            return','.join(df.values)
        ftp_real_shallow = ftp_real_shallow.groupby(['task_id','omc_id','network_type','ftp_type','profession_code','province_name','omc_name','port','ip'])['file_status'].apply(ab)
        ftp_real_shallow = ftp_real_shallow.reset_index()
        ftp_real_shallow = ftp_real_shallow.reindex(columns=['task_id','omc_id','ftp_type','profession_code','province_name','omc_name','port','ip','file_status','network_type'])
        ftp_real_shallow['net_type']='4G'

        #ftp_real_shallow.drop(['file_status'],axis=1,inplace=True)
        ftp_real = ftp_real.groupby(['ftp_type','province_code','province_name','omc_name','omc_id','task_id','network_type','profession_code','port','ip'])['file_status'].apply(ab)
        ftp_real = ftp_real.reset_index()


        # print(len(ftp_real))
        # print(ftp_real.loc[0,'ftp_type'])
        ftp_real=ftp_real.groupby(['ftp_type','province_code','province_name','omc_name','omc_id','network_type','profession_code','port'])['ip'].apply(ab)
        ftp_real = ftp_real.reset_index()
        ftp_real['ip数量']=ftp_real['ip'].map(lambda x:len(x.split(',')))
        ftp_real=ftp_real.reindex(columns=['ftp_type','province_code','province_name','omc_name','omc_id','profession_code','port','ip','ip数量','network_type'])
        ftp_real['net_type']='4G'

#-----4G-ftp-昨日进展---
        check_time_today=datetime.strptime(check_time,'%Y%m%d')
        check_time_yesterday=(check_time_today+timedelta(days=-1)).strftime('%Y%m%d')
        
        sql_ftp_real_yesterday = 'select b.ftask_id as task_id,a.id as omc_id,a.network_type,type as ftp_type,a.profession_code,a.province_code,d.name as province_name,a.name as omc_name,b.port, b.host as ip,b.max_file_size, b.file_status from omc a join ( select  f.omc_id, f.host, f.id as ftask_id, f.type , f.port, f.province_code ,g.file_status, max(g.file_size) as max_file_size   from  ftp_task_detail g join ftp_task f on g.task_id = f.id and f.is_del = \'f\' where g.data_time_in_file_name like \'%{}%\'  group by (f.omc_id, f.host, f.id, f.type, f.port, f.province_code,g.file_status)  ) b on a.id=b.omc_id join task_status_trace_event c on \'E/SCANNER/\'||b.ftask_id=c.full_path join province d on a.province_code=d.code'.format(check_time_yesterday)
        cursor.execute(sql_ftp_real_yesterday)
        ftp_real_rows_yesterday = cursor.fetchall()
        ftp_real_yesterday=pd.DataFrame(list(ftp_real_rows_yesterday),columns=['task_id','omc_id','network_type','ftp_type','profession_code','province_code','province_name','omc_name','port','ip','max_file_size','file_status'])
        ftp_real_yesterday=ftp_real_yesterday.append(ftp_real_today)
        ftp_real_yesterday=ftp_real_yesterday.append(ftp_real_today)
        ftp_4G_false=ftp_real_yesterday.drop_duplicates(subset=['task_id'],keep=False)

#-----2G-ftp-进展---
        sql_gsm_ftp_event = 'select a.task_id,d.omc_id,b.network_type,d.type as ftp_type,b.profession_code,b.province_code,c.name as province_name,b.name as omc_name,d.port,d.host as ip,max(a.target_file_size) as max_file_size ,a.success from gsm_file_status_trace_event a join gsm_file_task d on a.task_id=d.id join omc b on d.omc_id=b.id join province c on c.code=b.province_code where d.is_del=\'f\' and a.success=\'t\' and split_part(split_part(TO_TIMESTAMP(substring(event_time::character varying,1,10)::bigint)::character varying, \'-\', 1) || split_part(TO_TIMESTAMP(substring(event_time::character varying,1,10)::bigint)::character varying, \'-\', 2) || split_part(TO_TIMESTAMP(substring(event_time::character varying,1,10)::bigint)::character varying, \'-\', 3), \' \', 1) like \'%{}%\' group by (a.task_id,d.omc_id,b.network_type,d.type,b.province_code,c.name,b.name,d.port,d.host,a.success,b.profession_code)'.format(check_time)
        cursor.execute(sql_gsm_ftp_event)
        gsm_ftp_event_rows = cursor.fetchall()
        gsm_ftp_real_today = pd.DataFrame(list(gsm_ftp_event_rows),columns=['task_id','omc_id','network_type','ftp_type','profession_code','province_code','province_name','omc_name','port','ip','max_file_size','file_status'])
        gsm_ftp_event=pd.DataFrame(list(gsm_ftp_event_rows),columns=['task_id','omc_id','network_type','ftp_type','profession_code','province_code','province_name','omc_name','port','ip','max_file_size','file_status'])
        gsm_ftp_event_shallow=gsm_ftp_event.copy(deep=True)
        gsm_ftp_event_shallow.drop(['province_code','max_file_size'],axis=1,inplace=True)
        gsm_ftp_event_shallow = gsm_ftp_event_shallow.reindex(columns=['task_id','omc_id','ftp_type','profession_code','province_name','omc_name','port','ip','file_status','network_type'])
        gsm_ftp_event_shallow['net_type']='2G'
        gsm_ftp_event=gsm_ftp_event.groupby(['ftp_type','province_code','province_name','network_type','omc_name','omc_id','profession_code','port'])['ip'].apply(ab)
        gsm_ftp_event = gsm_ftp_event.reset_index()
        gsm_ftp_event['ip数量']=gsm_ftp_event['ip'].map(lambda x:len(x.split(',')))
        gsm_ftp_event=gsm_ftp_event.reindex(columns=['ftp_type','province_code','province_name','omc_name','omc_id','profession_code','port','ip','ip数量','network_type'])
        gsm_ftp_event['net_type']='2G'

# -----2G-ftp-昨日进展---
        sql_gsm_ftp_event_yesterday = 'select a.task_id,d.omc_id,b.network_type,d.type as ftp_type,b.profession_code,b.province_code,c.name as province_name,b.name as omc_name,d.port,d.host as ip,max(a.target_file_size) as max_file_size ,a.success from gsm_file_status_trace_event a join gsm_file_task d on a.task_id=d.id join omc b on d.omc_id=b.id join province c on c.code=b.province_code where d.is_del=\'f\' and a.success=\'t\' and split_part(split_part(TO_TIMESTAMP(substring(event_time::character varying,1,10)::bigint)::character varying, \'-\', 1) || split_part(TO_TIMESTAMP(substring(event_time::character varying,1,10)::bigint)::character varying, \'-\', 2) || split_part(TO_TIMESTAMP(substring(event_time::character varying,1,10)::bigint)::character varying, \'-\', 3), \' \', 1) like \'%{}%\' group by (a.task_id,d.omc_id,b.network_type,d.type,b.province_code,c.name,b.name,d.port,d.host,a.success,b.profession_code)'.format(check_time_yesterday)
        cursor.execute(sql_gsm_ftp_event_yesterday)
        gsm_ftp_real_rows_yesterday = cursor.fetchall()
        gsm_ftp_real_yesterday = pd.DataFrame(list(gsm_ftp_real_rows_yesterday),columns=['task_id','omc_id','network_type','ftp_type','profession_code','province_code','province_name','omc_name','port','ip','max_file_size','file_status'])
        gsm_ftp_real_yesterday = gsm_ftp_real_yesterday.append(gsm_ftp_real_today)
        gsm_ftp_real_yesterday = gsm_ftp_real_yesterday.append(gsm_ftp_real_today)
        gsm_ftp_false=gsm_ftp_real_yesterday.drop_duplicates(subset=['task_id'],keep=False)


#-----2G-DB-进展---
        sql_gsm_db_event = 'select a.task_id,d.omc_id,b.network_type,d.type as ftp_type,b.profession_code,b.province_code,c.name as province_name,b.name as omc_name,d.port,d.host as ip,max(a.target_file_size) as max_file_size ,a.success from gsm_database_status_trace_event a join  gsm_database_task d on a.task_id=d.id join omc b on d.omc_id=b.id join province c on c.code=b.province_code where d.is_del=\'f\' and a.success=\'t\' and split_part(split_part(TO_TIMESTAMP(substring(event_time::character varying,1,10)::bigint)::character varying, \'-\', 1) || split_part(TO_TIMESTAMP(substring(event_time::character varying,1,10)::bigint)::character varying, \'-\', 2) || split_part(TO_TIMESTAMP(substring(event_time::character varying,1,10)::bigint)::character varying, \'-\', 3), \' \', 1) like \'%{}%\' group by (a.task_id,d.omc_id,b.network_type,d.type,b.province_code,c.name,b.name,d.port,d.host,a.success,b.profession_code)'.format(check_time)
        cursor.execute(sql_gsm_db_event)
        gsm_db_event_rows = cursor.fetchall()
        gsm_db_real_today=pd.DataFrame(list(gsm_db_event_rows),columns=['task_id','omc_id','network_type','ftp_type','profession_code','province_code','province_name','omc_name','port','ip','max_file_size','file_status'])
        gsm_db_event=pd.DataFrame(list(gsm_db_event_rows),columns=['task_id','omc_id','network_type','ftp_type','profession_code','province_code','province_name','omc_name','port','ip','max_file_size','file_status'])
        gsm_db_event_shallow=gsm_db_event.copy(deep=True)
        gsm_db_event_shallow.drop(['province_code','max_file_size'],axis=1,inplace=True)
        gsm_db_event_shallow = gsm_db_event_shallow.reindex(columns=['task_id','omc_id','ftp_type','profession_code','province_name','omc_name','port','ip','file_status','network_type'])
        gsm_db_event_shallow['net_type']='2G'
        gsm_db_event=gsm_db_event.groupby(['ftp_type','province_code','province_name','network_type','omc_name','omc_id','profession_code','port'])['ip'].apply(ab)
        gsm_db_event = gsm_db_event.reset_index()
        gsm_db_event['ip数量']=gsm_db_event['ip'].map(lambda x:len(x.split(',')))
        gsm_db_event=gsm_db_event.reindex(columns=['ftp_type','province_code','province_name','omc_name','omc_id','profession_code','port','ip','ip数量','network_type'])
        gsm_db_event['net_type']='2G'

#-----2G-DB-昨日进展---
        sql_gsm_db_event_yesterday = 'select a.task_id,d.omc_id,b.network_type,d.type as ftp_type,b.profession_code,b.province_code,c.name as province_name,b.name as omc_name,d.port,d.host as ip,max(a.target_file_size) as max_file_size ,a.success from gsm_database_status_trace_event a join  gsm_database_task d on a.task_id=d.id join omc b on d.omc_id=b.id join province c on c.code=b.province_code where d.is_del=\'f\' and a.success=\'t\' and split_part(split_part(TO_TIMESTAMP(substring(event_time::character varying,1,10)::bigint)::character varying, \'-\', 1) || split_part(TO_TIMESTAMP(substring(event_time::character varying,1,10)::bigint)::character varying, \'-\', 2) || split_part(TO_TIMESTAMP(substring(event_time::character varying,1,10)::bigint)::character varying, \'-\', 3), \' \', 1) like \'%{}%\' group by (a.task_id,d.omc_id,b.network_type,d.type,b.province_code,c.name,b.name,d.port,d.host,a.success,b.profession_code)'.format(check_time_yesterday)
        cursor.execute(sql_gsm_db_event_yesterday)
        gsm_db_real_rows_yesterday = cursor.fetchall()
        gsm_db_real_yesterday = pd.DataFrame(list(gsm_db_real_rows_yesterday),columns=['task_id','omc_id','network_type','ftp_type','profession_code','province_code','province_name','omc_name','port','ip','max_file_size','file_status'])
        gsm_db_real_yesterday = gsm_db_real_yesterday.append(gsm_db_real_today)
        gsm_db_real_yesterday = gsm_db_real_yesterday.append(gsm_db_real_today)
        gsm_db_false=gsm_db_real_yesterday.drop_duplicates(subset=['task_id'],keep=False)

        gsm_false = pd.concat([gsm_db_false,gsm_ftp_false])

#--2G-告警-进展----
        sql_gsm_alarm = 'select distinct a.id,a.omc_id,a.protocol as alarm_type,c.name as omc_name,c.province_code,d.name as province_name,c.profession_code,a.port,a.host as ip from  gsm_alarm_task a join gsm_alarm_status_trace_event b on a.id = b.task_id join omc c on a.omc_id = c.id join province d on d.code = c.province_code where b.alarm_records !=0 and a.is_del = \'f\' and split_part(split_part(TO_TIMESTAMP(substring(event_time::character varying,1,10)::bigint)::character varying, \'-\', 1) || split_part(TO_TIMESTAMP(substring(event_time::character varying,1,10)::bigint)::character varying, \'-\', 2) || split_part(TO_TIMESTAMP(substring(event_time::character varying,1,10)::bigint)::character varying, \'-\', 3), \' \', 1) like \'%{}%\';'.format(check_time)
        cursor.execute(sql_gsm_alarm)
        sql_gsm_alarm_rows = cursor.fetchall()
        gsm_alarm=pd.DataFrame(list(sql_gsm_alarm_rows),columns=['id','omc_id','alarm_type','omc_name','province_code','province_name','profession_code','port','ip'])
        if gsm_alarm.empty:
            gsm_alarm=pd.DataFrame(columns=['omc_name', 'province_name', 'profession_code', 'ip', 'ip数量', 'alarm_type','network_type'])
        else:
            def ab(df):
                return','.join(df.values)
            gsm_alarm = gsm_alarm.groupby(['omc_name','province_name','profession_code','alarm_type'])['ip'].apply(ab)
            gsm_alarm = gsm_alarm.reset_index()
            gsm_alarm['ip数量']=gsm_alarm['ip'].map(lambda x:len(x.split(',')))
            gsm_alarm=gsm_alarm[['omc_name','province_name','profession_code','ip','ip数量','alarm_type']]
            gsm_alarm['network_type']='2G'


        real_all=pd.concat([ftp_real,gsm_ftp_event,gsm_db_event])
        real_all_event_shallow=pd.concat([ftp_real_shallow,gsm_ftp_event_shallow,gsm_db_event_shallow])

        n = 0
        #for row in ftp_task_rows:
            #row = list(row)
            #writer3.writerow(row)
            #n += 1

        print(n)
```
### 查询结束关闭数据库
```python
    tmp_file3.close()
    cursor.close()
    conn.close()
```
### 对查询的数据按需求进行分类处理
```python
ftp_db_task_shallow=ftp_db_task.copy(deep=True)
    def ab(df):
        return','.join(df.values)
    ftp_db_task=ftp_db_task.groupby(['omc_id','ftp_type','profession_code','province_code','province_name','omc_name','port','network_type','net_type'])['ip'].apply(ab)
    ftp_db_task = ftp_db_task.reset_index()
    ftp_db_task['ip数量']=ftp_db_task['ip'].map(lambda x:len(x.split(',')))
    ftp_db_task=ftp_db_task[['omc_id','ftp_type','profession_code','province_code','province_name','omc_name','port','ip','ip数量','network_type','net_type']]
    alarm_task=alarm_task.groupby(['omc_id','province_name','profession_code','omc_name','alarm_type','network_type'])['ip'].apply(ab)
    alarm_task = alarm_task.reset_index()
    alarm_task['ip数量']=alarm_task['ip'].map(lambda x:len(x.split(',')))
    alarm_task=alarm_task[['omc_id','province_name','profession_code','omc_name','ip','ip数量','alarm_type','network_type']]


    NRM_index_list=[]
    real_all.index = range(len(real_all))
    for j in range(len(real_all)):
        if real_all.loc[j,'ftp_type']=='NRM':
            NRM_index_list.append(j)
    list_all=list(range(len(real_all)))
    table_NRM_real=real_all.iloc[NRM_index_list,:]
    table_PM_real=real_all.iloc[list(set(list_all)-set(NRM_index_list)),:]
```
### 使用shell脚本获取告警数据
```python
#--4G-告警-进展----
    with open ('hc_alarm_0615.py','r') as ff:
        exec(ff.read())
    print('告警结果已获取完成！')
#--4G_socket结果表输出---
    alarm_time=time.strftime('%m%d',time.localtime(time.time()))
    socket_4G_filename=alarm_time+'_4G_socket.csv'
    socket_5G_filename=alarm_time+'_5G_socket.csv'
    corba_4G_filename=alarm_time+'_4G_corba.csv'
    corba_5G_filename=alarm_time+'_5G_corba.csv'
    #os.chdir(r'E:\VBshare\资源性能告警每日统计')
    if not os.path.getsize(socket_4G_filename):
        global table_socket_result
        table_socket_result=pd.DataFrame(columns=['omc_name', 'province_name', 'profession_code', 'ip', 'ip数量', 'alarm_type','network_type'])
    else:
        table_socket=pd.read_csv(socket_4G_filename, header = 0, encoding = "utf-8")
        socket_task.rename(columns={'id':'socket_task_id'}, inplace = True)
        table_socket['socket_task_id']=table_socket['socket_task_id'].map(lambda x:str(x))
        socket_task['socket_task_id']=socket_task['socket_task_id'].map(lambda x:str(x))
        table_socket.drop_duplicates(subset=['socket_task_id'], keep='first', inplace=True)
        table_socket_result=pd.merge(table_socket,socket_task,on=['socket_task_id'],how='left')

        def ab(df):
            return','.join(df.values)
        table_socket_result = table_socket_result.groupby(['omc_name','province_name','profession_code'])['ip'].apply(ab)
        table_socket_result = table_socket_result.reset_index()
        table_socket_result['ip数量']=table_socket_result['ip'].map(lambda x:len(x.split(',')))
        table_socket_result['alarm_type']='SOCKET'
        table_socket_result['network_type']='4G'
        if table_socket_result.empty:
            table_socket_result=pd.DataFrame(columns=['omc_name', 'province_name', 'profession_code', 'ip', 'ip数量', 'alarm_type','network_type'])
        else:
            table_socket_result=table_socket_result

#--5G_socket结果表输出---
    if not os.path.getsize(socket_5G_filename):
        global table_socket_5G_result
        table_socket_5G_result=pd.DataFrame(columns=['omc_name', 'province_name', 'profession_code', 'ip', 'ip数量', 'alarm_type','network_type'])
    else:
        table_socket_5G=pd.read_csv(socket_5G_filename,header =0,encoding = "utf-8")
        table_socket_5G = pd.DataFrame(table_socket_5G)
        table_socket_5G = table_socket_5G.dropna(axis=0,how='any')
        socket_task.rename(columns={'id':'socket_task_id'}, inplace = True)
        table_socket_5G['socket_task_id']=table_socket_5G['socket_task_id'].map(lambda x:str(x))
        socket_task['socket_task_id']=socket_task['socket_task_id'].map(lambda x:str(x))
        table_socket_5G.drop_duplicates(subset=['socket_task_id'], keep='first', inplace=True)
        table_socket_5G_result=pd.merge(table_socket_5G,socket_task,on=['socket_task_id'],how='left')

        def ab(df):
            return','.join(df.values)
        table_socket_5G_result = table_socket_5G_result.groupby(['omc_name','province_name','profession_code'])['ip'].apply(ab)
        table_socket_5G_result = table_socket_5G_result.reset_index()
        table_socket_5G_result['ip数量']=table_socket_5G_result['ip'].map(lambda x:len(x.split(',')))
        table_socket_5G_result['alarm_type']='SOCKET'
        table_socket_5G_result['network_type']='5G'
        if table_socket_5G_result.empty:
            #table_socket_5G_result=pd.DataFrame({'omc_name':'','province_name':'','profession_code':'','ip':'','ip数量':'','alarm_type':'','network_type':''},index=['0'])
            table_socket_5G_result=pd.DataFrame(columns=['omc_name', 'province_name', 'profession_code', 'ip', 'ip数量', 'alarm_type','network_type'])
        else:
            table_socket_5G_result=table_socket_5G_result

#--4G_corba结果表输出---
    #os.chdir(r'E:\VBshare\资源性能告警每日统计')
    if not os.path.getsize(corba_4G_filename):
        global table_corba_result
        #table_corba_result=pd.DataFrame({'omc_name':'','province_name':'','profession_code':'','ip':'','ip数量':'','alarm_type':'','network_type':''},index=['0'])
        table_corba_result=pd.DataFrame(columns=['omc_name', 'province_name', 'profession_code', 'ip', 'ip数量', 'alarm_type','network_type'])
    else:
        table_corba=pd.read_csv(corba_4G_filename,header =0,encoding = "utf-8")
        corba_task.rename(columns={'id':'corba_task_id'}, inplace = True)
        corba_task['corba_task_id']=corba_task['corba_task_id'].map(lambda x:str(x))
        table_corba['corba_task_id']=table_corba['corba_task_id'].map(lambda x:str(x))
        table_corba.drop_duplicates(subset=['corba_task_id'],keep='first',inplace=True)
        table_corba_result=pd.merge(table_corba,corba_task,on=['corba_task_id'],how='left')


        def ab(df):
            return','.join(df.values)
        table_corba_result = table_corba_result.groupby(['omc_name','province_name','profession_code'])['ip'].apply(ab)
        table_corba_result = table_corba_result.reset_index()
        table_corba_result['ip数量']=table_corba_result['ip'].map(lambda x:len(x.split(',')))
        table_corba_result['alarm_type']='CORBA'
        table_corba_result['network_type']='4G'
        if table_corba_result.empty:
            table_corba_result=pd.DataFrame(columns=['omc_name', 'province_name', 'profession_code', 'ip', 'ip数量', 'alarm_type','network_type'])
        else:
            table_corba_result=table_corba_result
#--5G_corba结果表输出---
    if not os.path.getsize(corba_5G_filename):
        global table_corba_5G_result
        #table_corba_result=pd.DataFrame({'omc_name':'','province_name':'','profession_code':'','ip':'','ip数量':'','alarm_type':'','network_type':''},index=['0'])
        table_corba_5G_result=pd.DataFrame(columns=['omc_name', 'province_name', 'profession_code', 'ip', 'ip数量', 'alarm_type','network_type'])
    else:
        table_corba_5G=pd.read_csv(corba_5G_filename,header =0,encoding = "utf-8")
        corba_task.rename(columns={'id':'corba_task_id'}, inplace = True)
        corba_task['corba_task_id']=corba_task['corba_task_id'].map(lambda x:str(x))
        table_corba_5G['corba_task_id']=table_corba_5G['corba_task_id'].map(lambda x:str(x))
        table_corba_5G.drop_duplicates(subset=['corba_task_id'],keep='first',inplace=True)
        table_corba_5G_result=pd.merge(table_corba_5G,corba_task,on=['corba_task_id'],how='left')

        def ab(df):
            return','.join(df.values)
        table_corba_5G_result = table_corba_5G_result.groupby(['omc_name','province_name','profession_code'])['ip'].apply(ab)
        table_corba_5G_result = table_corba_5G_result.reset_index()
        table_corba_5G_result['ip数量']=table_corba_5G_result['ip'].map(lambda x:len(x.split(',')))
        table_corba_5G_result['alarm_type']='CORBA'
        table_corba_5G_result['network_type']='5G'
        if table_corba_5G_result.empty:
            table_corba_5G_result=pd.DataFrame(columns=['omc_name', 'province_name', 'profession_code', 'ip', 'ip数量', 'alarm_type','network_type'])
        else:
            table_corba_5G_result=table_corba_5G_result

#--corba和socket结果汇总输出---
    table_alarm=pd.concat([table_socket_5G_result,table_socket_result,table_corba_result,table_corba_5G_result,gsm_alarm])
    table_alarm = table_alarm.reindex(columns=['omc_name','province_name','profession_code','ip','ip数量','alarm_type','network_type'])
```
### 今日和昨日流程比较，统计失败的流程
```python
#--文件失败流程结果输出---
    ftp_db_task_ip=ftp_db_task_shallow.iloc[:,[0]]
    ftp_db_real_ip=real_all_event_shallow.iloc[:,[0]]
    ftp_db_real_ip.rename(columns={'task_id':'id'}, inplace = True)

    ftp_db_ip_all=pd.concat([ftp_db_task_ip,ftp_db_real_ip])
    #ftp_db_ip_all=pd.DataFrame(columns=['id', 'omc_id', 'ftp_type', 'profession_code', 'province_code', 'omc_name', 'port', 'ip', 'network_type','net_type'])

    ftp_db_false_ip = ftp_db_ip_all.drop_duplicates(subset='id',keep=False)
    ftp_db_false_ip.sort_values(by=['id'],ascending = True)
    ftp_db_false_ip['id']=ftp_db_false_ip['id'].map(lambda x:int(x))
    ftp_db_task_shallow['id']=ftp_db_task_shallow['id'].map(lambda x:int(x))
    ftp_db_false=pd.merge(ftp_db_false_ip,ftp_db_task_shallow,on=['id'],how='left')

#--告警失败流程结果输出---
    df1=pd.DataFrame(alarm_task,columns=['omc_name','network_type'])
    df2=pd.DataFrame(table_alarm,columns=['omc_name','network_type'])
    df1=df1.append(df2)
    df1=df1.append(df2)
    table_alarm_false_name = df1.drop_duplicates(subset=['omc_name','network_type'],keep=False)
    table_alarm_false = pd.merge(table_alarm_false_name,alarm_task,on=['omc_name','network_type'],how='left')
    table_alarm_false = table_alarm_false.reindex(columns=['omc_id','omc_name','province_name','profession_code','ip','ip数量','alarm_type','network_type'])
```
### 统计成功采集流程和所有流程的数量，生成统计日报
```python
    # 数量统计
    ftp_task = ftp_db_task_shallow.copy()
    table_ftp = real_all_event_shallow.copy()
    # 创建DataFrame
    table_statistics = pd.DataFrame(np.arange(54).reshape(6, 9),index=['2G无线网', '2G核心网', '4G无线网', '4G核心网', '5G无线网', '5G核心网'],columns=['资源成功流程', '资源总流程', '资源成功占比', '性能成功流程', '性能总流程','性能成功占比', '告警成功流程', '告警总流程', '告警成功占比'])
    # 资源和性能
    # 无线
    table_success_wx_nrm_2G = table_ftp.loc[table_ftp['profession_code'].str.contains('WX') & table_ftp['ftp_type'].str.contains('NRM') & table_ftp['net_type'].str.contains('2G')]
    table_success_wx_pm_2G = table_ftp.loc[table_ftp['profession_code'].str.contains('WX') & table_ftp['ftp_type'].str.contains('PM') & table_ftp['net_type'].str.contains('2G')]
    table_success_wx_nrm = table_ftp.loc[table_ftp['profession_code'].str.contains('WX') & table_ftp['ftp_type'].str.contains('NRM') & table_ftp['net_type'].str.contains('4G')]
    table_success_wx_pm = table_ftp.loc[table_ftp['profession_code'].str.contains('WX') & table_ftp['ftp_type'].str.contains('PM') & table_ftp['net_type'].str.contains('4G')]
    table_success_wx_nrm_5G = table_ftp.loc[table_ftp['profession_code'].str.contains('WX') & table_ftp['ftp_type'].str.contains('NRM') & table_ftp['network_type'].str.contains('5G')]
    table_success_wx_pm_5G= table_ftp.loc[table_ftp['profession_code'].str.contains('WX') & table_ftp['ftp_type'].str.contains('PM') & table_ftp['network_type'].str.contains('5G')]
    table_success_wx_nrm_4G = table_ftp.loc[table_ftp['profession_code'].str.contains('WX') & table_ftp['ftp_type'].str.contains('NRM') & ~table_ftp['network_type'].str.contains('5G')]
    table_success_wx_pm_4G= table_ftp.loc[table_ftp['profession_code'].str.contains('WX') & table_ftp['ftp_type'].str.contains('PM') & ~table_ftp['network_type'].str.contains('5G')]

    table_all_wx_nrm_2G = ftp_task.loc[ftp_task['profession_code'].str.contains('WX') & ftp_task['ftp_type'].str.contains('NRM') & ftp_task['net_type'].str.contains('2G')]
    table_all_wx_pm_2G = ftp_task.loc[ftp_task['profession_code'].str.contains('WX') & ftp_task['ftp_type'].str.contains('PM') & ftp_task['net_type'].str.contains('2G')]
    table_all_wx_nrm = ftp_task.loc[ftp_task['profession_code'].str.contains('WX') & ftp_task['ftp_type'].str.contains('NRM') & ftp_task['net_type'].str.contains('4G')]
    table_all_wx_pm = ftp_task.loc[ftp_task['profession_code'].str.contains('WX') & ftp_task['ftp_type'].str.contains('PM') & ftp_task['net_type'].str.contains('4G')]
    table_all_wx_nrm_5G = ftp_task.loc[ftp_task['profession_code'].str.contains('WX') & ftp_task['ftp_type'].str.contains('NRM') & ftp_task['network_type'].str.contains('5G')]
    table_all_wx_pm_5G= ftp_task.loc[ftp_task['profession_code'].str.contains('WX') & ftp_task['ftp_type'].str.contains('PM') & ftp_task['network_type'].str.contains('5G')]
    table_all_wx_nrm_4G = ftp_task.loc[ftp_task['profession_code'].str.contains('WX') & ftp_task['ftp_type'].str.contains('NRM') & ~ftp_task['network_type'].str.contains('5G')]
    table_all_wx_pm_4G= ftp_task.loc[ftp_task['profession_code'].str.contains('WX') & ftp_task['ftp_type'].str.contains('PM') & ~ftp_task['network_type'].str.contains('5G')]

    # 核心
    table_success_hx_nrm_2G = table_ftp.loc[table_ftp['profession_code'].str.contains('HX') & table_ftp['ftp_type'].str.contains('NRM') & table_ftp['net_type'].str.contains('2G')]
    table_success_hx_pm_2G = table_ftp.loc[table_ftp['profession_code'].str.contains('HX') & table_ftp['ftp_type'].str.contains('PM') & table_ftp['net_type'].str.contains('2G')]
    table_success_hx_nrm = table_ftp.loc[table_ftp['profession_code'].str.contains('HX') & table_ftp['ftp_type'].str.contains('NRM') & table_ftp['net_type'].str.contains('4G')]
    table_success_hx_pm = table_ftp.loc[table_ftp['profession_code'].str.contains('HX') & table_ftp['ftp_type'].str.contains('PM') & table_ftp['net_type'].str.contains('4G')]
    table_success_hx_nrm_5G = table_ftp.loc[table_ftp['profession_code'].str.contains('HX') & table_ftp['ftp_type'].str.contains('NRM') & table_ftp['network_type'].str.contains('5G')]
    table_success_hx_pm_5G= table_ftp.loc[table_ftp['profession_code'].str.contains('HX') & table_ftp['ftp_type'].str.contains('PM') & table_ftp['network_type'].str.contains('5G')]
    table_success_hx_nrm_4G = table_ftp.loc[table_ftp['profession_code'].str.contains('HX') & table_ftp['ftp_type'].str.contains('NRM') & ~table_ftp['network_type'].str.contains('5G')]
    table_success_hx_pm_4G= table_ftp.loc[table_ftp['profession_code'].str.contains('HX') & table_ftp['ftp_type'].str.contains('PM') & ~table_ftp['network_type'].str.contains('5G')]

    table_all_hx_nrm_2G = ftp_task.loc[ftp_task['profession_code'].str.contains('HX') & ftp_task['ftp_type'].str.contains('NRM') & ftp_task['net_type'].str.contains('2G')]
    table_all_hx_pm_2G = ftp_task.loc[ftp_task['profession_code'].str.contains('HX') & ftp_task['ftp_type'].str.contains('PM') & ftp_task['net_type'].str.contains('2G')]
    table_all_hx_nrm = ftp_task.loc[ftp_task['profession_code'].str.contains('HX') & ftp_task['ftp_type'].str.contains('NRM') & ftp_task['net_type'].str.contains('4G')]
    table_all_hx_pm = ftp_task.loc[ftp_task['profession_code'].str.contains('HX') & ftp_task['ftp_type'].str.contains('PM') & ftp_task['net_type'].str.contains('4G')]
    table_all_hx_nrm_5G = ftp_task.loc[ftp_task['profession_code'].str.contains('HX') & ftp_task['ftp_type'].str.contains('NRM') & ftp_task['network_type'].str.contains('5G')]
    table_all_hx_pm_5G= ftp_task.loc[ftp_task['profession_code'].str.contains('HX') & ftp_task['ftp_type'].str.contains('PM') & ftp_task['network_type'].str.contains('5G')]
    table_all_hx_nrm_4G = ftp_task.loc[ftp_task['profession_code'].str.contains('HX') & ftp_task['ftp_type'].str.contains('NRM') & ~ftp_task['network_type'].str.contains('5G')]
    table_all_hx_pm_4G= ftp_task.loc[ftp_task['profession_code'].str.contains('HX') & ftp_task['ftp_type'].str.contains('PM') & ~ftp_task['network_type'].str.contains('5G')]

    table_statistics.loc['2G无线网', '资源成功流程'] = len(table_success_wx_nrm_2G)
    table_statistics.loc['2G无线网', '资源总流程'] = len(table_all_wx_nrm_2G)
    table_statistics.loc['2G无线网', '资源成功占比'] = str(round(len(table_success_wx_nrm_2G) / len(table_all_wx_nrm_2G),2) * 100) + '%'
    table_statistics.loc['2G无线网', '性能成功流程'] = len(table_success_wx_pm_2G)
    table_statistics.loc['2G无线网', '性能总流程'] = len(table_all_wx_pm_2G)
    table_statistics.loc['2G无线网', '性能成功占比'] = str(round(len(table_success_wx_pm_2G)/len(table_all_wx_pm_2G),2) * 100) + '%'

    table_statistics.loc['2G核心网', '资源成功流程'] = len(table_success_hx_nrm_2G)
    table_statistics.loc['2G核心网', '资源总流程'] = len(table_all_hx_nrm_2G)
    table_statistics.loc['2G核心网', '资源成功占比'] = str(round(len(table_success_hx_nrm_2G)/len(table_all_hx_nrm_2G),2) * 100) + '%'
    table_statistics.loc['2G核心网', '性能成功流程'] = len(table_success_hx_pm_2G)
    table_statistics.loc['2G核心网', '性能总流程'] = len(table_all_hx_pm_2G)
    table_statistics.loc['2G核心网', '性能成功占比'] = str(round(len(table_success_hx_pm_2G)/len(table_all_hx_pm_2G),2) * 100) + '%'

    table_statistics.loc['4G无线网', '资源成功流程'] = len(table_success_wx_nrm_4G)
    table_statistics.loc['4G无线网', '资源总流程'] = len(table_all_wx_nrm_4G)
    table_statistics.loc['4G无线网', '资源成功占比'] = str(round(len(table_success_wx_nrm_4G)/len(table_all_wx_nrm_4G),2) * 100) + '%'
    table_statistics.loc['4G无线网', '性能成功流程'] = len(table_success_wx_pm_4G)
    table_statistics.loc['4G无线网', '性能总流程'] = len(table_all_wx_pm_4G)
    table_statistics.loc['4G无线网', '性能成功占比'] = str(round(len(table_success_wx_pm_4G)/len(table_all_wx_pm_4G),2) * 100) + '%'

    table_statistics.loc['4G核心网', '资源成功流程'] = len(table_success_hx_nrm_4G)
    table_statistics.loc['4G核心网', '资源总流程'] = len(table_all_hx_nrm_4G)
    table_statistics.loc['4G核心网', '资源成功占比'] = str(round(len(table_success_hx_nrm_4G)/len(table_all_hx_nrm_4G),2) * 100) + '%'
    table_statistics.loc['4G核心网', '性能成功流程'] = len(table_success_hx_pm_4G)
    table_statistics.loc['4G核心网', '性能总流程'] = len(table_all_hx_pm_4G)
    table_statistics.loc['4G核心网', '性能成功占比'] = str(round(len(table_success_hx_pm_4G)/len(table_all_hx_pm_4G),2) * 100) + '%'

    table_statistics.loc['5G无线网', '资源成功流程'] = len(table_success_wx_nrm_5G)
    table_statistics.loc['5G无线网', '资源总流程'] = len(table_all_wx_nrm_5G)
    table_statistics.loc['5G无线网', '资源成功占比'] = str(round(len(table_success_wx_nrm_5G)/len(table_all_wx_nrm_5G),2) * 100) + '%'
    table_statistics.loc['5G无线网', '性能成功流程'] = len(table_success_wx_pm_5G)
    table_statistics.loc['5G无线网', '性能总流程'] = len(table_all_wx_pm_5G)
    table_statistics.loc['5G无线网', '性能成功占比'] = str(round(len(table_success_wx_pm_5G)/len(table_all_wx_pm_5G),2) * 100) + '%'

    table_statistics.loc['5G核心网', '资源成功流程'] = len(table_success_hx_nrm_5G)
    table_statistics.loc['5G核心网', '资源总流程'] = len(table_all_hx_nrm_5G)
    table_statistics.loc['5G核心网', '资源成功占比'] = str(round(len(table_success_hx_nrm_5G)/len(table_all_hx_nrm_5G),2) * 100) + '%'
    table_statistics.loc['5G核心网', '性能成功流程'] = len(table_success_hx_pm_5G)
    table_statistics.loc['5G核心网', '性能总流程'] = len(table_all_hx_pm_5G)
    table_statistics.loc['5G核心网', '性能成功占比'] = str(round(len(table_success_hx_pm_5G)/len(table_all_hx_pm_5G),2) * 100) + '%'


    # 告警
    # 无线
    table_alarm_success_wx_2G = table_alarm.loc[table_alarm['profession_code'].str.contains('WX') & table_alarm['network_type'].str.contains('2G')]
    table_alarm_success_wx_4G = table_alarm.loc[table_alarm['profession_code'].str.contains('WX') & table_alarm['network_type'].str.contains('4G')]
    table_alarm_success_wx_5G = table_alarm.loc[table_alarm['profession_code'].str.contains('WX') & table_alarm['network_type'].str.contains('5G')]
    table_alarm_all_wx_2G = alarm_task.loc[alarm_task['profession_code'].str.contains('WX') & alarm_task['network_type'].str.contains('2G')]
    table_alarm_all_wx_4G = alarm_task.loc[alarm_task['profession_code'].str.contains('WX') & alarm_task['network_type'].str.contains('4G')]
    table_alarm_all_wx_5G = alarm_task.loc[alarm_task['profession_code'].str.contains('WX') & alarm_task['network_type'].str.contains('5G')]
    # 核心
    table_alarm_success_hx_2G = table_alarm.loc[table_alarm['profession_code'].str.contains('HX') & table_alarm['network_type'].str.contains('2G')]
    table_alarm_success_hx_4G = table_alarm.loc[table_alarm['profession_code'].str.contains('HX') & table_alarm['network_type'].str.contains('4G')]
    table_alarm_success_hx_5G = table_alarm.loc[table_alarm['profession_code'].str.contains('HX') & table_alarm['network_type'].str.contains('5G')]
    table_alarm_all_hx_2G = alarm_task.loc[alarm_task['profession_code'].str.contains('HX') & alarm_task['network_type'].str.contains('2G')]
    table_alarm_all_hx_4G = alarm_task.loc[alarm_task['profession_code'].str.contains('HX') & alarm_task['network_type'].str.contains('4G')]
    table_alarm_all_hx_5G = alarm_task.loc[alarm_task['profession_code'].str.contains('HX') & alarm_task['network_type'].str.contains('5G')]


    table_statistics.loc['2G无线网', '告警成功流程'] = int(table_alarm_success_wx_2G['ip数量'].sum())
    table_statistics.loc['2G无线网', '告警总流程'] = int(table_alarm_all_wx_2G['ip数量'].sum())
    table_statistics.loc['2G无线网', '告警成功占比'] = str(round(table_statistics.loc['2G无线网', '告警成功流程'] / table_statistics.loc['2G无线网', '告警总流程'], 2) * 100) + '%'
    table_statistics.loc['2G核心网', '告警成功流程'] = int(table_alarm_success_hx_2G['ip数量'].sum())
    table_statistics.loc['2G核心网', '告警总流程'] = int(table_alarm_all_hx_2G['ip数量'].sum())
    table_statistics.loc['2G核心网', '告警成功占比'] = str(round(table_statistics.loc['2G核心网', '告警成功流程'] / table_statistics.loc['2G核心网', '告警总流程'], 2) * 100) + '%'
    table_statistics.loc['4G无线网', '告警成功流程'] = int(table_alarm_success_wx_4G['ip数量'].sum())
    table_statistics.loc['4G无线网', '告警总流程'] =  int(table_alarm_all_wx_4G['ip数量'].sum())
    table_statistics.loc['4G无线网', '告警成功占比'] = str(round(table_statistics.loc['4G无线网', '告警成功流程'] / table_statistics.loc['4G无线网', '告警总流程'], 2) * 100) + '%'
    table_statistics.loc['4G核心网', '告警成功流程'] =  int(table_alarm_success_hx_4G['ip数量'].sum())
    table_statistics.loc['4G核心网', '告警总流程'] =  int(table_alarm_all_hx_4G['ip数量'].sum())
    table_statistics.loc['4G核心网', '告警成功占比'] = str(round(table_statistics.loc['4G核心网', '告警成功流程'] / table_statistics.loc['4G核心网', '告警总流程'], 2) * 100) + '%'
    table_statistics.loc['5G无线网', '告警成功流程'] =  int(table_alarm_success_wx_5G['ip数量'].sum())
    table_statistics.loc['5G无线网', '告警总流程'] =  int(table_alarm_all_wx_5G['ip数量'].sum())
    table_statistics.loc['5G无线网', '告警成功占比'] = str(round(table_statistics.loc['5G无线网', '告警成功流程'] / table_statistics.loc['5G无线网', '告警总流程'], 2) * 100) + '%'
    table_statistics.loc['5G核心网', '告警成功流程'] =  int(table_alarm_success_hx_5G['ip数量'].sum())
    table_statistics.loc['5G核心网', '告警总流程'] =  int(table_alarm_all_hx_5G['ip数量'].sum())
    table_statistics.loc['5G核心网', '告警成功占比'] = str(round(table_statistics.loc['5G核心网', '告警成功流程'] / table_statistics.loc['5G核心网', '告警总流程'], 2) * 100) + '%'
```
### 将结果表输出为excel并保存
```python
    writer=pd.ExcelWriter('每日进展统计_'+check_time+'.xlsx')
    ftp_db_task_shallow.to_excel(writer,index=False, header=True,sheet_name='ftp_db_task_ip拆分')
    ftp_db_task.to_excel(writer,index=False, header=True,sheet_name='ftp_db_task')
    alarm_task.to_excel(writer,index=False, header=True,sheet_name='alarm_task')
    table_NRM_real.to_excel(writer,index=False, header=True,sheet_name='table_NRM_real')
    table_PM_real.to_excel(writer,index=False, header=True,sheet_name='table_PM_real')
    table_alarm.to_excel(writer,index=False, header=True,sheet_name='table_alarm')
    #gsm_alarm.to_excel(writer,index=False, header=True,sheet_name='gsm_alarm')
    real_all_event_shallow.to_excel(writer,index=False, header=True,sheet_name='real_ip拆分')
    ftp_db_false.to_excel(writer,index=False, header=True,sheet_name='file-db_histfalse')
    ftp_4G_false.to_excel(writer,index=False, header=True,sheet_name='new_4G_false')
    gsm_false.to_excel(writer,index=False, header=True,sheet_name='new_gsm_false')
    table_alarm_false.to_excel(writer,index=False, header=True,sheet_name='table_alarm_false')
    table_statistics.to_excel(writer,index=True, header=True,sheet_name='进展统计')
    writer.save()
```
### 利用SMTP自动发送邮件
```python
from email import encoders
from email.header import Header
from email.mime.text import MIMEText
from email.utils import parseaddr, formataddr
from email.mime.multipart import MIMEMultipart 
from email.mime.application import MIMEApplication
from email.mime.base import MIMEBase 
import smtplib
import base64
import time, datetime
from datetime import datetime,timedelta

def _format_addr(s):
    name, addr = parseaddr(s)
    return formataddr(( \
        Header(name, 'utf-8').encode(), \
        addr))


from_addr = 'email' #发件人邮箱

password = base64.b64decode(b'TGlseTE5OTMhITU=')    #base64解码
password = str(password,encoding ="utf-8")

to_addr = ['email'] #收件人邮箱

smtp_server ='host ip' #发邮件的服务器IP


msg = MIMEMultipart()


msg['From'] = _format_addr(u'发件人姓名 <%s>' % from_addr)
#msg['To'] = _format_addr(u'cairenqing <%s>' % to_addr) #单人邮件使用
msg['To'] = Header(",".join(to_addr))    #多人邮件使用
msg['Subject'] = Header(u'每日流程统计', 'utf-8').encode()

check_time=time.strftime('%Y%m%d',time.localtime(time.time()))

msg.attach(MIMEText('大家好： \n    附件为'+check_time+'采集流程统计 \n    请各位查收~', 'plain', 'utf-8'))


part_1 = MIMEApplication(open('每日进展统计_'+check_time+'.xlsx','rb').read())
part_1.add_header('Content-Disposition', 'attachment', filename = ('gbk','','每日进展统计_'+check_time+'.xlsx') )
msg.attach(part_1)

part_2 = MIMEApplication(open('redis_queue_'+check_time+'.txt','rb').read())
part_2.add_header('Content-Disposition', 'attachment', filename = ('gbk','','redis_queue_'+check_time+'.txt') )
msg.attach(part_2)



server = smtplib.SMTP(smtp_server, 18465)
server.set_debuglevel(1)
server.login(from_addr, password)
#server.sendmail(from_addr, [to_addr], msg.as_string())  #单人邮件使用
server.sendmail(from_addr, to_addr, msg.as_string())  #多人邮件使用
server.quit()
```

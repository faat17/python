# 利用python实现自动化数据统计的脚本
## 项目介绍
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



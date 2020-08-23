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


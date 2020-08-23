#coding=utf-8
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


msg['From'] = _format_addr(u'张亚男 <%s>' % from_addr)
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

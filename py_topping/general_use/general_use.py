import email, smtplib, ssl
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from datetime import datetime
import os
import csv
import re

class email_sender :
    def __init__(self, user, password, servername):
        """Store username , password and servername"""
        self.servername = servername
        self.password = password
        self.sender = user
        
    def executor(self,recipients, message ):    
        """Connect to Server and Send Email, will return status of sending in text""" 
        #Create Connection
        smtpObj = smtplib.SMTP(self.servername, timeout = 30)    
        smtpObj.login(self.sender.split('@')[0], self.password)
        ssl.create_default_context()
        try :
            #Send and Quit
            smtpObj.sendmail(self.sender, recipients, message.as_string())
            smtpObj.quit()
            return 'Send OK'
        except Exception as e:
            #Quit if error
            smtpObj.quit()
            return str(e)
        
    def send_email(self, sendto,subject,text_in):
        """Create Message Part"""
        #split for multple
        recipients = sendto.split(";")

        #Create Head of Email
        message = MIMEMultipart()
        message["From"] = self.sender
        message["To"] =  ', '.join(recipients)
        message["Subject"] = subject

        #Create Message Part
        html = """<html><body><p>{}</p></body></html>""".format(text_in)

        #Attach before send
        partHTML = MIMEText(html, 'html')
        message.attach(partHTML)

        #Sending Email and return status text
        return self.executor(recipients, message)

def log_csv(file_name,msg_in):
    logic = True
    if os.path.exists(file_name) : logic = False
    if '/' in file_name : 
        if file_name.split('/')[0] != '' :
            print(file_name.split('/')[0])
            dir_name = re.sub('(/.*\..*)','',file_name)
            if not os.path.isdir(dir_name) : os.mkdir(dir_name)
    with open(file_name.replace('/',''), 'a', newline='') as csvfile:
        fieldnames = ['Timestamp','msg']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if logic : writer.writeheader()
        writer.writerow({'Timestamp':str(datetime.now()),'msg':msg_in})
import email, smtplib, ssl
from email import encoders
from email.message import EmailMessage
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
import mimetypes
from datetime import datetime
import os , csv , re, socket, requests

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

    def add_file(self, message, attachment_path) :
        if os.path.exists(attachment_path):    
            with open(attachment_path, "rb") as attachment:
                # Add file as application/octet-stream
                # Email client can usually download this automatically as attachment
                base_attach = MIMEBase("application", "octet-stream")
                base_attach.set_payload(attachment.read())
            # Encode file in ASCII characters to send by email    
            encoders.encode_base64(base_attach)
            # Add header as key/value pair to attachment part
            base_attach.add_header(
                "Content-Disposition",
                f"attachment; filename= "+ attachment_path.split('/')[-1],)        
            # Add attachment to message and convert message to string
            message.attach(base_attach)
        return message

    def send(self, sendto,subject,text_in , attachment = None):
        """Create Message Part"""
        #split for multple
        recipients = sendto.split(";")

        #Create Head of Email
        message = MIMEMultipart()
        #message = EmailMessage()
        message["From"] = self.sender
        message["To"] =  ', '.join(recipients)
        message["Subject"] = subject

        #Create Message Part
        html = """<html><body><p>{}</p></body></html>""".format(text_in)
        #Attach before send
        partHTML = MIMEText(html, 'html')
        message.attach(partHTML)

        if attachment == None : pass
        elif type(attachment) != list : raise Exception("attachment must be a list")
        else : 
            for i in attachment : 
                message = self.add_file(message , i)
        #Sending Email and return status text
        return self.executor(recipients, message)

class lazy_LINE :
    def __init__(self, token, timeout = 60) :
        self.token = token
        self.timeout = timeout
    
    def help(self) :
        helper = 'Main Page : https://notify-bot.line.me/en\n'
        helper += 'Doc : https://notify-bot.line.me/doc/en\n'
        helper += 'Sticker List : https://developers.line.biz/en/docs/messaging-api/sticker-list'
        print(helper)

    def send(self, message , stickerPackageId = '', stickerId = '', notification = True, picture = '' , timeout = None) :
        if timeout == None : timeout = self.timeout
        payload = {'message' : message , 'notificationDisabled' : not notification}

        if (stickerPackageId != '') & (stickerId != '') :
            payload['stickerPackageId'] = stickerPackageId
            payload['stickerId'] = stickerId
        
        if picture == '' : 
            r = requests.post('https://notify-api.line.me/api/notify'
                                , headers={'Authorization' : 'Bearer {}'.format(self.token)}
                                , params = payload, timeout = timeout)
        else :
            r = requests.post('https://notify-api.line.me/api/notify'
                                , headers={'Authorization' : 'Bearer {}'.format(self.token)}
                                , params = payload , timeout = timeout
                                , files = {'imageFile': open(picture, 'rb')})
        return r

def log_csv(file_name,msg_in):
    logic = True
    if os.path.exists(file_name) : logic = False
    if '/' in file_name : 
        if file_name.split('/')[0] != '' :
            dir_name = re.sub('(/.*\..*)','',file_name)
            if not os.path.isdir(dir_name) : os.mkdir(dir_name)
    with open(file_name, 'a', newline='') as csvfile:
        fieldnames = ['Timestamp','msg']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        if logic : writer.writeheader()
        writer.writerow({'Timestamp':str(datetime.now()),'msg':msg_in})

def check_port(ip = None, port = None):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    if ip == None : ip = input("Please Input IP :")
    if port == None : port = input("Please Input port :")

    location = (str(ip), int(port))
    r = sock.connect_ex(location)
    if r == 0:
        print("Port is open")
    else:
        print("Port is not open")

def check_utc(utc_target) :
    current_utc = (datetime.now() - datetime.utcnow())
    current_utc = current_utc.total_seconds()/3600
    return int(round(utc_target - current_utc,0))

def healthcheck(cpu_interval = 1):
    import shutil, psutil
    pc_name = str(socket.gethostname())
    pc_ip = str(socket.gethostbyname(socket.gethostname()))
    du = shutil.disk_usage("/")
    rom_use = round(100 - du.free/du.total*100,1)
    cpu_use = psutil.cpu_percent(cpu_interval)
    ram_use = psutil.virtual_memory().percent

    return {'hostname' : [pc_name] ,
             'hostip' : [pc_ip],
             'cpu_use': [cpu_use], 
             'ram_use': [ram_use],
             'rom_use': [rom_use]}
import paramiko

class lazy_sftp_connect :
    """SFTP connection for lazy_SFTP"""
    def __init__(self, host_name, username, password, port) :
        self.host_name, self.username, self.password, self.port = host_name, username, password,port
        
    def __enter__(self) : 
        self.transport = paramiko.Transport((self.host_name,self.port))
        # Auth    
        self.transport.connect(None,self.username,self.password)
        # Go!    
        self.sftp = paramiko.SFTPClient.from_transport(self.transport)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) :
        if self.sftp: self.sftp.close()
        if self.transport: self.transport.close()

            
class lazy_SFTP :   
    """lazy_SFTP to interact with SFTP server"""
    def __init__(self, host_name, username, password, port = 22) :
        self.host_name, self.username, self.password, self.port = host_name, username, password,port
        self.con = lazy_sftp_connect(  host_name = self.host_name 
                                     , username = self.username
                                     , password = self.password
                                     , port = self.port)
        
    def listdir(self, path='.') :
        with self.con as connect :
            out = connect.sftp.listdir(path=path)
        return out
    
    def download(self, remotepath, localpath) :
        with self.con as connect :
            connect.sftp.get(localpath = localpath, remotepath = remotepath)

    def upload(self, remotepath, localpath) :
        with self.con as connect :
            connect.sftp.put(localpath = localpath, remotepath = remotepath)
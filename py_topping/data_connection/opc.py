from opcua import Client

class lazy_OPCUA_connection :
    def __init__(self, client) :
        self.client = client
    
    def __enter__(self) : 
        self.client.connect()
        return self.client

    def __exit__(self, exc_type, exc_val, exc_tb) :
        self.client.disconnect()

class lazy_OPCUA :
    def __init__(self, url, user = '', password = '') :
        self.client = Client(url)
        if user != '' : self.client.set_user(user)
        if password != '' : self.client.set_password(password)
        self.con = lazy_OPCUA_connection(self.client)
   
    def read(self, node, full_value = False) :
        if type(node) == list : multiple_node, out = True, []
        else : multiple_node = False
        with self.con as con :
            if multiple_node :
                for i in node :
                    var = con.get_node(i)
                    if full_value : out.append(var.get_data_value())
                    else : out.append(var.get_value())
            else : 
                var = con.get_node(node)
                if full_value : out = var.get_data_value()
                else : out = var.get_value()
        return out
    
    def write(self, node, value) :
        if (type(node) == list) & (type(value) == list) : 
            assert len(node) == len(value), "Number of nodes and values must be to same!!!"
            multiple_node, out = True, []
        else : multiple_node = False
        with self.con as con :
            if multiple_node :
                for i in node :
                    var = con.get_node(i)
                    var.set_value(value)
                    out.append(value)
            else : 
                var = con.get_node(node)
                var.set_value(value)
        return value

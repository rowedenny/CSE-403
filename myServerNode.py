from configparser import ConfigParser
import socket
import sys
import json
from threading import Thread, Lock 
from myDHTTable import myDHTTable

MAX_DATA_SIZE = 2048

class myServer:
    def __init__(self, name, conf_file):
        """ _dht_table : the local dht table
            _lookup_table: the local lookup table to know the mapping from key to node
        """
        self.name = name
        self.nb_put = 0
        self.connections = []
        self.__lock = Lock()
        self.__dht_table = myDHTTable(self.name)
        
        self.read_config(conf_file)
        self.build_connections()
        
    def read_config(self, conf_file):
        self.config = {}
        cf = ConfigParser()
        cf.read(conf_file)

        for section in cf.sections():
            node_name = section
            self.config[node_name] = cf.items(section)

    
    def build_connections(self, ):
        self.socket_pool = {}
        for node_name, config in self.config.items():
            self.socket_pool[node_name] = None
            
            server_host = config[0][1]
            server_port = int(config[1][1])

            if node_name == self.name:
                # listening 
                self.socket_pool[node_name] = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.socket_pool[node_name].setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
                self.socket_pool[node_name].bind((server_host, server_port))
                self.socket_pool[node_name].listen(5)
                print('starting up in {} on {} on {}'.format(self.name, server_host, server_port))
    
    def find_server(self, key):
        """ Look up the net_config and to check whether 'key' is located at local server
        """
        for node_name, config in self.config.items():
            host = config[0][1]
            port = int(config[1][1])
            mod_value = int(config[2][1])

            if int(key) % len(self.config.items()) == mod_value:
                return node_name, host, port
    
    def operate(self, operation, key, value = None):
        """ Operate the "operation" on this server with <key, value>
               directly operate the _dht_table with <key, value>
        """
        br, bd = self.__dht_table.operate(operation, key, value)
        return br, bd

    def handler(self, connection, address):
        while True:
            data = connection.recv(MAX_DATA_SIZE)

            if not data:
                break
            # decode data
            b = b''
            b += data
            d = json.loads(b.decode("utf-8"))
            print("request is", d)

            key_at_server, server_host, server_port = self.find_server(d['key'])
            message = {}
            if key_at_server == self.name:
                self.__lock.acquire()
                br, bd = self.operate(d['op'], d['key'], d['value'])
                self.__lock.release()
                if br == True:
                    message['ret'] = bd
                    message['status'] = 'success'
                    if d['op'] == 'put':
                        print('[INFO]: current successful puts number is ', self.__dht_table.nb_put)
                else:
                    message['ret'] = None
                    message['status'] = 'fail'
                message = json.dumps(message).encode('utf-8')
            else:
                self.__lock.acquire()
                if self.socket_pool[key_at_server] is None:
                    self.socket_pool[key_at_server] = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    self.socket_pool[key_at_server].setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
                    self.socket_pool[key_at_server].connect((server_host, server_port))
                    print('buid new connection with server :', (server_host, server_port))
                self.socket_pool[key_at_server].sendall(data)
                print('transmit request {} to {}'.format(data, key_at_server))
                message = self.socket_pool[key_at_server].recv(MAX_DATA_SIZE)
                print('transmit response:', message)
                self.__lock.release()

            connection.sendall(message)
            print('result send', message)

    def start_server(self):
        """ Starts a new `server_thread` for new clients
        """
        while True:
            connection, address = self.socket_pool[self.name].accept()
            print("connection from", connection)
            
            new_thread = Thread(target = self.handler,
                                args = (connection, address, ))
            new_thread.daemon = True
            new_thread.start()
            self.connections.append(connection)
            print(self.connections)

if __name__ == "__main__":
    ser1 = myServer(sys.argv[1], "./net_config")
    ser1.start_server()


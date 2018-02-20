import numpy as np
import sys
import os
import json
import time
import threading
import socket
from configparser import ConfigParser

MAX_DATA_SIZE = 2048

class myClient:
    def __init__(self, name, conf_file):
        """ 
        """
        self.name = name
        self.nb_put = 0
        
        self.read_config(conf_file)
        self.build_connections()
        
        self.__sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.__sock.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
    
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

            send_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            send_socket.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
            
            server_host = config[0][1]
            server_port = int(config[1][1])

            send_socket.connect((server_host, server_port))
            self.socket_pool[node_name] = send_socket
            print('buid connection with server :', (server_host, server_port))

    def run(self, operation, key, value = None):
        """ Randomly select a server to request operation 
            Request the "server" server_address to execute "operation" with <key, value>
        """
        message = {}
        message['op'] = operation
        message['key'] = key
        message['value'] = value

        target_server = np.random.choice(list(self.socket_pool.keys()), 1)[0]

        target_server = 'node1'

        self.socket_pool[target_server].sendall(json.dumps(message).encode('utf-8'))
        print("request sent to {}: {}".format(target_server, message))
        response = self.socket_pool[target_server].recv(MAX_DATA_SIZE)
        print("response:", response)
        
        b = b''
        b += response
        d = json.loads(b.decode('utf-8'))

        if operation == 'put' and d['status'] == 'success':
            self.nb_put += 1
            print('Currently successful put number is ', self.nb_put)

    def summary(self, ):
        print ('total successful put number is ', self.nb_put)
    def close(self,):
        for key in self.socket_pool.keys():
            self.socket_pool[key].close()

if __name__ == "__main__":
    key_range = 1000

    app = myClient("client", './net_config')
    for i in np.arange(0, 100):
        key = np.random.randint(0, key_range)
        value = None

        if np.random.random() <= 0.4:
            op = 'put'
            value = np.random.randint(10000)
        else:
            op = 'get'

        app.run(op, key, value)

        #time.sleep(5)
    app.summary()
    app.close()

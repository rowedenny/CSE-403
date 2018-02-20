import threading

class myDHTTable:
    """ The distributed hash table
        We use dictionary with put and get methods
    """
    def __init__(self, server_name, ):
        """ _map : the locally partitioned DHT
            _lock: a thread level locking to prevent the concurrently 
        """ 
        self._map = {}
        self.nb_put = 0
        self.nb_get = 0
        self.server_name = server_name

    def put(self, key, value):
        """ If key is not in _map yet, add <key, value> to _map, otherwise return False
        """
        ret = False
        if key not in self._map:
            self._map[key] = value
            ret = True
            self.nb_put += 1

        return ret

    def get(self, key):
        """ If key is already in _map, return its value, otherwise return None
        """
        value = None
        if key in self._map:
            value = self._map[key]
            self.nb_get += 1

        return value

    def operate(self, operation, key, value = None):
        if operation == "put":
            ret = self.put(key, value)
            if ret is True:
                return True, value
            else:
                return False, None
        elif operation == "get":
            ret = self.get(key)
            if ret is not None:
                return True, ret
            else:
                return False, None


import sys
import socket


"""
    1. Connects to his CP Enginge and authenticates it to the server (THROUGH SOCKETS)
    1,5. (Sets the id for the Engine - how should enginge know otherwise which requests are for it, location?)
    2 Repeatedly checks the status and sends it to the server in critical case ( THROUGH SOCKETS)

"""
def main():
    SERVER_ENGINE = sys.argv[1]
    PORT_ENGINE = int(sys.argv[2])
    ADDR_ENGINE = (SERVER_ENGINE, PORT_ENGINE)
    SERVER_CENTRAL = sys.argv[3]
    PORT_CENTRAL = sys.argv[4]
    ADDR_CENTRAL = (SERVER_CENTRAL, PORT_CENTRAL)

    monitor = Monitor(
        ADDR_CENTRAL,
        ADDR_ENGINE,
        sys.args[3]
    )
    monitor.start()

class Monitor:

    def __init__(self, addr_central, addr_engine, cp_id) :
        self._addr_central = addr_central
        self._addr_egnine = addr_engine
        self._cp_id = cp_id

    @property
    def ip_central(self):
        return self.addr_central
    
    @property
    def ip_engine(self):
        return self.adr_engine
    
    @property
    def cp_id(self):
        return self.cp_id
    
    def start(self):
        central_client, enginge_client = self.authenticate()
        self.run(central_client, enginge_client)

    def authenticate(self):
        # TODO("implement authentication protocol")
        engine_client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        engine_client.connect(self._addr_engine)

        central_client = client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        central_client.connect(self._addr_central)

        return(central_client, engine_client)
    
    def run(self, central_client, engine_client):
        
        # TODO("implement protocol")
        while(True):
            # TODO("wait with a timeout?")
            status = engine_client.recv()
            central_client.send(status)
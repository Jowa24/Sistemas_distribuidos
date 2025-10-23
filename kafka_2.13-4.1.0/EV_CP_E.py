import asyncio
import time
import threading
import socket
from kafka import KafkaProducer
from kafka import KafkaConsumer
import sys
from EV_Utile import InformationMessage, InformationTypeEngine
from EV_Utile import ChargingPointStatus


HEADER = 64
FORMAT = 'utf-8'


# simply build the object and starts it with the passed arguments
def main() :
    engine = Engine(
        sys.args[1],
        sys.args[2],
        sys.args[3],
        sys.args[4],
        sys.args[5]
    )
    engine.run()


class Engine:

    def __init__(self, cp_id, location, price_kw, broker_port, socket_addr):
        self._id = cp_id
        self._location = location
        self._price_kw = price_kw 
        self._consumer = KafkaConsumer(
            'Central-CP',
            bootstrap_servers= broker_port,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='my-group'
        )
        self._producer = KafkaProducer(bootstrap_servers= broker_port)
        self._power = 11
        self._ADDR = socket_addr
        self._status = ChargingPointStatus.DISCONNECTED


    @property
    def id(self):
        return self._id

    @property
    def location(self):
        return self._location

    @property
    def status(self):
        return self._status
    
    @status.setter
    def status(self, new_status):
        self._status = new_status

    @property
    def price_kw(self):
        return self._price_kw

    @property
    def power(self):
        return self._power
    
    @property
    def socket_ip(self) :
        return self._ADDR

    @property
    def consumer(self):
        return self._consumer

    @property
    def producer(self):
        return self._producer

    @property
    def broker_port(self):
        return self._broker_port
    
    
    def run_engine(self) :

        threading.Thread( args=self, target=self.charging_engine)
        threading.Thread( args=self, target=self.send_status)
        threading.Thread( args=self, target=self.user_input)
    
    # simulates the engine awaiting requests and only stops on userinput
    def charging_engine(self) :
        
        # [request_id] [driver_id] [cp_id] [location]
        for request in self.consumer:

            # check if enginge crashed
            if( self.status == ChargingPointStatus.DEFECT) :
                break
            
            msg = request.value.decode('UTF-8')
            request_id, driver_id, cp_id, location = msg.split()

            if( cp_id == self._id) :
                self.start_charging(request_id)

    def send_status(self) :
        server = socket.socket(socket.AF_INET , socket.SOCK_STREAM)
        server.bind(self.ADDR)
        
    def run_server(self, server) : 
        # TODO("implement communication protocol")
        server.listen()
        while(self.status != ChargingPointStatus.DEFECT) :
            conn, addr = server.accept()
            thread = threading.Thread(target=self.handle_client, args=(conn, addr))
            thread.start()

    def handle_client(self, conn, addr) :
        # TODO("implement communication protocol")
        while(self.status != ChargingPointStatus.DEFECT) :
            msg_length = conn.recv(HEADER).decode(FORMAT)
            if msg_length:
                msg_length = int(msg_length)
                msg = conn.recv(msg_length).decode(FORMAT)
                if msg == "send_status":
                    conn.send(self.status.decode(FORMAT))

    def user_input(self):
        # TODO("think about how to stop and restart machine")
        while(True) :
            user_input = input("Enter 'end' to end charging, 'crash' to simulate a crash of the eninge and 'fix' to reboot the engine after a crash")
            if user_input.lower() == 'end':
                self.status(ChargingPointStatus.ACTIVE)
            if user_input.input.lower() == 'crash':
                self.status(ChargingPointStatus.DEFECT) # this will kill charging_enginge() and send_status()
            if user_input.input.lower() == 'fix' :
                self.reboot_engine()

    # TODO("ticket sending, in combination mit crash implementieren")
    def start_charging(self, request_id) :
        # send starting message
        start_time = time.perf_counter()
        message = self.to_start_message(request_id, start_time)     
        self.producer.send('CP-Central', message.encode('UTF-8'))
        
        # enter charging state
        self.status = ChargingPointStatus.IN_USAGE
        while(self.status == ChargingPointStatus.IN_USAGE) :
            time.sleep(1)

            current_time = time.perf_counter()
            message = self.to_ongoing_message(request_id, start_time, current_time)
            self.producer.send('Cp-Central', message.encode('UTF-8'))

    # uses the InformationMessage class to construct and return a clean string with the defined format
    def to_start_message(self, request_id, start_time) -> str:
        message = InformationMessage(
                request_id,
                InformationTypeEngine.CHARGING_START,
                self.id,
                start_time,
                start_time, # note that this will mean duration = 0 -> consumption = 0, price = 0
                self.price_kw,
                self.power,
            )
        return str(message)

    # uses the InformationMessage class to construct and return a clean string with the defined format
    def to_ongoing_message(self, request_id, start_time, current_time) -> str:
        message = InformationMessage(
                request_id,
                InformationTypeEngine.CHARGING_ONGOING,
                self.id,
                start_time,
                current_time,
                self.price_kw,
                self.power,
            )
        return str(message)


"""
    def reboot_engine(self) :
        threading.Thread(args=self, target=self.charging_engine)
        threading.Thread(args=self, target=self.send_status)
 
    #form:[requestId] [type] [cp_id] [time] [consumption] [price_kw]
    def get_message(self, type_name : str, request_id : int, start_time : float, time : float) -> str:

        type = InformationTypeEngine[type_name]

        match type:
            case InformationTypeEngine.CHARGING_START :
                return f"{request_id} {type} {self.id} {start_time} {0} {self.price_kw}" 
            case InformationTypeEngine.CHARGING_ONGOING :
                duration = self.get_duration(start_time, time)
                return f"{request_id} {type} {self.id} {start_time} {self.get_consumption(duration)} {self.price_kw}" 
            case InformationTypeEngine.CHARGING_END :
                duration = self.get_duration(start_time, time)
                return f"{request_id} {type} {self.id} {start_time} {self.get_consumption(duration)} {self.price_kw}" 
        
    def get_duration(self, start_time : float, time : float) -> float : 
        return time - start_time

    def get_consumption(self, duration : float) -> float :
        return duration * self.power
    
    def get_price(self, start_time, time) -> float :
        consumption = self.get_consumption(self.get_duration(start_time, time))
        return consumption * self.price_kw

"""

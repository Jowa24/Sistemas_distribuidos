
import threading
from kafka import KafkaProducer
from kafka import KafkaConsumer
import sys
from EV_Utile import RequestMessage


# TODO ("More print statements to inform ")
def main():

    cp_list = sys.arg[1]
    id = sys.arg[2]
    broker_ip = sys.args[3]

    driver = Driver(
        cp_list,
        id,
        broker_ip
    )
    driver.start()

class Driver:

    def __init__(self, id, cp_list, broker_ip):
        self.cp_list = cp_list
        self.id = id
        self.consumer = KafkaConsumer(
            'CP-Driver',
            bootstrap_servers= broker_ip,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='my-group'
        )
        self.producer = KafkaProducer(bootstrapserver= broker_ip)

    @property
    def id(self):
        return self.id

    @property
    def cp_list(self):
        return self.cp_list
    
    @property
    def producer(self):
        return self.producer
    
    @property
    def consumer(self):
        return self.consumer
    
    def start(self):
        request_thread = threading.Thread(targets=self.request)
        request_thread.start()
        
        receive_thread = threading.Thread(targets=self.receive)
        receive_thread.start()
    
    # TODO("Delay behaviour einbauen")
    def request(self):
        for entry in self.cp_list:
            request = self.get_request(entry)
            self.producer.send('Driver-CP', request.encode('UTF-()'))

    def receive(self):
        for message in self.consumer:
            print(message.value.decode('UTF8'))

    def get_request(self, cp_id):
        request = RequestMessage(
            cp_id,
            self.id
        )
        return str(request)
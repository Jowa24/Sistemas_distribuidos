import re
import sys
import threading
import time
from kafka import KafkaConsumer, KafkaProducer, TopicPartition

from EV_Utile import (
    InformationMessage,
    InformationTypeEngine,
    InformationTypeMonitor,
    RequestMessage,
    StatusMessage,
    Ticket,
)

# Must match Central
NUM_PARTITIONS = 7

def main():
    if len(sys.argv) < 4:
        print("Usage: python3 EV_DRIVER.py <cp_list_comma_separated> <driver_id> <broker_address>")
        sys.exit(1)

    cp_list_str = sys.argv[1]
    cp_list = cp_list_str.split(',')
    id = sys.argv[2]
    broker_ip = sys.argv[3]  # e.g., 'kafka:9092'

    driver = Driver(
        id,
        cp_list,
        broker_ip
    )
    driver.start()


class Driver:

    def __init__(self, id, cp_list, broker_ip):
        self._cp_list = cp_list
        self._id = id
        print(f"Driver {self._id} starting...")
        
        self._consumer = KafkaConsumer(
            bootstrap_servers=broker_ip,
            enable_auto_commit=True
        )
        partition = int(self._id) % NUM_PARTITIONS
        self._consumer.assign([TopicPartition('Central-Driver', partition)])
        print(f"Driver {self._id} assigned to partition {partition}")

        self._producer = KafkaProducer(bootstrap_servers=broker_ip)
        print(f"Driver {self._id} connected to Kafka at {broker_ip}")

    # --- Properties ---
    @property
    def id(self): return self._id
    @property
    def cp_list(self): return self._cp_list
    @property
    def producer(self): return self._producer
    @property
    def consumer(self): return self._consumer

    # --- Methods ---
    
    def start(self):
        request_thread = threading.Thread(target=self.run_sequential_requests)
        request_thread.start()

    def run_sequential_requests(self):
        for entry in self.cp_list:
            cp_id = entry.strip()
            if not cp_id:
                continue

            print(f"Driver {self.id}: Requesting CP {cp_id}")
            request = self.get_request(cp_id)
            
            self._producer.send('Driver-Central',
                                value=request.encode('utf-8'),
                                key=str(self.id).encode('utf-8'))
            self._producer.flush()

            self.receive()

            print(f"Driver {self.id}: Waiting 4 seconds before next request...")
            time.sleep(4)
        
        print(f"Driver {self.id}: Request list finished.")

    def receive(self):
        print(f"Driver {self.id}: Listening for session outcome (Ticket or Error)...")
        for message in self.consumer:
            msg_str = message.value.decode('utf-8')

            try:
                msg_obj = StatusMessage.decode_message(msg_str)
                if msg_obj.cp_id != self._last_requested_cp_id:
                    continue
                
                print(f"Driver {self.id} (STATUS): {str(msg_obj)}")

                if msg_obj.type == InformationTypeMonitor.ERROR:
                    print(f"Driver {self.id}: ERROR! Charging session failed: {msg_obj.status.name}")
                    return

                continue

            except (ValueError, TypeError):
                pass

            try:
                msg_obj = Ticket.decode_message(msg_str)
                print(f"Driver {self.id} (TICKET): {str(msg_obj)}")
                return  # <-- STOPS receive() and starts the 4s pause

            except (ValueError, TypeError):
                pass 
            
            try:
                msg_data = InformationMessage.decode_message(msg_str)

                if msg_data.get('type') == InformationTypeEngine.CHARGING_START.value:
                    self._last_request_id = msg_data.get('request_id')

                if msg_data.get('request_id') != self._last_request_id:
                    continue

                msg_obj = InformationMessage(**msg_data)

                match msg_obj.type:
                    case InformationTypeEngine.CHARGING_START:
                        print(f"Driver {self.id} (INFO): Charging {msg_obj.request_id} STARTED.")
                    case InformationTypeEngine.CHARGING_ONGOING:
                        print(f"Driver {self.id} (INFO): Update: Price={msg_obj.price:.2f} EUR, Consumption={msg_obj.consumption:.2f} kWh")
                    case InformationTypeEngine.CHARGING_END:
                        pass 
                
                continue

            except (ValueError, TypeError):
                pass

            pass

    def get_request(self, cp_id):
        self._last_requested_cp_id = cp_id
        self._last_request_id = None 
        
        request = RequestMessage(
            cp_id,
            self.id
        )
        return str(request)


if __name__ == "__main__":
    main()


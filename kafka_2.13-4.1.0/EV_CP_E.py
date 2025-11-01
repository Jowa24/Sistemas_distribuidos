import time
import threading
import socket
from kafka import KafkaProducer, KafkaConsumer, TopicPartition
import sys

from EV_Utile import (
    InformationMessage, InformationTypeEngine, RequestMessage,
    ChargingPointStatus, frame_message, SyncSocketFrameProtocol,
    NUM_PARTITIONS
)

FORMAT = 'utf-8'
# NUM_PARTITIONS is imported from EV_Utile

def main():
    if len(sys.argv) < 5:
        print("Usage: python3 EV_CP_E.py <cp_id> <price_kw> <broker_address_ip:port> <socket_address_ip:port>")
        sys.exit(1)
        
    cp_id = sys.argv[1]
    price_kw = float(sys.argv[2])
    broker_address = sys.argv[3]
    
    try:
        socket_ip, socket_port = sys.argv[4].split(':')
        socket_addr = (socket_ip, int(socket_port))
    except ValueError:
        print("Socket address format must be 'ip:port'")
        sys.exit(1)

    engine = Engine(
        cp_id,
        price_kw,
        broker_address,
        socket_addr
    )
    engine.run_engine()


class Engine:

    def __init__(self, cp_id, price_kw, broker_address, socket_addr):
        self._id = int(cp_id)
        self._price_kw = price_kw 
        
        self._consumer = KafkaConsumer(
            bootstrap_servers=broker_address,
            enable_auto_commit=True
        )
        # FIX: Partitioning logic
        partition = int(self._id) % NUM_PARTITIONS
        self._consumer.assign([TopicPartition('Central-CP', partition)])
        print(f"Engine {self._id} assigned to partition {partition}")

        self._producer = KafkaProducer(bootstrap_servers=broker_address)
        self._power = 11.0 # kW
        self._ADDR = socket_addr
        self._status = ChargingPointStatus.DISCONNECTED
        print(f"Engine {self._id} starting...")

    # --- Properties ---
    @property
    def id(self): return self._id
    @property
    def status(self): return self._status
    @status.setter
    def status(self, new_status):
        if self._status != new_status:
            self._status = new_status
            print(f"Engine {self.id}: Status changed to {self._status.name}")
    @property
    def price_kw(self): return self._price_kw
    @property
    def power(self): return self._power
    @property
    def socket_ip(self) : return self._ADDR
    @property
    def consumer(self): return self._consumer
    @property
    def producer(self): return self._producer
    
    # --- Methods ---
    
    def run_engine(self):
        self.status = ChargingPointStatus.ACTIVE
        
        thread_charging = threading.Thread(target=self.charging_engine, args=())
        thread_charging.start()
        
        thread_socket = threading.Thread(target=self.run_server, args=())
        thread_socket.start()
        
        thread_input = threading.Thread(target=self.user_input, args=())
        thread_input.start()
        print(f"Engine {self.id} is running.")
        
        # --- FIX: Wait on the input thread ---
        # This keeps the main thread alive (so the container runs)
        # and allows the user to type 'crash' or 'fix'.
        thread_input.join()
    
    def charging_engine(self):
        # NEW: Debug log
        partition = self._consumer.assignment().pop().partition
        print(f"Engine {self.id}: Charging engine listening for requests on 'Central-CP' (Partition {partition})...")
        
        for request in self.consumer:
            if( self.status == ChargingPointStatus.DEFECT):
                print(f"Engine {self.id}: Charging engine stopping due to DEFECT status.")
                break
            
            # NEW: Debug log
            print(f"\nEngine {self.id}: [ChargingEngine]: ======================================")
            print(f"Engine {self.id}: [ChargingEngine]: <-- Received message from 'Central-CP'")
            
            msg = request.value.decode('utf-8')
            # NEW: Debug log
            print(f"Engine {self.id}: [ChargingEngine]:    Payload: {msg}")
            
            try:
                # FIX: decode_message returns an object
                req_data = RequestMessage.decode_message(msg)
                request_id = req_data.request_id
                cp_id_req = req_data.cp_id
                # NEW: Debug log
                print(f"Engine {self.id}: [ChargingEngine]:    Decoded: Request {request_id} for CP {cp_id_req}")

                if( cp_id_req == self.id ):
                    # NEW: Debug log
                    print(f"Engine {self.id}: [ChargingEngine]:    Request is for me. Starting charge thread.")
                    charge_thread = threading.Thread(target=self.start_charging, args=(request_id,))
                    charge_thread.start()
                else:
                    # NEW: Debug log
                    print(f"Engine {self.id}: [ChargingEngine]:    Ignoring request (not for me).")
                
            except Exception as e:
                print(f"Engine {self.id}: [ChargingEngine]:    ERROR: Could not decode request message '{msg}': {e}")

    def run_server(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind(self._ADDR)
            server.listen()
            print(f"Engine {self.id}: Socket server listening on {self._ADDR}")
            
            while(self.status != ChargingPointStatus.DEFECT):
                conn, addr = server.accept()
                thread = threading.Thread(target=self.handle_client, args=(conn, addr))
                thread.start()
        except OSError as e:
            # This error is expected when 'crash' is called and the socket closes
            if self.status != ChargingPointStatus.DEFECT:
                print(f"Engine {self.id}: CRITICAL - Could not bind to socket {self._ADDR}. {e}")
        finally:
            server.close()
            print(f"Engine {self.id}: Socket server shut down.")

    def handle_client(self, conn: socket.socket, addr):
        """
        R4.3: Reads and sends framed socket messages.
        """
        print(f"Engine {self.id}: Monitor connected from {addr}")
        conn.settimeout(10.0)
        protocol = SyncSocketFrameProtocol(conn) # Protocol handler
        
        try:
            while(self.status != ChargingPointStatus.DEFECT):
                # FIX: Read using the protocol
                msg = protocol.read_message() 
                
                if msg == "GET_STATUS":
                    response = f"status={self.status.value}&price_kw={self.price_kw}"
                    # FIX: Send using the protocol
                    conn.sendall(frame_message(response))
                else:
                    conn.sendall(frame_message("ERR_UNKNOWN_CMD"))

        except (socket.timeout, ConnectionError, ConnectionResetError, BrokenPipeError):
            # These are expected when the monitor disconnects
            print(f"Engine {self.id}: Monitor {addr} disconnected.")
        except Exception as e:
            if self.status != ChargingPointStatus.DEFECT:
                print(f"Engine {self.id}: Error handling client {addr}: {e}")
        finally:
            conn.close()

    def user_input(self):
        while(True):
            try:
                user_input = input(f"Engine {self.id} Input ('end', 'crash', 'fix'): \n")
                if user_input.lower() == 'end':
                    if self.status == ChargingPointStatus.IN_USAGE:
                        self.status = ChargingPointStatus.ACTIVE
                    else:
                        print(f"Engine {self.id}: Not currently charging.")
                elif user_input.lower() == 'crash':
                    self.status = ChargingPointStatus.DEFECT
                    # Break loop to allow main thread to exit
                    break 
                elif user_input.lower() == 'fix':
                    if self.status == ChargingPointStatus.DEFECT:
                        print(f"Engine {self.id}: Rebooting...")
                        self.reboot_engine()
                    else:
                        print(f"Engine {self.id}: Not in DEFECT state.")
            except EOFError:
                # This happens when container STDIN is closed
                break
    
    def reboot_engine(self):
        self.status = ChargingPointStatus.ACTIVE
        # Restart the socket server thread
        thread_socket = threading.Thread(target=self.run_server, args=())
        thread_socket.start()
        print(f"Engine {self.id}: Reboot complete. Socket server restarted.")

    def start_charging(self, request_id):
        # NEW: Debug log
        print(f"Engine {self.id}: [ChargeThread {request_id}]: Starting charge...")
        start_time = time.time()
        message = self.to_start_message(request_id, start_time)
        
        # NEW: Debug log
        print(f"Engine {self.id}: [ChargeThread {request_id}]: --> Sending CHARGING_START to 'CP-Central'")
        # FIX: Send to Kafka partition based on CP ID
        self._producer.send('CP-Central', 
                            value=message.encode(FORMAT),
                            key=str(self.id).encode(FORMAT))
        
        self.status = ChargingPointStatus.IN_USAGE
        
        charge_duration_seconds = 10 
        end_time = start_time + charge_duration_seconds
        
        ongoing_message_sent = False # NEW: To reduce log spam
        
        while time.time() < end_time:
            if self.status != ChargingPointStatus.IN_USAGE:
                # NEW: Debug log
                print(f"Engine {self.id}: [ChargeThread {request_id}]: Charge interrupted by status change.")
                break
            time.sleep(1)
            current_time = time.time()
            message = self.to_ongoing_message(request_id, start_time, current_time)
            
            if not ongoing_message_sent:
                # NEW: Debug log
                print(f"Engine {self.id}: [ChargeThread {request_id}]: --> Sending (first) CHARGING_ONGOING to 'CP-Central'")
                ongoing_message_sent = True
                
            self._producer.send('CP-Central', 
                                value=message.encode(FORMAT),
                                key=str(self.id).encode(FORMAT))

        final_time = time.time()
        
        if self.status == ChargingPointStatus.DEFECT:
            print(f"Engine {self.id}: [ChargeThread {request_id}]: Charging stopped due to crash.")
        else:
            self.status = ChargingPointStatus.ACTIVE
            end_message = self.to_end_message(request_id, start_time, final_time)
            # NEW: Debug log
            print(f"Engine {self.id}: [ChargeThread {request_id}]: --> Sending CHARGING_END to 'CP-Central'")
            self._producer.send('CP-Central', 
                                value=end_message.encode(FORMAT),
                                key=str(self.id).encode(FORMAT))
            print(f"Engine {self.id}: [ChargeThread {request_id}]: Charging finished.")
            
        self._producer.flush()
        # NEW: Debug log
        print(f"Engine {self.id}: [ChargeThread {request_id}]: All messages flushed.")

    # --- Message Helper ---
    
    def to_start_message(self, request_id, start_time) -> str:
        message = InformationMessage(
                request_id=request_id,
                type=InformationTypeEngine.CHARGING_START,
                cp_id=self.id,
                start_time=start_time,
                current_time=start_time,
                price_kw=self.price_kw,
                cp_power=self.power,
            )
        return str(message)

    def to_ongoing_message(self, request_id, start_time, current_time) -> str:
        message = InformationMessage(
                request_id=request_id,
                type=InformationTypeEngine.CHARGING_ONGOING,
                cp_id=self.id,
                start_time=start_time,
                current_time=current_time,
                price_kw=self.price_kw,
                cp_power=self.power,
            )
        return str(message)
    
    def to_end_message(self, request_id, start_time, end_time) -> str:
        message = InformationMessage(
                request_id=request_id,
                type=InformationTypeEngine.CHARGING_END,
                cp_id=self.id,
                start_time=start_time,
                current_time=end_time,
                price_kw=self.price_kw,
                cp_power=self.power,
            )
        return str(message)

if __name__ == "__main__":
    main()
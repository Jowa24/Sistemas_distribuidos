import asyncio
import aiosqlite
import sqlite3
import sys
import threading
import time
from kafka import KafkaProducer, KafkaConsumer
# FIX: NewTopic is in kafka.admin
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

# Import all necessary classes from the latest Util file
from EV_Utile import (
    ChargingPointStatus,
    InformationMessage,
    InformationTypeEngine,
    InformationTypeMonitor,
    RequestMessage,
    StatusMessage,
    Ticket,
    AsyncSocketFrameProtocol,  # <-- R4.3 Socket Protocol
    NUM_PARTITIONS             # <-- Partitioning fix
)
import EV_DB_Setup

# Configuration for Monitor Timeouts (R3.2)
MONITOR_HEARTBEAT_TIMEOUT = 30  # 30 seconds without a message -> DISCONNECTED
MONITOR_CHECK_INTERVAL = 10     # Check for timeouts every 10 seconds

# FIX: Define the DB Path
DB_PATH = "db_data/EV.db"

def main():
    if len(sys.argv) < 3:
        print("Usage: python3 EV_Central.py <Broker_address_ip:port> <Listener_port>")
        sys.exit(1)
    
    broker_ip = sys.argv[1]       # e.g., 'kafka:9092'
    listener_port = int(sys.argv[2]) # e.g., 8080
    
    central = Central(
        broker_ip,
        listener_port
    )
    central.run_central()


class Central:

    def __init__(self, broker_ip, listener_port):
        print("Central starting...")
        self._broker_ip = broker_ip
        self._listener_port = listener_port
        
        # Stores the last heartbeat time for each monitor's CP_ID
        self._monitor_last_seen = {}

        print(f"Central connecting to Kafka at {self._broker_ip}")
        # FIX: Use broker_ip and unique group_ids
        self._request_consumer = KafkaConsumer(
            'Driver-Central',
            bootstrap_servers=self._broker_ip,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='central-driver-requests'
        )
        self._information_consumer = KafkaConsumer(
            'CP-Central',
            bootstrap_servers=self._broker_ip,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='central-cp-info'
        )
        self._request_producer = KafkaProducer(bootstrap_servers=self._broker_ip)
        self._information_producer = KafkaProducer(bootstrap_servers=self._broker_ip)

        self.createTopics()
        EV_DB_Setup.setUpDB() # This will now create the DB at DB_PATH
        print("Central setup complete.")


    def run_central(self):
        print("Central running...")
        
        # FIX: Start the asyncio event loop in a separate thread
        monitor_thread = threading.Thread(target=self.start_monitor_server)
        monitor_thread.start()
        
        request_thread = threading.Thread(target=self.requestHandler)
        request_thread.start()
        
        information_thread = threading.Thread(target=self.informationHandler)
        information_thread.start()
        
        # --- FIX: Wait on the monitor thread ---
        # This keeps the main thread alive, which keeps the container running.
        monitor_thread.join()

    
    def createTopics(self):
        admin_client = None
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=self._broker_ip,
                client_id='central-setup'
            )
            
            # FIX: Use NUM_PARTITIONS for partitioning
            topic_list = [
                NewTopic(name="Driver-Central", num_partitions=NUM_PARTITIONS, replication_factor=1),
                NewTopic(name="Central-Driver", num_partitions=NUM_PARTITIONS, replication_factor=1),
                NewTopic(name="Central-CP", num_partitions=NUM_PARTITIONS, replication_factor=1),
                NewTopic(name="CP-Central", num_partitions=NUM_PARTITIONS, replication_factor=1)
            ]

            admin_client.create_topics(new_topics=topic_list, validate_only=False)
            print("Topics created successfully!")
        except TopicAlreadyExistsError:
            print("Topics already exist, skipping creation.")
        except Exception as e:
            print(f"Topic creation failed (is Kafka running at {self._broker_ip}?): {e}")
        finally:
            if admin_client:
                admin_client.close()

    
    def requestHandler(self):
        print("Request handler running... Waiting for messages on 'Driver-Central'...")
        # FIX: Connect to the correct DB path
        connection = sqlite3.connect(DB_PATH)
        cursor = connection.cursor()
        
        try:
            for request in self._request_consumer:
                # NEW: Debug log
                print(f"\n[Central RequestHandler]: ==================================================")
                print(f"[Central RequestHandler]: <-- Received message from 'Driver-Central'")
                
                msg = request.value.decode('utf-8')
                # NEW: Debug log
                print(f"[Central RequestHandler]:    Payload: {msg}")
                
                try:
                    # FIX: decode_message returns an object
                    values = RequestMessage.decode_message(msg)
                    driver_id = values.driver_id
                    cp_id = values.cp_id
                    # NEW: Debug log
                    print(f"[Central RequestHandler]:    Decoded: Driver {driver_id} for CP {cp_id}")
                except Exception as e:
                    print(f"[Central RequestHandler]:    ERROR: Invalid request message, skipping: {e} -- MSG: {msg}")
                    continue

                if driver_id is None or cp_id is None:
                    print(f"[Central RequestHandler]:    ERROR: Invalid request, missing driver_id or cp_id: {msg}")
                    continue

                # NEW: Debug log
                print(f"[Central RequestHandler]:    Validating CP {cp_id}...")
                if (self.validate(cp_id, connection)):
                    # NEW: Debug log
                    print(f"[Central RequestHandler]:    Validation SUCCESS for CP {cp_id}")
                    
                    cursor.execute("SELECT MAX(request_id) FROM requests")
                    result = cursor.fetchone()
                    request_id = 1 if result is None or result[0] is None else result[0] + 1
                    # NEW: Debug log
                    print(f"[Central RequestHandler]:    Assigned new Request ID: {request_id}")

                    # FIX: DB schema is priceKW
                    cursor.execute("SELECT priceKW FROM charging_points WHERE id = ?", (cp_id,))
                    result = cursor.fetchone()
                    if result is None:
                        print(f"[Central RequestHandler]:    ERROR: Could not find price for cp_id {cp_id} after validation.")
                        continue 
                    price_kw = result[0]

                    # FIX: Explicit column names and 'done' = False
                    cursor.execute("""
                                INSERT INTO requests (request_id, cp_id, consumption, price, price_kw, driver_id, done)
                                VALUES(?, ?, ?, ?, ?, ?, ?)
                    """, (request_id, cp_id, 0.0, 0.0, price_kw, driver_id, False))
                    connection.commit()
                    # NEW: Debug log
                    print(f"[Central RequestHandler]:    Request {request_id} saved to DB.")

                    msg_with_request_id = RequestMessage(
                        cp_id, driver_id, 0, 0, price_kw, request_id
                    )
                    msg_str = str(msg_with_request_id)

                    # FIX: Send to specific partition required by EV_CP_E.py
                    target_partition = int(cp_id) % NUM_PARTITIONS
                    # NEW: Debug log
                    print(f"[Central RequestHandler]: --> Sending request {request_id} to 'Central-CP' (Partition {target_partition})")
                    self._request_producer.send('Central-CP', 
                                                value=msg_str.encode('utf-8'),
                                                partition=target_partition)
                    self._request_producer.flush()
                    # NEW: Debug log
                    print(f"[Central RequestHandler]:    Request {request_id} flushed.")

                else: 
                    # NEW: Debug log
                    print(f"[Central RequestHandler]:    Validation FAILED for CP {cp_id}. Request denied.")
                    # TODO: Send rejection message to driver
        except Exception as e:
            print(f"CRITICAL: requestHandler crashed: {e}")
        finally:
            connection.close()
            print("Request handler stopped.")

    
    def informationHandler(self):
        print("Information handler running... Waiting for messages on 'CP-Central'...")
        # FIX: Connect to the correct DB path
        connection = sqlite3.connect(DB_PATH)
        cursor = connection.cursor()
        
        try:
            for information in self._information_consumer:
                # NEW: Debug log
                print(f"\n[Central InfoHandler]: ==================================================")
                print(f"[Central InfoHandler]: <-- Received message from 'CP-Central'")
                
                msg = information.value.decode('utf-8')
                # NEW: Debug log
                print(f"[Central InfoHandler]:    Payload: {msg}")
                
                try:
                    # FIX: Create object from dict to get calculated fields
                    values = InformationMessage.decode_message(msg)
                    if values.get(InformationMessage.REQUEST) is None:
                        raise ValueError("Missing request_id")
                    info_obj = InformationMessage(**values)
                    # NEW: Debug log
                    print(f"[Central InfoHandler]:    Decoded: Request {info_obj.request_id}, Type: {info_obj.type.name}")
                    
                except Exception as e:
                    print(f"[Central InfoHandler]:    ERROR: Invalid info message, skipping: {e} -- MSG: {msg}")
                    continue
                
                request_id = info_obj.request_id
                cp_id = info_obj.cp_id
                consumption = info_obj.consumption
                price = info_obj.price
                price_kw = info_obj.price_kw

                cursor.execute("SELECT driver_id FROM requests WHERE request_id = ?", (request_id,))
                result = cursor.fetchone()
                if result is None:
                    print(f"[Central InfoHandler]:    ERROR: Received info for unknown request_id {request_id}")
                    continue
                driver_id = result[0]
                
                # FIX: Send to specific partition required by EV_DRIVER.py
                target_partition = int(driver_id) % NUM_PARTITIONS
                # NEW: Debug log
                print(f"[Central InfoHandler]:    Message is for Driver {driver_id} (Partition {target_partition})")
                
                match info_obj.type:

                    case InformationTypeEngine.CHARGING_START:
                        print(f"[Central InfoHandler]:    Type: CHARGING_START. Updating CP {cp_id} status to IN_USAGE.")
                        # FIX: SQL needs WHERE clause
                        cursor.execute("UPDATE charging_points SET status = ? WHERE id = ?", 
                                    (ChargingPointStatus.IN_USAGE.value, cp_id))
                        connection.commit()

                    case InformationTypeEngine.CHARGING_ONGOING:
                        # NEW: Debug log
                        print(f"[Central InfoHandler]:    Type: CHARGING_ONGOING. Updating request {request_id} consumption/price.")
                        # FIX: SQL needs WHERE clause
                        cursor.execute("UPDATE requests SET consumption = ?, price = ? WHERE request_id = ?", 
                                    (consumption, price, request_id))
                        connection.commit()

                    case InformationTypeEngine.CHARGING_END:
                        print(f"[Central InfoHandler]:    Type: CHARGING_END. Finalizing request {request_id}.")
                        cursor.execute("UPDATE charging_points SET status = ? WHERE id = ?", 
                                    (ChargingPointStatus.ACTIVE.value, cp_id))
                        
                        cursor.execute("UPDATE requests SET done = ?, consumption = ?, price = ? WHERE request_id = ?", 
                                    (True, consumption, price, request_id))
                        connection.commit()

                        ticket = Ticket(request_id, cp_id, consumption, price, price_kw)
                        ticket_str = str(ticket)
                        
                        # NEW: Debug log
                        print(f"[Central InfoHandler]: --> Sending TICKET for {request_id} to 'Central-Driver' (Partition {target_partition})")
                        self._information_producer.send("Central-Driver", 
                                                        value=ticket_str.encode('utf-8'),
                                                        partition=target_partition)
                        self._information_producer.flush()
                        continue # Don't send the generic info message
                
                # Forward START and ONGOING messages to the driver
                info_str = str(info_obj)
                # NEW: Debug log
                print(f"[Central InfoHandler]: --> Forwarding INFO ({info_obj.type.name}) to 'Central-Driver' (Partition {target_partition})")
                self._information_producer.send("Central-Driver", 
                                                value=info_str.encode('utf-8'),
                                                partition=target_partition)
                self._information_producer.flush()
        except Exception as e:
            print(f"CRITICAL: informationHandler crashed: {e}")
        finally:
            connection.close()
            print("Information handler stopped.")


    def validate(self, cp_id, connection: sqlite3.Connection):
            """
            Checks if the station is ready for charging.
            Uses a synchronous DB connection.
            """
            cursor = connection.cursor()
            try:
                # FIX: Select only what you need
                cursor.execute("SELECT status FROM charging_points WHERE id = ?", (cp_id,))
                result = cursor.fetchone()
                
                if result is None:
                    print(f"Validation Error: CP {cp_id} not found in DB.")
                    return False

                cp_status = ChargingPointStatus(result[0])
                
                # FIX: Match on the enum object, not its name
                match cp_status:
                    case ChargingPointStatus.ACTIVE:
                        return True
                    case ChargingPointStatus.OUT_OF_ORDER:
                        print(f"Validation: Station {cp_id} is out of order")
                        return False
                    case ChargingPointStatus.IN_USAGE:
                        print(f"Validation: Station {cp_id} is already in use")
                        return False
                    case ChargingPointStatus.DEFECT:
                        print(f"Validation: Station {cp_id} is defect")
                        return False
                    case ChargingPointStatus.DISCONNECTED:
                        print(f"Validation: Station {cp_id} is disconnected")
                        return False
                    case _:
                        return False
            except Exception as e:
                print(f"Validation Error: {e}")
                return False

    # --- Async Monitor Handling ---

    def start_monitor_server(self):
        """Thread target: Starts the asyncio event loop for the socket server."""
        print(f"Monitor server starting event loop...")
        try:
            asyncio.run(self._run_async_server())
        except Exception as e:
            print(f"CRITICAL: Monitor server event loop crashed: {e}")

    async def _run_async_server(self):
        """Initializes the socket server and the timeout checker."""
        server = await asyncio.start_server(
            self.handle_monitor, 
            port=self._listener_port,
            host='0.0.0.0' # Listen on all interfaces (for Docker)
        )
        print(f"Socket server listening on 0.0.0.0:{self._listener_port}")

        # Run the server and the timeout checker concurrently
        await asyncio.gather(
            server.serve_forever(),
            self._check_monitor_timeouts()
        )

    async def _check_monitor_timeouts(self):
        """
        R3.2: Checks for monitors that haven't sent a heartbeat.
        """
        await asyncio.sleep(MONITOR_CHECK_INTERVAL) # Initial delay
        print("Monitor timeout checker running...")
        
        # FIX: Connect to the correct DB path
        async with aiosqlite.connect(DB_PATH) as db:
            while True:
                try:
                    timed_out_cps = []
                    current_time = time.time()
                    
                    # Create a snapshot of the keys to avoid runtime errors
                    cp_ids = list(self._monitor_last_seen.keys())
                    
                    # Find all monitors that haven't been seen recently
                    for cp_id in cp_ids:
                        if cp_id in self._monitor_last_seen:
                            if current_time - self._monitor_last_seen[cp_id] > MONITOR_HEARTBEAT_TIMEOUT:
                                timed_out_cps.append(cp_id)
                    
                    if timed_out_cps:
                        print(f"Monitors timed out: {timed_out_cps}. Setting status to DISCONNECTED.")
                        for cp_id in timed_out_cps:
                            await db.execute(
                                "UPDATE charging_points SET status = ? WHERE id = ?",
                                (ChargingPointStatus.DISCONNECTED.value, cp_id)
                            )
                            # Remove from check list to prevent spamming
                            del self._monitor_last_seen[cp_id]
                        
                        await db.commit()
                        
                except Exception as e:
                    print(f"Error in monitor timeout checker: {e}")

                await asyncio.sleep(MONITOR_CHECK_INTERVAL)

    async def handle_monitor(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """
        R4.3: Handles a single monitor connection using the <stx>D<etx><lrc> protocol.
        """
        addr = writer.get_extra_info('peername')
        print(f"Monitor connected from {addr}")
        
        protocol = AsyncSocketFrameProtocol(reader)
        monitor_cp_id = None
        
        try:
            # FIX: Connect to the correct DB path
            async with aiosqlite.connect(DB_PATH) as db:
                # Read messages using the frame protocol
                async for decodedData in protocol.read_messages():
                    
                    try:
                        # Parse the key-value string
                        msg = StatusMessage.decode_message(decodedData)
                    except Exception as e:
                        print(f"Invalid message from {addr}: {e}. Data: '{decodedData}'")
                        continue
                    
                    monitor_cp_id = msg.cp_id
                    
                    # R3.2: Update heartbeat time for this monitor
                    self._monitor_last_seen[monitor_cp_id] = time.time()

                    # FIX: Match on the message type
                    match msg.type:

                        case InformationTypeMonitor.AUTHENTICATION:
                            print(f"Monitor Auth: CP {msg.cp_id}, Status: {msg.status.name}")
                            # FIX: SQL (INSERT ON CONFLICT) and removed 'location'
                            await db.execute("""
                            INSERT INTO charging_points (id, status, priceKW)
                            VALUES (?, ?, ?)
                            ON CONFLICT(id) DO UPDATE SET
                                status=excluded.status,
                                priceKW=excluded.priceKW
                            """, (msg.cp_id, msg.status.value, msg.price_kw))

                        case InformationTypeMonitor.STATUS_UPDATE:
                            await db.execute("UPDATE charging_points SET status = ? WHERE id = ?", 
                                            (msg.status.value, msg.cp_id))

                        case InformationTypeMonitor.ERROR:
                            print(f"Monitor ERROR: CP {msg.cp_id} reported {msg.status.name}.")
                            await db.execute("UPDATE charging_points SET status = ? WHERE id = ?", 
                                            (msg.status.value, msg.cp_id))
                            
                            # Notify the active driver
                            await self.notify_driver(db, msg.cp_id, str(msg))

                        case InformationTypeMonitor.ERROR_RESOLVED:
                            print(f"Monitor Error Resolved: CP {msg.cp_id} is now {msg.status.name}.")
                            await db.execute("UPDATE charging_points SET status = ? WHERE id = ?", 
                                            (msg.status.value, msg.cp_id))
                            
                            await self.notify_driver(db, msg.cp_id, str(msg))
                    
                    await db.commit()
        
        except (asyncio.IncompleteReadError, ConnectionResetError):
            print(f"Monitor {addr} (CP {monitor_cp_id}) disconnected.")
        except Exception as e:
            print(f"Error in handle_monitor for {addr}: {e}")
        finally:
            if monitor_cp_id and monitor_cp_id in self._monitor_last_seen:
                # Stop tracking this monitor for timeouts
                del self._monitor_last_seen[monitor_cp_id]
            writer.close()
            await writer.wait_closed()
            
    async def notify_driver(self, db: aiosqlite.Connection, cp_id: str, message_str: str):
        """
        (Async) Finds an active driver at a CP and sends them a message.
        """
        cursor = await db.execute("""
            SELECT driver_id
            FROM requests
            WHERE cp_id = ? AND done = False
        """, (cp_id,))
        
        result = await cursor.fetchone()
        await cursor.close()

        if result:
            driver_id = result[0]
            print(f"Notifying Driver {driver_id} about event at CP {cp_id}")
            
            # FIX: Send to specific partition
            target_partition = int(driver_id) % NUM_PARTITIONS
            self._information_producer.send(
                'Central-Driver',
                value=message_str.encode('utf-8'),
                partition=target_partition
            )
            self._information_producer.flush()
        else:
            print(f"Event at CP {cp_id}, but no active driver was found.")

                      
if __name__ == "__main__":
    main()
import asyncio
import aiosqlite
import sqlite3
import sys
import threading
import time
from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError


from EV_Utile import (
    ChargingPointStatus,
    InformationMessage,
    InformationTypeEngine,
    InformationTypeMonitor,
    RequestMessage,
    StatusMessage,
    Ticket,
    AsyncSocketFrameProtocol
)
import EV_DB_Setup

# --- Config ---
MONITOR_HEARTBEAT_TIMEOUT = 30
MONITOR_CHECK_INTERVAL = 10
NUM_PARTITIONS = 7

def main():
    if len(sys.argv) < 3:
        print("Usage: python3 EV_Central.py <Broker_address_ip:port> <Listener_port>")
        sys.exit(1)
    
    broker_ip = sys.argv[1]
    listener_port = int(sys.argv[2])
    
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
        self._monitor_last_seen = {}

        print(f"Central connecting to Kafka at {self._broker_ip}")
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
        EV_DB_Setup.setUpDB()
        print("Central setup complete.")

    def run_central(self):
        print("Central running...")
        monitor_thread = threading.Thread(target=self.start_monitor_server)
        monitor_thread.start()
        request_thread = threading.Thread(target=self.requestHandler)
        request_thread.start()
        information_thread = threading.Thread(target=self.informationHandler)
        information_thread.start()
    
    def createTopics(self):
        admin_client = None
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=self._broker_ip,
                client_id='central-setup'
            )
            
            topic_list = [
                NewTopic(name="Driver-Central", num_partitions=NUM_PARTITIONS, replication_factor=1),
                NewTopic(name="Central-Driver", num_partitions=NUM_PARTITIONS, replication_factor=1),
                NewTopic(name="Central-CP", num_partitions=NUM_PARTITIONS, replication_factor=1),
                NewTopic(name="CP-Central", num_partitions=NUM_PARTITIONS, replication_factor=1)
            ]

            admin_client.create_topics(new_topics=topic_list, validate_only=False)
            print(f"Topics created with {NUM_PARTITIONS} partitions.")
        except TopicAlreadyExistsError:
            print("Topics already exist, skipping creation.")
        except Exception as e:
            print(f"Topic creation failed (is Kafka running at {self._broker_ip}?): {e}")
        finally:
            if admin_client:
                admin_client.close()
    
    def requestHandler(self):
        print("Request handler running...")
        connection = sqlite3.connect('EV.db')
        cursor = connection.cursor()
        
        try:
            for request in self._request_consumer:
                msg = request.value.decode('utf-8')
                
                try:
                    req_obj = RequestMessage.decode_message(msg)
                    driver_id = req_obj.driver_id
                    cp_id = req_obj.cp_id
                except Exception as e:
                    print(f"Invalid request message, skipping: {e} -- MSG: {msg}")
                    continue

                if driver_id is None or cp_id is None:
                    print(f"Invalid request, missing driver_id or cp_id: {msg}")
                    continue

                print(f"Processing request from Driver {driver_id} for CP {cp_id}")

                if (self.validate(cp_id, connection)):
                    
                    cursor.execute("SELECT MAX(request_id) FROM requests")
                    result = cursor.fetchone()
                    request_id = 1 if result is None or result[0] is None else result[0] + 1

                    cursor.execute("SELECT priceKW FROM charging_points WHERE id = ?", (cp_id,))
                    result = cursor.fetchone()
                    if result is None:
                        print(f"Error: Could not find price for cp_id {cp_id} after validation.")
                        continue 
                    price_kw = result[0]

                    cursor.execute("""
                                INSERT INTO requests (request_id, cp_id, consumption, price, price_kw, driver_id, done)
                                VALUES(?, ?, ?, ?, ?, ?, ?)
                    """, (request_id, cp_id, 0.0, 0.0, price_kw, driver_id, False))
                    connection.commit()

                    req_obj.request_id = request_id
                    msg_str = str(req_obj)

                    key = str(int(cp_id) % NUM_PARTITIONS).encode('utf-8')
                    
                    self._request_producer.send('Central-CP', 
                                                value=msg_str.encode('utf-8'),
                                                key=key)
                    self._request_producer.flush()
                    print(f"Request {request_id} validated and sent to CP {cp_id} (Partition Key: {key.decode()})")

                else: 
                    print(f"Request for CP {cp_id} denied: validation failed.")
        except Exception as e:
            print(f"CRITICAL: requestHandler crashed: {e}")
        finally:
            connection.close()
            print("Request handler stopped.")
    
    def informationHandler(self):
        print("Information handler running...")
        connection = sqlite3.connect("EV.db")
        cursor = connection.cursor()
        
        try:
            for information in self._information_consumer:
                msg = information.value.decode('utf-8')
                
                try:
                    values = InformationMessage.decode_message(msg)
                    if values.get(InformationMessage.REQUEST) is None:
                        raise ValueError("Missing request_id")
                    
                    info_obj = InformationMessage(**values)
                    
                except Exception as e:
                    print(f"Invalid info message, skipping: {e} -- MSG: {msg}")
                    continue
                
                request_id = info_obj.request_id
                cp_id = info_obj.cp_id
                consumption = info_obj.consumption
                price = info_obj.price
                price_kw = info_obj.price_kw

                cursor.execute("SELECT driver_id FROM requests WHERE request_id = ?", (request_id,))
                result = cursor.fetchone()
                if result is None:
                    print(f"Error: Received info for unknown request_id {request_id}")
                    continue
                driver_id = result[0]
                
                driver_key = str(int(driver_id) % NUM_PARTITIONS).encode('utf-8')
                
                match info_obj.type:
                    case InformationTypeEngine.CHARGING_START:
                        print(f"Info: CP {cp_id} started charging for request {request_id}")
                        cursor.execute("UPDATE charging_points SET status = ? WHERE id = ?", 
                                    (ChargingPointStatus.IN_USAGE.value, cp_id))
                        connection.commit()

                    case InformationTypeEngine.CHARGING_ONGOING:
                        cursor.execute("UPDATE requests SET consumption = ?, price = ? WHERE request_id = ?", 
                                    (consumption, price, request_id))
                        connection.commit()

                    case InformationTypeEngine.CHARGING_END:
                        print(f"Info: CP {cp_id} finished charging for request {request_id}")
                        cursor.execute("UPDATE charging_points SET status = ? WHERE id = ?", 
                                    (ChargingPointStatus.ACTIVE.value, cp_id))
                        
                        cursor.execute("UPDATE requests SET done = ?, consumption = ?, price = ? WHERE request_id = ?", 
                                    (True, consumption, price, request_id))
                        connection.commit()

                        ticket = Ticket(request_id, cp_id, consumption, price, price_kw)
                        ticket_str = str(ticket)
                        
                        self._information_producer.send("Central-Driver", 
                                                        value=ticket_str.encode('utf-8'),
                                                        key=driver_key)
                        continue 
                
                info_str = str(info_obj)
                self._information_producer.send("Central-Driver", 
                                                value=info_str.encode('utf-8'),
                                                key=driver_key)
        except Exception as e:
            print(f"CRITICAL: informationHandler crashed: {e}")
        finally:
            connection.close()
            print("Information handler stopped.")

    def validate(self, cp_id, connection: sqlite3.Connection):
            cursor = connection.cursor()
            try:
                cursor.execute("SELECT status FROM charging_points WHERE id = ?", (cp_id,))
                result = cursor.fetchone()
                
                if result is None:
                    print(f"Validation Error: CP {cp_id} not found in DB.")
                    return False

                cp_status = ChargingPointStatus(result[0])
                
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
        print(f"Monitor server starting event loop...")
        try:
            asyncio.run(self._run_async_server())
        except Exception as e:
            print(f"CRITICAL: Monitor server event loop crashed: {e}")

    async def _run_async_server(self):
        server = await asyncio.start_server(
            self.handle_monitor, 
            port=self._listener_port,
            host='0.0.0.0'
        )
        print(f"Socket server listening on 0.0.0.0:{self._listener_port}")

        await asyncio.gather(
            server.serve_forever(),
            self._check_monitor_timeouts()
        )

    async def _check_monitor_timeouts(self):
        await asyncio.sleep(MONITOR_CHECK_INTERVAL) 
        print("Monitor timeout checker running...")
        
        async with aiosqlite.connect('EV.db') as db:
            while True:
                try:
                    timed_out_cps = []
                    current_time = time.time()
                    
                    for cp_id, last_seen in list(self._monitor_last_seen.items()):
                        if current_time - last_seen > MONITOR_HEARTBEAT_TIMEOUT:
                            timed_out_cps.append(cp_id)
                    
                    if timed_out_cps:
                        print(f"Monitors timed out: {timed_out_cps}. Setting status to DISCONNECTED.")
                        for cp_id in timed_out_cps:
                            await db.execute(
                                "UPDATE charging_points SET status = ? WHERE id = ?",
                                (ChargingPointStatus.DISCONNECTED.value, cp_id)
                            )
                            if cp_id in self._monitor_last_seen:
                                del self._monitor_last_seen[cp_id]
                        
                        await db.commit()
                        
                except Exception as e:
                    print(f"Error in monitor timeout checker: {e}")

                await asyncio.sleep(MONITOR_CHECK_INTERVAL)

    async def handle_monitor(self, reader, writer):
        """
        Implementiert R4.3: Liest gerahmte Nachrichten vom Monitor.
        """
        addr = writer.get_extra_info('peername')
        print(f"Monitor connected from {addr}")
        
        monitor_cp_id = None
        protocol = AsyncSocketFrameProtocol(reader) # Protokoll-Handler
        
        try:
            async with aiosqlite.connect('EV.db') as db:
                # Liest kontinuierlich GÜLTIGE, GERRRAHMTE Nachrichten
                async for decodedData in protocol.read_messages():
                    
                    try:
                        msg = StatusMessage.decode_message(decodedData)
                    except Exception as e:
                        print(f"Invalid message from {addr}: {e}. Data: '{decodedData}'")
                        continue
                    
                    monitor_cp_id = msg.cp_id
                    self._monitor_last_seen[monitor_cp_id] = time.time()

                    match msg.type:
                        case InformationTypeMonitor.AUTHENTICATION:
                            print(f"Monitor Auth: CP {msg.cp_id}, Status: {msg.status.name}, Price: {msg.price_kw}")
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
            if monitor_cp_id:
                print(f"Setting CP {monitor_cp_id} to DISCONNECTED due to socket close.")
                if monitor_cp_id in self._monitor_last_seen:
                    del self._monitor_last_seen[monitor_cp_id]
                
                async with aiosqlite.connect('EV.db') as db:
                    await db.execute(
                        "UPDATE charging_points SET status = ? WHERE id = ?",
                        (ChargingPointStatus.DISCONNECTED.value, monitor_cp_id)
                    )
                    await db.commit()

            writer.close()
            await writer.wait_closed()
            
    async def notify_driver(self, db: aiosqlite.Connection, cp_id: str, message_str: str):
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
            
            key = str(int(driver_id) % NUM_PARTITIONS).encode('utf-8')
                
            self._information_producer.send(
                'Central-Driver',
                value=message_str.encode('utf-8'),
                key=key
            )
            self._information_producer.flush()
        else:
            print(f"Event at CP {cp_id}, but no active driver was found.")

                      
if __name__ == "__main__":
    main()


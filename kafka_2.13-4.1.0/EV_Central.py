import asyncio
import aiosqlite
from EV_Utile import ChargingPointStatus, InformationMessage, InformationTypeEngine, InformationTypeMonitor, RequestMessage
from enum import Enum
import EV_DB_Setup

from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient, NewTopic

import random
import socket
import sqlite3
import sys

import threading

def main() :
    # check arguments
    if len(sys.argv) < 3:
        print("Usage: python3 EV_Central.py <Broker_ip> <Listener_port>")
    
    broker_ip = sys.argv[1]
    listener_port = sys.argv[2]
    central = Central(
        broker_ip,
        listener_port
    )

    # call function that handles running the different components of the central
    central.run_central()


class Central:

    def __init__(self, broker_ip, listener_port ):
        self._broker_ip = broker_ip
        self._listener_port = listener_port
        self._request_conumser = KafkaConsumer(
        'Driver-Central',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group'
        )
        self._information_consumer = KafkaConsumer(
        'CP-Central',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group'
        )
        self._request_producer = KafkaProducer(bootstrap_servers='localhost:9092')
        self._information_producer = KafkaProducer(bootstrap_servers='localhost:9092')

        self.createTopics()
        EV_DB_Setup.setUpDB()



    # DONE
    def run_central(self):

        # Start the different parts of the central
        monitor_thread = threading.Thread(target=self.monitorHandler)
        monitor_thread.start()
        request_thread = threading.Thread(target=self.requestHandler)
        request_thread.start()
        information_thread = threading.Thread(target=self.informationHandler)
        information_thread.start()

    # DONE
    # SetUp the wanted Kafka Topics
    def createTopics(self) :
        # Connect to the broker
        admin_client = KafkaAdminClient(
            bootstrap_servers= self.broker_port,
            client_id='setup-script'
        )

        # Define topics
        topic_list = [
            NewTopic(
                name="Driver-Central",
                num_partitions=1,
                replication_factor=1
            ),
            NewTopic(
                name="Central-Driver",
                num_partitions=1,
                replication_factor=1
            ),
            NewTopic(
                name="Central-CP",
                num_partitions=1,
                replication_factor=1
            ),
            NewTopic(
                name="CP-Central",
                num_partitions=1,
                replication_factor=1
            )
    ]

        try:
            admin_client.create_topics(new_topics=topic_list, validate_only=False)
            print("Topics created successfully!")
        except Exception as e:
            print("Topic creation skipped or failed:", e)
        finally:
            admin_client.close()

    # DONE 
    def requestHandler(self):

        for request in self.request_consumer:

            msg = request.value.decode('utf-8')
            values = RequestMessage.decode_message(msg)
            driver_id = values[RequestMessage.DRIVER]
            cp_id = values[RequestMessage.CP]

            if (self.validate(cp_id)) :

                # write request in DB so information is sent to correct driver
                connection = sqlite3.connect('EV.db')
                cursor = connection.cursor()
                
                
                cursor.execute("""
                            SELECT request_id
                            FROM requests 
                            ORDER BY request_id DESC
                """)
                request_id = cursor.fetchone() + 1

                cursor.execute("""
                            INSERT INTO requests (request_id, driver_id, cp_id)
                            VALUES( ?, ?, ?)
                """, (request_id, driver_id, cp_id))

                msg_with_request_id = RequestMessage(
                    cp_id,
                    driver_id,
                    request_id
                )
                msg_with_request_id = str(msg_with_request_id)

                self.request_producer.send('Central-CP', msg_with_request_id.encode('utf-8'))

            else : 
                print(f"cp: {cp_id} is not reachable right now")
            
    # Done
    # Receives the information from the charging point Stores it in the DB
    def informationHandler(self) :
        connection = sqlite3.connect("EV.db")
        cursor = connection.cursor()
        
        try:
            for information in self.information_cnsumer:

                msg = information.value.decode('utf-8')
                
                values = InformationMessage.decode_message(msg)
                request_id = values[InformationMessage.REQUEST]
                type = values[InformationMessage.TYPE]
                cp_id = values[InformationMessage.CP]
                start_time = values[InformationMessage.START]
                current_time = values[InformationMessage.CURRENT]
                price_kw = values[InformationMessage.KW]

                information_message = InformationMessage(
                    request_id,
                    type,
                    cp_id,
                    start_time,
                    current_time,
                    price_kw
                )
                consumption = information_message.consumption
                price = information_message.price


                match InformationTypeEngine(type):

                    case 'CHARGING_START' :
                        cursor.execute("""
                        UPDATE charging_points
                        SET consumption = 0, price = ? 
                        WHERE id = ?
                        """, (consumption, price, cp_id))
                        connection.commit()

                        # send information to driver
                        information_message = str(information_message)
                        self.information_producer.send("Central-Driver", information_message.decode('UTF-8'))
                    
                    case 'CHARGING_ONGOING' :
                        cursor.execute("""
                        UPDATE charging_points
                        SET consumption = ?, price = ? 
                        WHERE id = ?
                        """, (consumption, price, cp_id))
                        connection.commit()

                        # send information to driver
                        information_message = str(information_message)
                        self.information_producer.send("Central-Driver", information_message.decode('UTF-8'))

                    case 'CHARGING_END' :
                        cursor.execute("""
                        UPDATE charging_points
                        SET consumption = ?, price = ? 
                        WHERE id = ?
                        """, (consumption, price, cp_id))
                        connection.commit()

                        # sending a ticket to the driver
                        ticket = Ticket(
                            request_id,
                            cp_id,
                            consumption,
                            price,
                            price_kw,
                        )
                        ticket = str(ticket)
                        self.information_producer.send("Central-Driver", ticket.decode('UTF-8'))

        finally:
            connection.close()



        # Checks if the station is ready for charging and outputs information about status
        def validate(self, cp_id):

            # get charging point - form: (id, location, status, priceKW, consumption, price)
            connection = sqlite3.connect("EV.db")
            cursor = connection.cursor()

            cursor.execute("SELECT * FROM charging_points WHERE id = ?", 
                        (cp_id)
                        )
            chargingPoint = cursor.fetchone()

            # check status
            cp_status = ChargingPointStatus(chargingPoint[2])
            match cp_status.name :
                case 'ACTIVE' :
                    return True
                case 'OUT_OF_ORDER' :
                    print("Station ? is out of order", cp_id)
                    return False
                case 'IN_USAGE' :
                    print("Station ? is already used", cp_id)
                    return False
                case 'DEFECT' :
                    print("Station ? is defect", cp_id)
                    return False
                case 'DISCONNECTED':
                    print("Station ? is disconnected and is asked to authenticate itself to the server", cp_id)
                    # TODO("How to react in this case, is giving the information enough?")
                    return False


    # Puts the runServer() coroutine into the eventloop
    async def monitorHandler(self):
        
            asyncio.run(self.runServer())


        async def runServer(self):

            server = await asyncio.server_start(self.getStatus(), port=self.listenerPort )
            async with server:
                await server.serve_forever()


        # Listens to the EV_CP_M and updates the DB
        async def getStatus(self) :

            async with aiosqlite.connect('EV.db') as db:
                while data := await reader.read_line():

                    decodedData = data.decode().strip()
                    type, cpId, location,  status, priceKW, consumption, price = decodedData.split()

                    match InformationTypeMonitor(type) :

                        case 'AUTHENTICATION' :

                            # TODO("inform")

                            await db.execute("""
                            INSERT OR IGNORE INTO charging_points (id, status)
                            VALUES (?, ?, ?, ?, ?, ?)
                            """, (cpId, location, status, priceKW, consumption, price))
                            await db.commit()

                        case 'STATUS_CONFIRMATION' :
                            continue

                        case 'STATUS_UPDATE' :

                            # TODO("inform")

                            await db.execute( """
                                UPDATE charging_points
                                SET status = ? 
                                WHERE id = ?
                            """, (status, cpId) )
                            await db.commit()

                        case 'ERROR':

                            # TODO("inform users about problem ")

                            await db.execute( """
                                UPDATE charging_points
                                SET status = 4
                                WHERE id = ?
                            """, (cpId) )
                            await db.commit()
                            
                        case 'ERROR_RESOLVED' :
                            
                            # TODO("inform users about continuation of charging process")

                            await db.execute( """
                                UPDATE charging_points
                                SET status = 3
                                WHERE id = ?
                            """, (cpId) )

                            # TODO("if necessary send ticket to user")

                            await db.commit()





if __name__ == "__main__":
    main()
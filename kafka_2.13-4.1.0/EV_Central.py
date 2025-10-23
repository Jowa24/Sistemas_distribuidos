import asyncio
import aiosqlite
from EV_Utile import InformationMessage
from enum import Enum
import EV_DB_Setup

from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient, NewTopic

import random
import socket
import sqlite3
import sys

import threading

# Simulates the server
# arguments: 
#   --ipPort Broker 
#   --ipPort listening
def main() :
    # check arguments
    if len(sys.argv) < 3:
        print("Usage: python3 EV_Central.py <Broker_ip> <Listener_port>")
    
    brokerIP = sys.argv[1]
    ListenerPort = sys.argv[2]

    # call function that handles running the different components of the central
    central(brokerIP, ListenerPort)


class Central:

    def __init__(self, )


def central(brokerIP : str, listenerPort : str ) :

    # Get the setUp defined by the arguments given to main
    requestConsumer, requestProducer, informationConsumer, informationProducer = bootstrapCentral(brokerIP)

    # Start the different parts of the central
    thread = threading.Thread( args=listenerPort,target=monitorHandler)
    thread.start()
    thread = threading.Thread( args= (requestConsumer, requestProducer) ,target=requestHandler)
    thread.start()
    thread = threading.Thread( args=(informationConsumer, informationProducer), target=informationHandler)
    thread.start()


# TODO("Use arguments to set ports")
def bootstrapCentral( brokerIP : str) :

    """
        Check for engines in Database
        & instantiate graphical interface
    """
    EV_DB_Setup.setUpDB()

    # TODO("SetUp Graphical interface")

    
    
    """
        instantiate Producer and Consumer
        & subscribe to relevant topics
    """
    createTopics( brokerIP)

    # subscribe to requests of drivers
    requestConsumer = KafkaConsumer(
    'Driver-Central',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group'
    )
    # subscribe to information arriving from the engine
    informationConsumer = KafkaConsumer(
    'CP-Central',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group'
    )
    # create producers to pass on requests anf information 
    requestProducer = KafkaProducer(bootstrap_servers='localhost:9092')
    informationProducer = KafkaProducer(bootstrap_servers='localhost:9092')

    return (requestConsumer, requestProducer, informationConsumer, informationProducer)

# SetUp the wanted Kafka Topics
def createTopics( brokerPort) :
    # Connect to the broker
    admin_client = KafkaAdminClient(
        bootstrap_servers= brokerPort,
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
        )
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

def requestHandler( requestConsumer : KafkaConsumer, requestProducer : KafkaProducer)

    for request in requestConsumer:

        msg = request.value.decode('utf-8')
        driverId, cpId, location = msg.split()

        if (validate(cpId, location)) :

            # write request in DB so information is sent to correct driver
            connection = sqlite3.connect('EV.db')
            cursor = connection.cursor()
            
            
            cursor.execute("""
                        SELECT requestId
                        FROM requests 
                        WHERE id = ?
                        ORDER BY requestId DESC
            """, (driverId))
            requestId = cursor.fetchone() + 1

            cursor.execute("""
                        INSERT INTO requests (requestId, driverId, cpId)
                        VALUES( ?, ?, ?)
            """, (requestId, driverId, cpId))

            msgWithRequestId = requestId + " " + msg
            requestProducer.send('CR', msgWithRequestId.encode('utf-8'))

        else : 
            #TODO("better notification")
            print("Request could not be verified")
        


# Receives the information from the charging point Stores it in the DB
def informationHandler( informationConsumer : KafkaConsumer, informationProducer : KafkaProducer) :
    connection = sqlite3.connect("EV.db")
    cursor = connection.cursor()

    """
        msg should have the form:
        [requestId] [type], [cp_id], [consumption], [price_kw]
    """
    try:
        for information in informationConsumer:

            # write information to database
            msg = information.value.decode('utf-8')
            
            values = InformationMessage.decode_message(msg)
            request_id = values["request_id"]
            type = values["type"]
            cp_id = values["cp_id"]#
            price = values["price"]
            consumption = values["consumption"]
            price_kw = values["price_kw"]


            match informationTypeEnginge(type):

                case 'CHARGING_START' :
                    # TODO("handle start with extra information")
                    cursor.execute("""
                    UPDATE charging_points
                    SET consumption = 0, price = ? 
                    WHERE id = ?
                    """, (consumption, price, cp_id))
                    connection.commit()

                    # send information to driver
                    informationProducer.send("ProducerCentral", msg.value)
                
                case 'CHARGING_ONGOING' :
                    cursor.execute("""
                    UPDATE charging_points
                    SET consumption = ?, price = ? 
                    WHERE id = ?
                    """, (consumption, price, cp_id))
                    connection.commit()

                    # send information to driver
                    informationProducer.send("ProducerCentral", msg.value)

                case 'CHARGING_END' :
                    cursor.execute("""
                    UPDATE charging_points
                    SET consumption = ?, price = ? 
                    WHERE id = ?
                    """, (consumption, price, cp_id))
                    connection.commit()

                    # TODO("handle sending a ticket to the driver")
                    informationProducer.send("ProducerCentral", msg.value)

    finally:
        connection.close()



    # Checks if the station is ready for charging and outputs information about status
    def validate(cp_id : str, location : str) :

    # get charging point - form: (id, location, status, priceKW, consumption, price)
    connection = sqlite3.connect("EV.db")
    cursor = connection.cursor()

    cursor.execute("SELECT * FROM charging_points WHERE id = ? AND location = ?", 
                   (cp_id, location)
                )
    chargingPoint = cursor.fetchone()

    # check status
    cp_status = Status(chargingPoint[2])
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
    async def monitorHandler( listenerPort : str) :
    
    asyncio.run(runServer( listenerPort))


    async def runServer( listenerPort : str) :

    server = await asyncio.server_start(getStatus(), port=listenerPort )
    async with server:
        await server.serve_forever()


    # Listens to the EV_CP_M and updates the DB
    async def getStatus( reader, writer) :

        async with aiosqlite.connect('EV.db') as db:
            while data := await reader.read_line():

                decodedData = data.decode().strip()
                type, cpId, location,  status, priceKW, consumption, price = decodedData.split()

                match informationTypeMonitor(type) :

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
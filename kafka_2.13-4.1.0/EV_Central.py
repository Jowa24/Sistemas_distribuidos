import asyncio
import aiosqlite

from enum import Enum
import EV_DB_Setup
from flask import Flask

from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient, NewTopic

import socket
import sqlite3
import sys

import threading

# Simulates the server
# arguments: 
#   --ipPort Broker 
#   --ipPort listening

"""
    1. Server needs two arguments
     - throw error if less arguments received

    2. Server starts the bootstrap process
    - Check for active CP's
    - Start Monitor(?)

    3. Wait for 
        a. request for new CP
        b. request for charging
    
    Charging:
        a. validateRequest (userID : int, cpID : int)
        b. sendRequest (userID : int, cpID : int) - request is send to Topic "Order"
        c. statusCharging() - outputs the status of the process to the consol
            - waits for the user to end the charging process
        d. sendTicket() - final ticket is sent with information

    Registration:
        a. update Status of the CP

    4. Extras:
        a. activate Charging Point
        b. stop Charging Point
"""
def main() :
    # check arguments
    if len(sys.argv) < 3:
        print("Usage: python3 EV_Central.py <Broker_ip> <Listener_port>")
    
    brokerIP = sys.argv[1]
    ListenerPort = sys.argv[2]

    # call function that handles running the different components of the central
    central(brokerIP, ListenerPort)

"""
    Creates the different Threads handling the different roles of the server
        1. MonitorHandler keeps contact with the monitors of the CP's and keeps their status in DB and monitors them
        2. RequestHandler handles the validating and on-passing of the requests
        3. InformationHandler saves all the information in the DB and logs it
"""
def central(brokerIP : str, listenerPort : str ) :

    # Get the setUp defined by the arguments given to main
    requestConsumer, requestProducer, informationConsumer, informationProducer = bootstrapCentral(brokerIP)

    # Start the different parts of the central
    threading.Thread( args=listenerPort,target=monitorHandler)
    threading.Thread( args= (requestConsumer, requestProducer) ,target=requestHandler)
    threading.Thread( args=(informationConsumer, informationProducer), target=informationHandler)

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
def createTopics( borkerPort) :
    # Connect to the broker
    admin_client = KafkaAdminClient(
        bootstrap_servers= TODO,
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

def requestHandler( requestConsumer : KafkaConsumer, requestProducer : KafkaProducer):
    """
        msg should have the form:
        [driver_id] [cp_id] [location]
    """
    for request in requestConsumer:

        msg = request.value.decode('utf-8')
        userId, cpId, location = msg.split()

        if (validate(cpId, location)) :
            requestProducer.send('CR', msg.value)
        else : 
            print("Request could not be verified")
        

"""
    Receives the information from the charging point
    Stores it in the DB
"""
def informationHandler( informationConsumer : KafkaConsumer, informationProducer : KafkaProducer) :
    connection = sqlite3.connect("EV.db")
    cursor = connection.cursor()

    """
        msg should have the form:
        [type], [id], [consumption], [price]
    """
    try:
        for information in informationConsumer:

            # write information to database
            msg = information.value.decode('utf-8')
            type, cpId, consumption, price = msg.split()

            match informationTypeEnginge(type):

                case 'CHARGING_START' :
                    # TODO("handle start with extra information")
                    cursor.execute("""
                    UPDATE charging_points
                    SET consumption = 0, price = ? 
                    WHERE id = ?
                    """, (consumption, price, cpId))
                    connection.commit()

                    # send information to driver
                    informationProducer.send("ProducerCentral", msg.value)
                
                case 'CHARGING_ONGOING' :
                    cursor.execute("""
                    UPDATE charging_points
                    SET consumption = ?, price = ? 
                    WHERE id = ?
                    """, (consumption, price, cpId))
                    connection.commit()

                    # send information to driver
                    informationProducer.send("ProducerCentral", msg.value)

                case 'CHARGING_END' :
                    cursor.execute("""
                    UPDATE charging_points
                    SET consumption = ?, price = ? 
                    WHERE id = ?
                    """, (consumption, price, cpId))
                    connection.commit()
                    # TODO("handle sending a ticket to the driver")
                    # send ticket to driver
                    informationProducer.send("ProducerCentral", msg.value)

            


    finally:
        connection.close()


"""
    Checks if the station is ready for charging and outputs information about status
    returns True if status is ATIVE 
    returns False in every other case
"""
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

"""
    Puts the runServer() coroutine into the eventloop
    Allows for "concurrent" efficent handling of the different connections through the event loop
"""
async def monitorHandler( listenerPort : str) :
    
    asyncio.run(runServer( listenerPort))

"""
    Uses async to create a server that is accepting new connections and handling them "concurrently" 
    To handle the connections the getStatus() method is run
"""
async def runServer( listenerPort : str) :

    server = await asyncio.server_start(getStatus(), port=listenerPort )
    async with server:
        await server.serve_forever()

"""
    Listens to the EV_CP_M 
    Reveives the status 
    Updates the CP in the DB
    Received data should have structure:
    [cp_id] [status_as_int]
"""
async def getStatus( reader, writer) :

    async with aiosqlite.connect('EV.db') as db:
        while data := await reader.read_line():

            decodedData = data.decode().strip()
            type, cpId, location,  status, priceKW, consumption, price = decodedData.split()

            match informationTypeMonitor(type) :

                case 'AUTHENTICATION' :
                    await db.execute("""
                    INSERT OR IGNORE INTO charging_points (id, status)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """, (cpId, location, status, priceKW, consumption, price))
                    await db.commit()

                case 'STATUS_CONFIRMATION' :
                    continue

                case 'STATUS_UPDATE' :
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
                    await db.commit()


class Status(Enum):
    ACTIVE = 1
    OUT_OF_ORDER = 2
    IN_USAGE = 3
    DEFECT = 4
    DISCONNECTED = 5

class informationTypeMonitor(Enum):
    AUTHENTICATION = 1
    STATUS_CONFIRMATION = 2
    STATUS_UPDATE = 3
    ERROR = 4
    ERROR_RESOLVED = 5

class informationTypeEnginge(Enum):
    CHARGING_START = 1
    CHARGING_ONGOING = 2
    CHARGING_END = 3


if __name__ == "__main__":
    main()
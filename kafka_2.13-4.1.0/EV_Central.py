from kafka import KafkaProducer
import socket
import sys

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
    
    host_ip = sys.argv[1]
    Listener_port = sys.argv[2]

    bootstrapCentral()
    runCentral()

def bootstrapCentral() :


def runCentral() :
    """
        instantiate Producer and Consumer
        subscribe to relevant topics
    """
    while True:
        """
            Case distinction for the input arriving
        """
    
if __name__ == "__main__":
    main()
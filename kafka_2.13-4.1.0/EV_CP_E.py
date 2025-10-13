
from kafka import KafkaProducer
from kafka import KafkaConsumer
import sys

# Simulates an arbitrary Charging Point's Engine
# arguments:
#   --ipPort Broker


"""
    1. Waits till it get's an order from the server
    2. Simulates the charging process and logs it to the server
    3. Waits till the server tells him to stop

    Extra:
        a. when KO is typed into the console, the Enginge goes offline
"""
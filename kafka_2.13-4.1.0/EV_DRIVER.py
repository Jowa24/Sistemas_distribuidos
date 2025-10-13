
from kafka import KafkaProducer
import sys

# Implements an arbitrary Driver
# arguments
#   --id
#   --ipPort Broker

"""
    1. Sends request to start Charging
    2. When validated starts Charging
    3. Ends charging
    Repeat after sleeping 5 seconds
"""
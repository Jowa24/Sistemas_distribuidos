
import sys

# Implements the Monitor of the Charging Point
# arguments:
#   --ipPort EV_Central
#   --ipPort EV_CP_E
#   --id 

"""
    1. Connects to his CP Enginge and authenticates it to the server (THROUGH SOCKETS)
    1,5. (Sets the id for the Engine - how should enginge know otherwise which requests are for it, location?)
    2 Repeatedly checks the status and sends it to the server in critical case ( THROUGH SOCKETS)

"""
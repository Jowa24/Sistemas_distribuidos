from enum import Enum
import re

class ChargingPointStatus(Enum):
    ACTIVE = 1
    OUT_OF_ORDER = 2
    IN_USAGE = 3
    DEFECT = 4
    DISCONNECTED = 5

class InformationTypeMonitor(Enum):
    AUTHENTICATION = 1
    STATUS_CONFIRMATION = 2
    STATUS_UPDATE = 3
    ERROR = 4
    ERROR_RESOLVED = 5

class InformationTypeEngine(Enum):
    CHARGING_START = 1
    CHARGING_ONGOING = 2
    CHARGING_END = 3

class RequestMessage:

    def __init__(self, cp_id, driver_id):
        self._cp_id = cp_id
        self._driver_id = driver_id
        self.request_id = None

    @property
    def driver_id(self):
        return self._driver_id
    
    @property
    def dp_id(self):
        return self._cp_id
    
    @property
    def request_id(self):
        return self._request_id
    
    @request_id.setter
    def request_id(self, new_request_id):
        self._request_id = new_request_id

    def __str__(self):
        return(
            f"{self.cp_id} {self.driver_id} {self.request_id}"
        )

class InformationMessage:

    KEY_NAMES = ["request_id", "type", "cp_id", "start_time", "price_kw", "consumption", "price"]

    def __init__(self, request_id, type, cp_id, start_time, current_time, price_kw, cp_power = 11,):
        self._request_id = request_id
        self._type = type
        self._cp_id = cp_id
        self._start_time = start_time
        self._current_time = current_time
        self._price_kw = price_kw
        self._cp_power = cp_power
        # calculate duration and consumption
        self._duration = self.get_duration()
        self._consumption = self.get_consumption()
        self.price = self.get_price()

    @property
    def request_id(self): return self._request_id
    @property
    def type(self): return self._type
    @property
    def cp_id(self): return self._cp_id
    @property
    def start_time(self): return self._start_time
    @property
    def current_time(self): return self._current_time
    @property
    def price_kw(self): return self._price_kw

    #form:[requestId] [type] [cp_id] [time] [consumption] [price_kw]
    def __str__(self) -> str:
        return (
            f"request_id={self.request_id} type={self.type} cp_id{self.cp_id} "
            f"start_time{self.start_time:.2f} price_kw={self.price_kw:.2f} consumption={self.consumption:.2f} "
            f"price={self.price:.2f}"
        )

    # a method to obtain a dictonary filled with the values from the messages indexed by their names
    @classmethod
    def decode_message(message):
        global KEY_NAMES

        message_dictonary = {}
        for key_name in KEY_NAMES:
            value = InformationMessage.extract_value(key_name, message)
            message_dictonary[key_name] = message

        return message_dictonary
    
    # helper method for decode_message
    @classmethod
    def extract_value(key_name, message):
        pattern = rf'{key_name}=([\d\w\.\-_]+)'
        
        match = re.search(pattern, message)
        
        if match:
            return match.group(1)
        return None

    def get_duration(self) -> float : 
        return self.current_time - self.start_time

    def get_consumption(self) -> float :
        return self.duration * self.power
    
    def get_price(self) -> float :
        consumption = self.get_consumption(self.get_duration(self.start_time, self.time))
        return consumption * self.price_kw

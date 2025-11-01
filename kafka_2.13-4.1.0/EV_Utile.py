import asyncio
import socket
from enum import Enum
import re
import sys # <-- Import sys

# --- Partitioning Config ---
# Must be the same in Central, Driver, and Enginexs
NUM_PARTITIONS = 7 # <-- FIX: Added missing variable

# --- Socket Protocol Constants ---
STX = b'\x02'  # Start of Text
ETX = b'\x03'  # End of Text
FORMAT = 'utf-8'

# --- Socket Protocol Framing ---

def _calculate_lrc(data_bytes: bytes) -> bytes:
    """Calculates the LRC (XOR checksum) for the data."""
    lrc = 0
    for byte in data_bytes:
        lrc ^= byte
    return lrc.to_bytes(1, 'big')

def frame_message(data_str: str) -> bytes:
    """
    Frames a data string into the <stx>D<etx><lrc> protocol.
    """
    data_bytes = data_str.encode(FORMAT)
    lrc_byte = _calculate_lrc(data_bytes)
    return STX + data_bytes + ETX + lrc_byte

class SyncSocketFrameProtocol:
    """
    Handles buffering and de-framing for sync (blocking) sockets.
    Used by Monitor and Engine.
    """
    def __init__(self, sock: socket.socket):
        self._sock = sock
        self._buffer = b''

    def read_message(self) -> str:
        """
        Blocks until one complete, valid <stx>D<etx><lrc> frame is
        received, verified, and returned.
        """
        while True:
            # Check buffer for existing complete frame
            stx_pos = self._buffer.find(STX)
            if stx_pos != -1:
                etx_pos = self._buffer.find(ETX, stx_pos)
                if etx_pos != -1 and etx_pos > stx_pos:
                    frame_end_pos = etx_pos + 2  # for <etx> and <lrc>
                    if len(self._buffer) >= frame_end_pos:
                        
                        # Extract components
                        frame = self._buffer[stx_pos:frame_end_pos]
                        data_bytes = frame[1:-2] # D
                        lrc_byte = frame[-1:]    # <lrc>
                        
                        # Consume from buffer *before* returning
                        self._buffer = self._buffer[frame_end_pos:]
                        
                        # Validate LRC
                        if _calculate_lrc(data_bytes) == lrc_byte:
                            return data_bytes.decode(FORMAT)
                        else:
                            print("SocketProtocol Error: Checksum mismatch. Dropping frame.")
                            continue # Continue loop to find next valid frame
            
            # No complete frame in buffer, read more from socket
            try:
                new_data = self._sock.recv(4096)
                if not new_data:
                    raise ConnectionError("Socket closed by peer.")
                self._buffer += new_data
            except socket.timeout:
                raise socket.timeout # Re-raise for the monitor to catch
            except Exception as e:
                raise ConnectionError(f"Socket read error: {e}")

class AsyncSocketFrameProtocol:
    """
    Handles buffering and de-framing for asyncio sockets.
    Used by Central.
    """
    def __init__(self, reader: asyncio.StreamReader):
        self._reader = reader
        self._buffer = b''

    async def read_messages(self):
        """
        An async generator that yields valid, de-framed messages
        as they arrive.
        """
        while True:
            try:
                # Check buffer for existing complete frame
                stx_pos = self._buffer.find(STX)
                if stx_pos != -1:
                    etx_pos = self._buffer.find(ETX, stx_pos)
                    if etx_pos != -1 and etx_pos > stx_pos:
                        frame_end_pos = etx_pos + 2  # for <etx> and <lrc>
                        if len(self._buffer) >= frame_end_pos:
                            
                            frame = self._buffer[stx_pos:frame_end_pos]
                            data_bytes = frame[1:-2]
                            lrc_byte = frame[-1:]
                            
                            self._buffer = self._buffer[frame_end_pos:]
                            
                            if _calculate_lrc(data_bytes) == lrc_byte:
                                yield data_bytes.decode(FORMAT)
                            else:
                                print("SocketProtocol Error: Checksum mismatch. Dropping frame.")
                            continue # Check buffer again for more frames
                
                # Buffer empty or incomplete, read more
                new_data = await self._reader.read(4096)
                if not new_data:
                    break # Connection closed
                self._buffer += new_data
            except (asyncio.IncompleteReadError, ConnectionResetError):
                break
            except Exception as e:
                print(f"SocketProtocol Error: {e}")
                break

# --- Data Classes ---

class ChargingPointStatus(Enum):
    ACTIVE = 1
    OUT_OF_ORDER = 2
    IN_USAGE = 3
    DEFECT = 4
    DISCONNECTED = 5

class InformationTypeMonitor(Enum):
    AUTHENTICATION = 1
    STATUS_CONFIRMATION = 2 # This is unused, but kept for reference
    STATUS_UPDATE = 3
    ERROR = 4
    ERROR_RESOLVED = 5

class InformationTypeEngine(Enum):
    CHARGING_START = 1
    CHARGING_ONGOING = 2
    CHARGING_END = 3

class RequestMessage:
    REQUEST = "request_id"
    DRIVER = "driver_id"
    CP = "cp_id"
    CONSUMPTION = "consumption"
    PRICE = "price"
    KW = "price_kw"
    KEY_NAMES = [REQUEST, DRIVER, CP, CONSUMPTION, PRICE, KW]

    def __init__(self, cp_id, driver, consumption=0, price=0, price_kw=None, request_id=None):
        self._request_id = request_id
        self._cp_id = cp_id
        self._driver = driver
        self._price_kw = price_kw
        self._consumption = consumption
        self._price = price
    
    @property
    def cp_id(self): return self._cp_id
    @property
    def driver_id(self): return self._driver
    @property
    def consumption(self): return self._consumption
    @property
    def price(self): return self._price
    @property
    def price_kw(self): return self._price_kw
    @property
    def request_id(self): return self._request_id
    @request_id.setter
    def request_id(self, new_request_id): self._request_id = new_request_id

    def __str__(self):
        return (
            f"cp_id={self.cp_id} driver_id={self.driver_id} request_id={self.request_id} "
            f"consumption={self.consumption} price_kw={self.price_kw} price={self.price}"
        )
    
    @classmethod
    def decode_message(cls, message):
        message_dictionary = {}
        for key_name in cls.KEY_NAMES:
            value = cls.extract_value(key_name, message)
            message_dictionary[key_name] = value
        
        # Return as an object
        return cls(
            cp_id=message_dictionary.get(cls.CP),
            driver=message_dictionary.get(cls.DRIVER),
            consumption=message_dictionary.get(cls.CONSUMPTION, 0),
            price=message_dictionary.get(cls.PRICE, 0),
            price_kw=message_dictionary.get(cls.KW),
            request_id=message_dictionary.get(cls.REQUEST)
        )
    
    @classmethod
    def extract_value(cls, key_name, message):
        pattern = rf'{key_name}=([\d\w\.\-_]+)'
        match = re.search(pattern, message)
        if match:
            value = match.group(1)
            if value == 'None': return None
            try: return int(value)
            except ValueError:
                try: return float(value)
                except ValueError: return value
        return None

class InformationMessage:
    REQUEST = "request_id"
    TYPE = "type"
    CP = "cp_id"
    START = "start_time"
    CURRENT = "current_time"
    KW = "price_kw"
    KEY_NAMES = [REQUEST, TYPE, CP, START, CURRENT, KW]

    def __init__(self, request_id, type, cp_id, start_time, current_time, price_kw, cp_power=11.0):
        self._request_id = request_id
        if isinstance(type, InformationTypeEngine):
            self._type = type
        else:
            self._type = InformationTypeEngine(int(type))
        self._cp_id = cp_id
        self._start_time = float(start_time)
        self._current_time = float(current_time)
        self._price_kw = float(price_kw)
        self._cp_power = float(cp_power)
        # Calculated fields
        self._duration = self.get_duration()
        self._consumption = self.get_consumption()
        self._price = self.get_price()

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
    @property
    def cp_power(self): return self._cp_power
    @property
    def duration(self): return self._duration
    @property
    def consumption(self): return self._consumption
    @property
    def price(self): return self._price

    def __str__(self) -> str:
        return (
            f"request_id={self.request_id} type={self.type.value} cp_id={self.cp_id} "
            f"start_time={self.start_time:.2f} current_time={self.current_time:.2f} price_kw={self.price_kw:.2f}"
        )

    @classmethod
    def decode_message(cls, message):
        message_dictionary = {}
        for key_name in cls.KEY_NAMES:
            value = cls.extract_value(key_name, message)
            if value is None:
                 raise ValueError(f"Missing required key in InformationMessage: {key_name} in '{message}'")
            message_dictionary[key_name] = value
        
        # Return as a dictionary
        return message_dictionary
    
    @classmethod
    def extract_value(cls, key_name, message):
        pattern = rf'{key_name}=([\d\w\.\-_]+)'
        match = re.search(pattern, message)
        if match:
            value = match.group(1)
            if value == 'None': return None
            # Try float first for timestamps/prices
            try: return float(value)
            except ValueError:
                try: return int(value)
                except ValueError: return value
        return None

    def get_duration(self) -> float: 
        # Duration in hours
        return (self.current_time - self.start_time) / 3600.0

    def get_consumption(self) -> float:
        # Consumption in kWh
        return self.duration * self.cp_power
    
    def get_price(self) -> float:
        return self.consumption * self.price_kw

class Ticket:
    # Updated regex to be more specific
    TICKET_PATTERN = re.compile(
        r"Ticket for request=(?P<request_id>[\w\d]+) at "
        r"cp (?P<cp_id>[\w\d]+) with consumption=(?P<consumption>[\d\.]+), "
        r"total price=(?P<price>[\d\.]+) for price per kw=(?P<price_kw>[\d\.]+)"
    )

    def __init__(self, request_id, cp_id, consumption, price, price_kw):
        self._request_id = request_id
        self._cp_id = cp_id
        self._consumption = float(consumption)
        self._price = float(price) 
        self._price_kw = float(price_kw)
         
    @property
    def request_id(self): return self._request_id
    @property
    def cp_id(self): return self._cp_id
    @property
    def consumption(self): return self._consumption
    @property
    def price(self): return self._price
    @property
    def price_kw(self): return self._price_kw

    def __str__(self) -> str:
        return (
            f"Ticket for request={self.request_id} at "
            f"cp {self.cp_id} with consumption={self.consumption:.2f}, "
            f"total price={self.price:.2f} for price per kw={self.price_kw}"
        )
    
    @classmethod
    def decode_message(cls, ticket_string: str):
        match = cls.TICKET_PATTERN.search(ticket_string)
        if not match:
            raise ValueError("String does not match Ticket format.")
        
        data = match.groupdict()
        return cls(**data)

class StatusMessage:
    TYPE = "type"
    CP_ID = "cp_id"
    STATUS = "status"
    PRICE_KW = "price_kw"
    # location removed
    KEY_NAMES = [TYPE, CP_ID, STATUS, PRICE_KW]
    
    def __init__(self, type, cp_id, status, price_kw):
        if isinstance(type, InformationTypeMonitor):
            self._type = type
        else:
            self._type = InformationTypeMonitor(int(type))
            
        self._cp_id = str(cp_id)
        
        if isinstance(status, ChargingPointStatus):
            self._status = status
        else:
            self._status = ChargingPointStatus(int(status))
            
        self._price_kw = float(price_kw)

    @property
    def type(self): return self._type
    @property
    def cp_id(self): return self._cp_id
    @property
    def status(self): return self._status
    @property
    def price_kw(self): return self._price_kw
    
    def encode_for_socket(self):
        # location removed
        return (
            f"{self.TYPE}={self.type.value} {self.CP_ID}={self.cp_id} "
            f"{self.STATUS}={self.status.value} {self.PRICE_KW}={self.price_kw}"
        )

    @classmethod
    def decode_message(cls, message_string):
        message_dictionary = {}
        for key_name in cls.KEY_NAMES:
            value = cls.extract_value(key_name, message_string)
            if value is None:
                raise ValueError(f"Missing key in StatusMessage: {key_name} in '{message_string}'")
            message_dictionary[key_name] = value
        
        return cls(
            type=message_dictionary[cls.TYPE],
            cp_id=message_dictionary[cls.CP_ID],
            status=message_dictionary[cls.STATUS],
            price_kw=message_dictionary[cls.PRICE_KW]
        )

    @classmethod
    def extract_value(cls, key_name, message):
        pattern = rf'{key_name}=([\w\d\.\-]+)'
        match = re.search(pattern, message)
        if match:
            value = match.group(1)
            try: return int(value)
            except ValueError:
                try: return float(value)
                except ValueError: return value
        return None

    def __str__(self):
        status_name = self.status.name
        match self.type:
            case InformationTypeMonitor.AUTHENTICATION:
                # location removed
                return (f"CP_AUTH: ID={self.cp_id} is {status_name}. "
                        f"Price: {self.price_kw} EUR/kWh.")
            case InformationTypeMonitor.STATUS_UPDATE:
                return f"CP_UPDATE: ID={self.cp_id} is now {status_name}."
            case InformationTypeMonitor.ERROR:
                return f"CP_ERROR: Charging point {self.cp_id} has reported an error: {status_name}."
            case InformationTypeMonitor.ERROR_RESOLVED:
                return f"CP_RESOLVED: The error at charging point {self.cp_id} has been resolved. Status: {status_name}."
            case _:
                return f"CP_INFO: ID={self.cp_id}, Status={status_name}."



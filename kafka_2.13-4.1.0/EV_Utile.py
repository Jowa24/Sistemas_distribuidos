import asyncio
import socket
from enum import Enum
import re
import sys # <-- Import sys
import sqlite3
from datetime import datetime 
import threading 

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
            # =============================================================================
# DATABASE MANAGER
# =============================================================================

class DatabaseManager:
    def __init__(self, db_path='charging_network.db'):
        self.db_path = db_path
        self._lock = threading.Lock()
        self.init_database()
    
    def init_database(self):
        """Initialize SQLite database with all required tables"""
        with self._lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Charging Points table - aligned with your ChargingPointStatus enum
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS charging_points (
                    id TEXT PRIMARY KEY,
                    location TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'DISCONNECTED',
                    price_per_kwh REAL NOT NULL,
                    current_power REAL DEFAULT 0,
                    current_amount REAL DEFAULT 0,
                    current_driver_id TEXT,
                    last_heartbeat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Drivers table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS drivers (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    email TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Charging Sessions table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS charging_sessions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    cp_id TEXT NOT NULL,
                    driver_id TEXT NOT NULL,
                    start_time TIMESTAMP NOT NULL,
                    end_time TIMESTAMP,
                    total_energy REAL DEFAULT 0,
                    total_cost REAL DEFAULT 0,
                    status TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (cp_id) REFERENCES charging_points (id),
                    FOREIGN KEY (driver_id) REFERENCES drivers (id)
                )
            ''')
            
            # Fault Logs table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS fault_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    cp_id TEXT NOT NULL,
                    fault_type TEXT NOT NULL,
                    description TEXT,
                    reported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    resolved_at TIMESTAMP,
                    status TEXT NOT NULL,
                    FOREIGN KEY (cp_id) REFERENCES charging_points (id)
                )
            ''')
            
            # Insert sample data
            self._insert_sample_data(cursor)
            
            conn.commit()
            conn.close()
    
    def _insert_sample_data(self, cursor):
        """Insert sample data for testing - aligned with your existing enums"""
        # Sample charging points
        sample_cps = [
            ('CP001', 'Downtown Plaza', 0.35),
            ('CP002', 'City Mall Parking', 0.32),
            ('CP003', 'University Campus', 0.30),
            ('CP004', 'Airport Terminal B', 0.40),
            ('CP005', 'Central Station', 0.38)
        ]
        
        for cp_id, location, price in sample_cps:
            cursor.execute('''
                INSERT OR IGNORE INTO charging_points (id, location, price_per_kwh, status)
                VALUES (?, ?, ?, ?)
            ''', (cp_id, location, price, ChargingPointStatus.DISCONNECTED.name))
        
        # Sample drivers
        sample_drivers = [
            ('DRV001', 'Alice Johnson', 'alice@email.com'),
            ('DRV002', 'Bob Smith', 'bob@email.com'),
            ('DRV003', 'Carol Davis', 'carol@email.com')
        ]
        
        for driver_id, name, email in sample_drivers:
            cursor.execute('''
                INSERT OR IGNORE INTO drivers (id, name, email)
                VALUES (?, ?, ?)
            ''', (driver_id, name, email))
    
    def get_charging_points(self):
        """Get all charging points"""
        with self._lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute('''
                SELECT id, location, status, price_per_kwh, current_power, current_amount, current_driver_id
                FROM charging_points
            ''')
            results = cursor.fetchall()
            conn.close()
            
            charging_points = []
            for row in results:
                charging_points.append({
                    'id': row[0],
                    'location': row[1],
                    'status': row[2],
                    'price_per_kwh': row[3],
                    'current_power': row[4],
                    'current_amount': row[5],
                    'current_driver_id': row[6]
                })
            return charging_points
    
    def update_cp_status(self, cp_id, status, driver_id=None):
        """Update charging point status - accepts ChargingPointStatus enum or string"""
        with self._lock:
            # Convert enum to string if needed
            if isinstance(status, ChargingPointStatus):
                status_str = status.name
            else:
                status_str = status
            
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            if driver_id:
                cursor.execute('''
                    UPDATE charging_points 
                    SET status = ?, current_driver_id = ?, last_heartbeat = CURRENT_TIMESTAMP
                    WHERE id = ?
                ''', (status_str, driver_id, cp_id))
            else:
                cursor.execute('''
                    UPDATE charging_points 
                    SET status = ?, current_driver_id = NULL, last_heartbeat = CURRENT_TIMESTAMP
                    WHERE id = ?
                ''', (status_str, cp_id))
            
            conn.commit()
            conn.close()
    
    def update_telemetry(self, cp_id, power, amount, driver_id=None):
        """Update real-time telemetry data"""
        with self._lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            if driver_id:
                cursor.execute('''
                    UPDATE charging_points 
                    SET current_power = ?, current_amount = ?, current_driver_id = ?, last_heartbeat = CURRENT_TIMESTAMP
                    WHERE id = ?
                ''', (power, amount, driver_id, cp_id))
            else:
                cursor.execute('''
                    UPDATE charging_points 
                    SET current_power = ?, current_amount = ?, last_heartbeat = CURRENT_TIMESTAMP
                    WHERE id = ?
                ''', (power, amount, cp_id))
            
            conn.commit()
            conn.close()
    
    def create_charging_session(self, cp_id, driver_id):
        """Start a new charging session"""
        with self._lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO charging_sessions (cp_id, driver_id, start_time, status)
                VALUES (?, ?, CURRENT_TIMESTAMP, 'ACTIVE')
            ''', (cp_id, driver_id))
            session_id = cursor.lastrowid
            conn.commit()
            conn.close()
            return session_id
    
    def complete_charging_session(self, session_id, total_energy, total_cost):
        """Complete a charging session"""
        with self._lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE charging_sessions 
                SET end_time = CURRENT_TIMESTAMP, total_energy = ?, total_cost = ?, status = 'COMPLETED'
                WHERE id = ?
            ''', (total_energy, total_cost, session_id))
            conn.commit()
            conn.close()
    
    def log_fault(self, cp_id, fault_type, description="No description"):
        """Log a fault for a charging point"""
        with self._lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO fault_logs (cp_id, fault_type, description, status)
                VALUES (?, ?, ?, 'REPORTED')
            ''', (cp_id, fault_type, description))
            conn.commit()
            conn.close()
    
    def resolve_fault(self, cp_id):
        """Resolve all active faults for a charging point"""
        with self._lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE fault_logs 
                SET resolved_at = CURRENT_TIMESTAMP, status = 'RESOLVED'
                WHERE cp_id = ? AND status = 'REPORTED'
            ''', (cp_id,))
            conn.commit()
            conn.close()
    
    def get_cp_by_id(self, cp_id):
        """Get charging point by ID"""
        with self._lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute('''
                SELECT id, location, status, price_per_kwh, current_power, current_amount, current_driver_id
                FROM charging_points WHERE id = ?
            ''', (cp_id,))
            result = cursor.fetchone()
            conn.close()
            
            if result:
                return {
                    'id': result[0],
                    'location': result[1],
                    'status': result[2],
                    'price_per_kwh': result[3],
                    'current_power': result[4],
                    'current_amount': result[5],
                    'current_driver_id': result[6]
                }
            return None

# =============================================================================
# DASHBOARD HELPER FUNCTIONS
# =============================================================================

def get_status_display(status):
    """Convert status to display format with emojis"""
    status_map = {
        'ACTIVE': "🟢 AVAILABLE",
        'OUT_OF_ORDER': "🔴 OUT OF ORDER", 
        'IN_USAGE': "🔵 SUPPLYING",
        'DEFECT': "🔴 DEFECT",
        'DISCONNECTED': "⚫ DISCONNECTED"
    }
    return status_map.get(status, "⚫ UNKNOWN")

def print_dashboard(db_manager):
    """Print a console-based dashboard"""
    charging_points = db_manager.get_charging_points()
    
    print("\n" + "="*80)
    print("           EV CHARGING NETWORK - CENTRAL DASHBOARD")
    print("="*80)
    
    stats = {
        'total': len(charging_points),
        'active': len([cp for cp in charging_points if cp['status'] == 'ACTIVE']),
        'in_usage': len([cp for cp in charging_points if cp['status'] == 'IN_USAGE']),
        'faulty': len([cp for cp in charging_points if cp['status'] in ['OUT_OF_ORDER', 'DEFECT']]),
        'disconnected': len([cp for cp in charging_points if cp['status'] == 'DISCONNECTED'])
    }
    
    print(f"📊 STATS: Total: {stats['total']} | Available: {stats['active']} | "
          f"Supplying: {stats['in_usage']} | Faulty: {stats['faulty']} | "
          f"Disconnected: {stats['disconnected']}")
    print("-" * 80)
    
    for cp in charging_points:
        status_display = get_status_display(cp['status'])
        print(f"🔌 {cp['id']} | 📍 {cp['location']:20} | {status_display:25} | 💰 €{cp['price_per_kwh']}/kWh", end="")
        
        if cp['status'] == 'IN_USAGE':
            print(f" | 🔋 {cp['current_power'] or 0:.1f}kW | 💵 €{cp['current_amount'] or 0:.2f} | 👤 {cp['current_driver_id'] or 'Unknown'}")
        else:
            print()
    
    print("="*80)



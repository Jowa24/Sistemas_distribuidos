# adosqlite.py - Custom database abstraction layer for EV Charging System
import sqlite3
import threading
from typing import List, Tuple, Optional

class EVDatabase:
    def __init__(self, db_path: str = "ev_charging.db"):
        self.db_path = db_path
        self._local = threading.local()
        self.setup_database()
    
    def get_connection(self):
        """Get thread-local database connection"""
        if not hasattr(self._local, 'connection'):
            self._local.connection = sqlite3.connect(self.db_path, check_same_thread=False)
            self._local.connection.row_factory = sqlite3.Row
        return self._local.connection
    
    def setup_database(self):
        """Initialize database tables"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Charging Stations table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS charging_stations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                station_id TEXT UNIQUE NOT NULL,
                location TEXT,
                total_ports INTEGER DEFAULT 4,
                available_ports INTEGER DEFAULT 4,
                power_capacity REAL,
                status TEXT DEFAULT 'active',
                last_heartbeat DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Users/Drivers table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS drivers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                driver_id TEXT UNIQUE NOT NULL,
                username TEXT,
                vehicle_type TEXT,
                battery_capacity REAL,
                current_session_id INTEGER
            )
        ''')
        
        # Charging Sessions table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS charging_sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT UNIQUE NOT NULL,
                station_id TEXT,
                driver_id TEXT,
                start_time DATETIME DEFAULT CURRENT_TIMESTAMP,
                end_time DATETIME,
                requested_energy REAL,
                delivered_energy REAL DEFAULT 0,
                status TEXT DEFAULT 'active',
                FOREIGN KEY (station_id) REFERENCES charging_stations (station_id),
                FOREIGN KEY (driver_id) REFERENCES drivers (driver_id)
            )
        ''')
        
        conn.commit()
    
    # Add methods for station management, session handling, etc.
    def register_station(self, station_id: str, location: str, ports: int = 4):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO charging_stations 
            (station_id, location, total_ports, available_ports) 
            VALUES (?, ?, ?, ?)
        ''', (station_id, location, ports, ports))
        conn.commit()
    
    def update_station_heartbeat(self, station_id: str):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            UPDATE charging_stations 
            SET last_heartbeat = CURRENT_TIMESTAMP 
            WHERE station_id = ?
        ''', (station_id,))
        conn.commit()

# Create a global instance
ev_database = EVDatabase()
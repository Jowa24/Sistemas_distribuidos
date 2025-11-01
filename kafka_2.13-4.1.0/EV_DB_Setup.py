import sqlite3
import os # <-- FIX: Import os

# FIX: Define a path inside the mounted volume
DB_PATH = "db_data/EV.db"

def setUpDB() :
    # FIX: Ensure the directory exists
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    
    # FIX: Connect to the correct path
    connection = sqlite3.connect(DB_PATH)

    cursor = connection.cursor()

    # Updated: 'location TEXT' removed
    cursor.execute("""
                CREATE TABLE IF NOT EXISTS charging_points(
                id INTEGER PRIMARY KEY, 
                status INTEGER, 
                priceKW REAL 
                )  
        """)
    connection.commit()

    cursor.execute("""
                CREATE TABLE IF NOT EXISTS requests(
                request_id INTEGER PRIMARY KEY,
                cp_id INTEGER,
                consumption FLOAT,
                price FLOAT,
                price_kw FLOAT,
                driver_id INTEGER,
                done BOOLEAN
                )
        """)
    connection.commit()

    connection.close()

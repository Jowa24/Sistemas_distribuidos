import sqlite3

def setUpDB() :
    connection = sqlite3.connect("EV.db")

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


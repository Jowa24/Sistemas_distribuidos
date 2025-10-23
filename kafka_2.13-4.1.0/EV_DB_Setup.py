import sqlite3

def setUpDB() :
    connection = sqlite3.connect("EV.db")

    cursor = connection.cursor()

    cursor.execute("""
                CREATE TABLE IF NOT EXISTS charging_points(
                id INTEGER, 
                location TEXT, 
                status INTEGER, 
                priceKW REAL, 
                consumption, REAL
                price REAL
                )  
        """)
    connection.commit()

    cursor.execute("""
                CREATE TABLE IF NOT EXISTS requests(
                requestId INTEGER,
                driverId INTEGER,
                cpId INTEGER
                )
        """)
    connection.comit()

    connection.close()
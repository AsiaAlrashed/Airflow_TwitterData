import sqlite3


class WeatherLoader:

    def __init__(self, db_path):
        # create data base if it is not found else connect it
        self.connection = sqlite3.connect(db_path)
        ## Setting up an cursor that allows us to execute commands on the database
        self.cursor = self.connection.cursor()
        self.create_tabels()

    def create_tabels(self):
        # Create locations table with unique location names
        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS locations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE
            )
        """
        )

        # Create weather_data table with composite primary key (location_id, date)
        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS weather_data (
                location_id INTEGER,
                date TEXT,
                temperature REAL,
                humidity REAL,
                weather TEXT,
                wind_speed REAL,
                year INTEGER,
                month INTEGER,
                day INTEGER,
                hour INTEGER,
                PRIMARY KEY (location_id, date),
                FOREIGN KEY (location_id) REFERENCES locations(id)
            )
        """
        )
        self.connection.commit()

    def insert_location(self, location_name):
        # Try to insert the location; if it exists, ignore it
        self.cursor.execute(
            """
        INSERT OR IGNORE INTO locations (name) VALUES (?)
        """,
            (location_name,),
        )
        self.connection.commit()
        self.cursor.execute("SELECT id FROM locations WHERE name = ?", (location_name,))
        # return id location for fetch its data weather in def insert_data_weather
        return self.cursor.fetchone()[0]

    def insert_weather_data(self, location_name, data_frame):
        # Ensure the location is in the locations table
        location_id = self.insert_location(location_name)
        # Insert weather data
        for _, row in data_frame.iterrows():
            self.cursor.execute(
                """
                INSERT OR REPLACE INTO weather_data (location_id, date, temperature, humidity, weather, wind_speed, year, month, day, hour)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    location_id,
                    row["date"],
                    row["temperature"],
                    row["humidity"],
                    row["weather"],
                    row["wind_speed"],
                    row["year"],
                    row["month"],
                    row["day"],
                    row["hour"],
                ),
            )
        self.connection.commit()

    def quruer_weather_data(self):
        print("*************weather_data*********************")
        self.cursor.execute("SELECT * FROM weather_data")
        rows = self.cursor.fetchall()
        # Display each row
        for row in rows:
            print(row)

    def quruer_location(self):
        print("*************locations*********************")
        self.cursor.execute("SELECT * FROM locations")
        rows = self.cursor.fetchall()
        # Display each row
        for row in rows:
            print(row)

    def quruer_weather_date_location(self, location_name):
        print(f"*********************{location_name}************************")
        quruer = """
            SELECT weather_data.* 
            FROM weather_data
            JOIN locations ON weather_data.location_id = locations.id
            WHERE locations.name = ?
        """
        self.cursor.execute(quruer, (location_name,))
        return self.cursor.fetchall()

    # def close_connection(self):
    #     self.connection.close()

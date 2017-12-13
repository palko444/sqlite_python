import sqlite3


class Sql:
    '''Class for sql sensor operations'''

    def __init__(self, sqliteFile):
        self.sqlite_file = sqliteFile
        self.connect = sqlite3.connect(self.sqlite_file)
        self.cursor = self.connect.cursor()
        self.create_tables()

    def create_tables(self):
        '''Create required tables'''
        self.cursor.execute(
            'CREATE TABLE IF NOT EXISTS sensor (rowid INTEGER PRIMARY KEY, sensor_name TEXT)')
        self.cursor.execute(
            'CREATE TABLE IF NOT EXISTS sensor_data (rowid INTEGER PRIMARY KEY AUTOINCREMENT, time TEXT, sensor_id INTEGER, sensor_value REAL)')
        self.connect.commit()

    def get_sensor_ids(self):
        '''get all sensors ID'''
        select = self.cursor.execute('select * from sensor')
        all_records = select.fetchall()
        data = {}
        for key, value in all_records:
            data.update({value: key})
        return data

    def add_sensor(self, sensor_name):
        '''Add sensor to sensors table'''
        self.cursor.execute(
            "INSERT INTO sensor (sensor_name) VALUES (?)", (sensor_name,))
        rowid = self.cursor.lastrowid
        self.connect.commit()
        return rowid

    def is_sensor_in_table(self, sensor_name):
        '''Check if sensor is already in DB'''
        return sensor_name in self.get_sensor_ids()

    def add_data(self, time, sensor_name, sensor_value):
        '''Add data to sql table'''
        sensor_id = 1
        if self.is_sensor_in_table(sensor_name):
            sensor_id = self.get_sensor_ids().get(sensor_name)
        else:
            sensor_id = self.add_sensor(sensor_name)
        self.cursor.execute("INSERT INTO sensor_data (time, sensor_id, sensor_value) VALUES (?, ?, ?)",
                            (time, sensor_id, sensor_value))
        self.connect.commit()

    def get_sql_data(self, hours):
        '''Get sql data for all sensors for specified time'''
        data = self.cursor.execute(
            "select time, sensor_name, sensor_value from sensor_data inner join sensor on sensor.rowid = sensor_data.sensor_id where time >= datetime('now', '-" + hours + " hours');")
        return data.fetchall()

    def close_db(self):
        '''Close DB connection'''
        self.connect.close()

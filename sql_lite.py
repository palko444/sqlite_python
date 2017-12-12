import sqlite3


class Sql:
    '''Class for sql'''

    def __init__(self, sqliteFile):
        self.sqlite_file = sqliteFile
        self.create_tables()

    def get_connect(self):
        '''Create sql connect'''
        return sqlite3.connect(self.sqlite_file)

    def get_cursor(self):
        '''Create sql cursor'''
        return self.get_connect().cursor()

    def create_tables(self):
        '''Create required tables'''
        self.get_cursor().execute(
            'CREATE TABLE IF NOT EXISTS sensor (rowid INTEGER PRIMARY KEY AUTOINCREMENT, sensor_name TEXT)')
        self.get_cursor().execute(
            'CREATE TABLE IF NOT EXISTS sensor_data (rowid INTEGER PRIMARY KEY AUTOINCREMENT, time TEXT, sensor_id INTEGER, sensor_value REAL)')
        self.get_connect().commit()

    def get_sensor_ids(self):
        '''get all sensors ID'''
        select = self.get_cursor().execute('select * from sensor')
        all_records = select.fetchall()
        data = {}
        for key, value in all_records:
            data.update({value: key})
        return data

    def add_sensor(self, sensor_name):
        '''Add sensor to sensors table'''
        connect = self.get_connect()
        cursor = connect.cursor()
        cursor.execute(
            "INSERT INTO sensor (sensor_name) VALUES (?)", (sensor_name,))
        rowid = cursor.lastrowid
        connect.commit()
        return rowid

    def is_sensor_in_table(self, sensor_name):
        '''Check if sensor is already in DB'''
        return sensor_name in self.get_sensor_ids()

    def add_data(self, time, sensor_name, sensor_value):
        '''Add data to sql table'''
        sensor_id = 0
        if self.is_sensor_in_table(sensor_name):
            sensor_id = self.get_sensor_ids().get(sensor_name)
        else:
            sensor_id = self.add_sensor(sensor_name)
        connect = self.get_connect()
        cursor = connect.cursor()
        cursor.execute("INSERT INTO sensor_data (time, sensor_id, sensor_value) VALUES (?, ?, ?)",
                                  (time, sensor_id, sensor_value))
        connect.commit()
        connect.close()



# c.execute('CREATE TABLE IF NOT EXISTS sensors (id INTEGER PRIMARY KEY AUTOINCREMENT, sensorName text)')
# c.execute("INSERT INTO sensors(sensorName) VALUES (?)",(a,))
# ll = c.lastrowid
# #c.execute("INSERT INTO sensors VALUES (?,?)",(ll+1,'lalu'))
# print (ll)
# conn.commit()
# c.execute('select * from sensors')
# print (c.fetchall())
# conn.close()

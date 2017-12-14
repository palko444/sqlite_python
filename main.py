import sql_lite

SQL_DB = sql_lite.Sql('/home/pala/mydb')
# SQL_DB.add_data('2017-12-13 20:00', 's1_teplotaas', '25.3')
# print(SQL_DB.get_sql_data("4"))
print(SQL_DB.get_sql_data("-50 minutes"))
SQL_DB.close_db()

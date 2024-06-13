import mysql.connector

def my_Sql(host, name, password, db_name):
    
    connection = mysql.connector.connect(
        host=host,
        user=name,
        password=password,
        database=db_name
    )
    cursor = None
    if connection.is_connected():
        cursor = connection.cursor()
        print("Connected to MySQL database")
    return connection, cursor
    

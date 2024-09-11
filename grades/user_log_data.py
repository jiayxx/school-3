import pandas as pd
import mysql.connector
from mysql.connector import Error

# MySQL database connection configuration
host = 'localhost'
user = 'root'
password = ''
database_actions = 'user_logs_db'

# Function to create table if it doesn't exist
def create_table_if_not_exists(connection):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS user_logs (
        Timestamp DATETIME,
        User_ID VARCHAR(50),
        IP_Address VARCHAR(50),
        Action VARCHAR(50)
    );
    """
    cursor = connection.cursor()
    try:
        cursor.execute(create_table_query)
        connection.commit()
        print("Table 'user_logs' is ready.")
    except Error as e:
        print(f"Error creating table: {e}")
    finally:
        cursor.close()

# Function to insert data into a MySQL table
def insert_data(df, table_name, connection):
    # Create the MySQL insert query
    placeholders = ', '.join(['%s'] * len(df.columns))
    columns = ', '.join([f'`{col.replace(" ", "_")}`' for col in df.columns])  # Replace spaces in column names with underscores
    sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
    
    cursor = connection.cursor()
    try:
        for row in df.itertuples(index=False):
            cursor.execute(sql, tuple(row))
        connection.commit()
        print(f"Data inserted into {table_name}")
    except Error as e:
        print(f"Error: {e}")
        connection.rollback()
    finally:
        cursor.close()

# Function to fetch and display data from the table
def display_data(connection, table_name):
    query = f"SELECT * FROM {table_name}"
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        rows = cursor.fetchall()
        df = pd.DataFrame(rows, columns=[i[0] for i in cursor.description])
        print("Processed Data:")
        print(df)
    except Error as e:
        print(f"Error fetching data: {e}")
    finally:
        cursor.close()

# Main script
try:
    # Connect to the MySQL database for user logs
    connection = mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=database_actions
    )
    
    if connection.is_connected():
        print("Connected to MySQL database")

        # Create table if it does not exist
        create_table_if_not_exists(connection)

        # Import User Logs Data
        user_logs_data = pd.read_csv('user_logs_data.csv')

        # Optional: Convert 'Timestamp' column to proper datetime format
        user_logs_data['Timestamp'] = pd.to_datetime(user_logs_data['Timestamp'], format='%d/%m/%Y %H:%M')

        # Insert data into MySQL table
        insert_data(user_logs_data, 'user_logs', connection)

        # Display processed data
        display_data(connection, 'user_logs')
    
except Error as e:
    print(f"Error while connecting to MySQL: {e}")

finally:
    if connection.is_connected():
        connection.close()
        print("MySQL connection closed")

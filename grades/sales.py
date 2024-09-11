import pandas as pd
import mysql.connector
from mysql.connector import Error

# MySQL database connection configuration
host = 'localhost'
user = 'root'
password = ''
database_sales = 'sales_db'

# Function to create table if it doesn't exist
def create_table_if_not_exists(connection):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS sales (
        Date DATE,
        Sales_ID VARCHAR(50),
        Product_Name VARCHAR(100),
        Quantity INT,
        Unit_Price DECIMAL(10, 2),
        Total_Amount DECIMAL(10, 2)
    );
    """
    cursor = connection.cursor()
    try:
        cursor.execute(create_table_query)
        connection.commit()
        print("Table 'sales' is ready.")
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
    # Connect to the MySQL database for sales
    connection = mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=database_sales
    )
    
    if connection.is_connected():
        print("Connected to MySQL database")

        # Create table if it does not exist
        create_table_if_not_exists(connection)

        # Import Sales Data
        sales_data = pd.read_csv('sales_data.csv')

        # Optional: Convert 'Date' column to proper date format
        sales_data['Date'] = pd.to_datetime(sales_data['Date'], format='%d/%m/%Y')

        # Insert data into MySQL table
        insert_data(sales_data, 'sales', connection)

        # Display processed data
        display_data(connection, 'sales')

except Error as e:
    print(f"Error while connecting to MySQL: {e}")

finally:
    if connection.is_connected():
        connection.close()
        print("MySQL connection closed")

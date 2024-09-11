import pandas as pd
import mysql.connector
from mysql.connector import Error

# MySQL database connection configuration
host = 'localhost'          # or your database host
user = 'root'               # your MySQL username
password = ''               # your MySQL password
database_grades = 'grades_db'

# Function to create table if it doesn't exist
def create_table_if_not_exists(connection):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS grades (
        Student_ID VARCHAR(50),
        Name VARCHAR(100),
        Subject VARCHAR(100),
        Prelim DECIMAL(5,2),
        Midterm DECIMAL(5,2),
        Final_Exam DECIMAL(5,2),
        PRIMARY KEY (Student_ID, Subject)
    );
    """
    cursor = connection.cursor()
    try:
        cursor.execute(create_table_query)
        connection.commit()
        print("Table 'grades' is ready.")
    except Error as e:
        print(f"Error creating table: {e}")
    finally:
        cursor.close()

# Function to insert data into a MySQL table
def insert_data(df, table_name, connection):
    # Adjust column names to match the table structure
    df.columns = df.columns.str.replace(' ', '_').str.replace('Exam', 'Exam')
    placeholders = ', '.join(['%s'] * len(df.columns))
    columns = ', '.join([f'`{col}`' for col in df.columns])
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

# Function to fetch and display students with grades below 85
def display_students_below_threshold(connection, table_name, threshold):
    query = f"""
    SELECT Student_ID, Name, Subject, Prelim, Midterm, Final_Exam
    FROM {table_name}
    WHERE Prelim < {threshold}
       OR Midterm < {threshold}
       OR Final_Exam < {threshold};
    """
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        rows = cursor.fetchall()
        df = pd.DataFrame(rows, columns=[i[0] for i in cursor.description])
        print(f"Students with Prelim, Midterm, or Final Exam grades below {threshold}:")
        print(df)
    except Error as e:
        print(f"Error fetching data: {e}")
    finally:
        cursor.close()

# Main script
try:
    # Connect to the MySQL database for grades
    connection = mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=database_grades
    )
    
    if connection.is_connected():
        print("Connected to MySQL database")

        # Create table if it does not exist
        create_table_if_not_exists(connection)

        # Import Grades Data
        grades_data = pd.read_csv('grades_data.csv')

        # Insert data into MySQL table
        insert_data(grades_data, 'grades', connection)

        # Display students with Prelim, Midterm, or Final Exam grades below 85
        display_students_below_threshold(connection, 'grades', threshold=85)

except Error as e:
    print(f"Error while connecting to MySQL: {e}")

finally:
    if connection.is_connected():
        connection.close()
        print("MySQL connection closed")

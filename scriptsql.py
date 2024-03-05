# # Install DCMAN 2 Database.
#
# import os
# import sys
# import mysql.connector
#
# # Configuration for MySQL connection
# host = 'localhost'
# user = 'root'
# password = 'password'
# database = 'try'
#
# # Path to the SQL script file
# # sql_file = 'dcman2.sql'
#
# # Connect to MySQL
# try:
#     connection = mysql.connector.connect(
#         host=host,
#         user=user,
#         password=password
#     )
#     cursor = connection.cursor()
#
#     # Check if the database exists
#     cursor.execute(f"SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '{database}'")
#     result = cursor.fetchone()
#
#     print("Choose among the available actions:")
#     print("1:Create database.")
#     print("2:Drop database.")
#     print("3:Update the table.")
#     choice = int(input("Select your choice: "))
#     if choice == 1:
#         if result:
#            # Database exists, exit with notification
#             print(f"The '{database}' database already exists.")
#
#         # Create the database
#         else:
#             cursor.execute(f"CREATE DATABASE {database}")
#             print(f"The '{database}' database has been created.")
#
#         # # Restore the database using the SQL script file
#         # script_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), sql_file)
#         # with open(script_path, 'r', encoding='latin-1') as file:
#         #     sql_script = file.read()
#         #
#         # cursor.execute(sql_script)
#         # print(f"The '{database}' database has been restored.")
#
#     else:
#         if result:
#             # Database exists, exit with notification
#             print(f"The '{database}' database already exists.")
#             # Drop the database
#             cursor.execute(f"DROP DATABASE {database}")
#             print(f"The '{database}' database has been dropped.")
#         else:
#             print(f"The '{database}' database does not exist.")
#
#
# except mysql.connector.Error as error:
#     print(f"Error: {error}")
#
# finally:
#     # Close the database connection
#     if connection.is_connected():
#         cursor.close()
#         connection.close()

import os
import mysql.connector

# Configuration for MySQL connection
host = 'localhost'
user = 'root'
password = 'password'
database = 'try'

# Path to the SQL script file
# sql_file = 'dcman2.sql'

def create_database():
    cursor.execute(f"CREATE DATABASE {database}")
    # cursor.execute(f"USE {database}")  # Set the default database
    print(f"The '{database}' database has been created.")

    # Restore the database using the SQL script file
    # script_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), sql_file)
    # with open(script_path, 'r', encoding='utf-8') as file:
    #     sql_script = file.read()

    # cursor.execute(sql_script)
    # print(f"The '{database}' database has been restored.")

def drop_database():
    cursor.execute(f"DROP DATABASE {database}")
    print(f"The '{database}' database has been dropped.")

def show_databases():
    cursor.execute("show databases")
    for i in cursor:
        print(i)
    # return i

def use_database():
    cursor.execute(f"USE {database}")
    print("Database changed")

def create_Table(query):
    cursor.execute(query)

def show_Table():
    cursor.execute("show tables")
    print(f"The databases are")
    for i in db:
        print(i)


def insert_data(query,val):
    # Implement the INSERT logic here
    cursor.execute(query,val)
    connection.commit()

def select(query):
    cursor.execute(query)
    resultSet = db.fetchall()
    return resultSet

def update_table(query):
    # Implement the UPDATE logic here
    cursor.execute(query)

def drop_Table(query):
    # Implement the DELETE logic here
    cursor.execute(query)

# Connect to MySQL
try:
    connection = mysql.connector.connect(
        host=host,
        user=user,
        password=password
    )
    cursor = connection.cursor()

    # Check if the database exists
    cursor.execute(f"SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '{database}'")
    result = cursor.fetchone()

    print("Available actions:")
    print("1. Create the database")
    print("2. Drop the database")
    print("3. Show database")
    print("4. Select database")
    print("5. Show Tables")
    print("6. Create table")
    print("7. Insert values")
    print("8. Update table")
    print("9. Drop table")
    choice = int(input("Enter your choice: "))
    use_database()
    if choice == 1:
        if result:
            print(f"The '{database}' database already exists.")
        else:
            create_database()

    elif choice == 2:
        if result:
            drop_database()
        else:
            print(f"The '{database}' database does not exist.")

    elif choice == 3:
        if result:
            show_databases()

    elif choice == 4:
        if result:
            use_database()
        else:
            print("No database selected")

    elif choice == 5:
        if result:
            show_Table()

    elif choice == 6:
        if result:
            create_Table("CREATE TABLE IF NOT EXISTS employees(empID INT PRIMARY KEY, name VARCHAR(250), age INT)")
            # insert_data("INSERT into employees(empID,name) VALUES (%s,%s)", ("1", "Anu"))
            print("Table created")
        else:
            print(f"The table already exists.")


    elif choice == 7:
        if result:
            insert_data("INSERT into employees(empID,name) VALUES (%s,%s)", ("1","Anu"))
            print("Data got inserted")
        else:
            print("The table doesnot exist")

    elif choice == 8:
        if not result:
            print(f"The '{database}' database does not exist. Please create it first.")
        else:
            update_data()

    elif choice == 9:
        if result:
            drop_Table("Drop table if exists employees")

    else:
        print("Invalid choice. Please enter a valid option (1/2/3/4/5).")

except mysql.connector.Error as error:
    print(f"Error: {error}")

finally:
    # Close the database connection
    if 'connection' in locals() and connection.is_connected():
        cursor.close()
        connection.close()



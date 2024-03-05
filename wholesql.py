import mysql.connector
from mysql.connector import Error
from tabulate import tabulate
import pandas as pd
import subprocess

def connect_to_mysql(username,password):
    try:
        connection = mysql.connector.connect(
            host='localhost',
            user=username,
            password=password
        )
        if connection.is_connected():
            print("Connected to MySQL!")
            return connection,username,password
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None,None,None

def dump(connection,username,password,database,sql_file_path):
    command = f" mysql -u {username} --password={password} {database} < {sql_file_path}"
    try:
        cursor = connection.cursor()
        subprocess.run(command, shell=True, check=True)
        print("SQL file successfully imported.")
    except subprocess.CalledProcessError as e:
        print(f"Error occurred: {e}")

def show_databases(connection):
    try:
        cursor = connection.cursor()
        cursor.execute(f"SHOW DATABASES")
        db = cursor.fetchall()
        print(tabulate(db,headers=cursor.column_names,tablefmt='grid'))

    except Error as e:
        print(f"Error in showing database: {e}")

def create_database(connection, database_name):
    try:
        cursor = connection.cursor()
        cursor.execute(f"CREATE DATABASE {database_name}")
        print(f"Database '{database_name}' created successfully.")
    except Error as e:
        print(f"Error creating database: {e}")

def drop_database(connection, database_name):
    try:
        cursor = connection.cursor()
        cursor.execute(f"DROP DATABASE IF EXISTS {database_name}")
        print(f"Database '{database_name}' dropped successfully.")
    except Error as e:
        print(f"Error dropping database: {e}")

def show_table(connection,db_name):
    query0 = f"USE {db_name}"
    try:
        cursor = connection.cursor()
        cursor.execute(query0)
        # print(f"Database changed to {db_name}")
        print("The tables are: ")
        cursor.execute(f"SHOW TABLES")
        db = cursor.fetchall()
        print(tabulate(db,headers=cursor.column_names,tablefmt='grid'))
    except Error as e:
        print(f"Error in showing database: {e}")

def create_table(connection, db_name, table_name, table_schema):
    query0 = f"USE {db_name}"
    query = f"CREATE TABLE {table_name} ({table_schema})"
    try:
        cursor = connection.cursor()
        cursor.execute(query0)
        print(f"Database changed to {db_name}")
        cursor.execute(query)
        print(f"Table '{table_name}' created successfully.")
    except Error as e:
        print(f"Error creating table: {e}")

def update_table(connection, db_name, table_name,condition_column, condition_value, set_column=None, set_value=None,choice=None):
    query0 = f"USE {db_name}"
    query = f"UPDATE {table_name} SET {set_column} = %s WHERE {condition_column} = %s"
    values = (set_value, condition_value)
    query2 = f"DELETE FROM {table_name} WHERE {condition_column} = %s"
    values2 = (condition_value,)
    try:
        cursor = connection.cursor()
        cursor.execute(query0)
        print(f"Database changed to {db_name}")
        if choice == 1:
            cursor.execute(query, values)
            print(f"Table '{table_name}' updated successfully.")
        else:
            cursor.execute(query2, values2)
            print(f"Row deleted from {table_name}.")
        connection.commit()
    except Error as e:
        print(f"Error updating table: {e}")

def insert_into_table(connection, db_name, table_name, columns, values_list):
    query0 = f"USE {db_name}"
    query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))})"
    try:
        cursor = connection.cursor()
        cursor.execute(query0)
        print(f"Database changed to {db_name}")
        cursor.executemany(query, values_list)
        print(f"Data inserted into '{table_name}' successfully.")
        connection.commit()
    except Error as e:
        print(f"Error inserting data: {e}")

def select(connection, db_name, columns, table_name,condition=None,choice=None):
    query0 = f"USE {db_name}"
    # show_table(connection,db_name)
    query = f"SELECT {columns} FROM {table_name}"
    query2 = f"SELECT {columns} FROM {table_name} WHERE {condition}"
    try:
        cursor = connection.cursor()
        cursor.execute(query0)
        print(f"Database changed to {db_name}")
        if choice == 1:
            cursor.execute(query)
        else:
            cursor.execute(query2)
        result = cursor.fetchall()
        column_names = [desc[0] for desc in cursor.description]
        print(tabulate(result, headers=cursor.column_names,tablefmt='grid'))
        # for row in result:
        #     print(row)
        print("Successfully selected")
    except Error as e:
        print(f"Error while selecting data: {e}")

def alter_table(connection, db_name, table_name, column_name,datatype=None,choice=None):
    query0 = f"USE {db_name}"
    query1 = f"ALTER TABLE {table_name} ADD {column_name} {datatype}"
    query2 = f"ALTER TABLE {table_name} DROP COLUMN {column_name}"
    query3 = f"ALTER TABLE {table_name} MODIFY COLUMN {column_name} {datatype}"
    try:

            cursor = connection.cursor()
            cursor.execute(query0)
            print(f"Database changed to {db_name}")
            if choice == 1:
                cursor.execute(query1)
                print(f"{column_name} added successfully.")
            elif choice == 2:
                cursor.execute(query2)
                print(f"{column_name} dropped successfully.")
            elif choice == 3:
                cursor.execute(query3)
                print(f"{column_name} modified successfully.")
            else:
                print("Invalid entry")
            connection.commit()
    except Error as e:
        print(f"Error updating table: {e}")

def drop_table(connection, db_name, table_name):
    query0 = f"USE {db_name}"
    query = f"DROP TABLE IF EXISTS {table_name}"
    try:
        cursor = connection.cursor()
        cursor.execute(query0)
        print(f"Database changed to {db_name}")
        cursor.execute(query)
        print(f"'{table_name}' is dropped successfully.")
    except Error as e:
        print(f"Error dropping data: {e}")

def main():
    username = input("Enter your username: ")
    password = input("Enter your password: ")
    connection,username,password = connect_to_mysql(username,password)
    if not connection:
        return

    try:
        while True:
            print("Available actions: \n'DS' : Dump sql file to a database \n'SD' : show database' \n'C' : create a database \n'D' : drop a database \n'ST' : show tables \n'T' : create a table \n'U' : update a table \n'I' : insert into a table, \n 'S' : select from table \n'A' : alter table, \n'DT' : drop a table \n'EXIT' : exit from program")
            user_input = input("Enter your choice: ").upper()

            if user_input == "DS":
                database = input("Enter the db to which the file is to imported: ")
                query0 = f"USE {database}"
                cursor = connection.cursor()
                cursor.execute(query0)
                sql_file_path = input("Enter the path to sql file in local system: ")
                dump(connection,username,password,database,sql_file_path)

            elif user_input == "SD":
                show_databases(connection)

            elif user_input == "C":
                db_name = input("Enter the name for the new database: ")
                create_database(connection, db_name)

            elif user_input == "D":
                db_name = input("Enter the name of the database to drop: ")
                drop_database(connection, db_name)

            elif user_input == "ST":
                db_name = input("Enter database name: ")
                show_table(connection, db_name)

            elif user_input == "T":
                db_name = input("Enter the database name: ")
                table_name = input("Enter the name for the new table: ")
                table_schema = input("Enter the table schema (column1 datatype1, column2 datatype2, ...): ")
                create_table(connection, db_name, table_name, table_schema)

            elif user_input == "U":
                db_name = input("Enter the database name: ")
                while True:
                    print("Available actions: \n1. Update table with condition \n2. Delete row from table based on condition")
                    choice = int(input("Enter the choice:"))
                    if choice == 1:
                        table_name = input("Enter the name of the table to update: ")
                        set_column = input("Enter the column to set: ")
                        set_value = input("Enter the new value: ")
                        condition_column = input("Enter the condition column: ")
                        condition_value = input("Enter the condition value: ")
                        update_table(connection, db_name, table_name, set_column, set_value, condition_column, condition_value,choice=1)
                    else:
                        table_name = input("Enter the name of the table to update: ")
                        condition_column = input("Enter the condition column: ")
                        condition_value = input("Enter the condition value: ")
                        update_table(connection, db_name, table_name, condition_column,condition_value, choice=2)

                    quest = input("Do you want to update again (y/n): ")
                    if quest != 'y':
                        break

            elif user_input == "I":
                db_name = input("Enter the database name: ")
                table_name = input("Enter the name of the table to insert into: ")
                columns = input("Enter the columns (comma-separated): ").split(",")

                values_list = []
                while True:
                    values_input = input("Enter the values for a row (comma-separated) or 'done' to finish: ")
                    if values_input.lower() == "done":
                        break
                    values = tuple(values_input.split(","))
                    values_list.append(values)
                insert_into_table(connection, db_name, table_name, columns, values_list)

            elif user_input == "S":
                db_name = input("Enter the database name: ")
                show_table(connection,db_name)
                while True:
                    print("Available select statements: \n1. Without condition \n2. With condition")
                    choice = int(input("Select your choice: "))
                    if choice == 1:
                        table_name = input("Enter the name of table to be selected: ")
                        columns = input("Enter the column to be selected: ")
                        select(connection,db_name,columns,table_name,choice=1)
                    else:
                        table_name = input("Enter the name of table to be selected: ")
                        columns = input("Enter the column to be selected: ")
                        condition = input("Enter the condition: ")
                        select(connection, db_name, columns, table_name, condition,choice=2)

                    quest = input("DO you want to select again (y/n): ")
                    if quest != 'y':
                        break

            elif user_input == 'A':
                db_name = input("Enter the database name: ")
                while True:
                    print("Available actions under alter command: \n1. Add column \n2. Drop column \n3. Modify column ")
                    choice = int(input("Enter the choices: "))
                    if choice == 1:
                        table_name = input("Enter the name of the table to insert into: ")
                        column_name = input("Enter the column name to add: ")
                        datatype = input("Enter the datatype: ")
                        alter_table(connection,db_name,table_name,column_name,datatype,choice=1)

                    elif choice ==2:
                        table_name = input("Enter the name of the table: ")
                        column_name = input("Enter the column name to drop: ")
                        alter_table(connection, db_name, table_name, column_name,choice=2)

                    elif choice == 3:
                        table_name = input("Enter the name of the table to be modified: ")
                        column_name = input("Enter the column name to modify: ")
                        datatype = input("Enter the datatype: ")
                        alter_table(connection,db_name,table_name,column_name,datatype,choice=3)

                    quest = input("DO you want to alter again (y/n): ")
                    if quest != 'y':
                        break

            elif user_input == "DT":
                db_name = input("Enter the database name: ")
                table_name = input("Enter the name of the table to drop: ")
                drop_table(connection, db_name, table_name)

            elif user_input =='EXIT':
                break

            else:
                print("Invalid input. Please try again.")

    finally:
        connection.close()

if __name__ == "__main__":
    main()

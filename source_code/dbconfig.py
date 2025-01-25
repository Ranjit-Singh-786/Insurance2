import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
from source_code.logger import logging
from source_code.exception import InsuranceException 
import pandas as pd
from pymongo import MongoClient
import os , sys 
load_dotenv()

mysql_user = os.getenv("mysql_user")
mysql_password = os.getenv("mysql_user_password")
mysql_database_name = os.getenv("mysql_database_name")

mongodb_connection_string = os.getenv("mongodb_connection_string")

def connect_to_mongodb():
    try:
        client = MongoClient(mongodb_connection_string) 
        print("successfully connected with mongodb")
        return client 
    except Exception as e: 
        custom_exception = InsuranceException(e,sys)
        logging.error(f'Unable to connect with mongodb : {custom_exception.error_message}')
        raise InsuranceException(e,sys)
        return None 

def connect_to_mysql():
    try:
        # Connect to the MySQL server
        connection = mysql.connector.connect(
            host='localhost',      
            user=mysql_user,           
            password=mysql_password,       
            database=mysql_database_name, # Replace with your database name
            allow_local_infile=True  # Enable loading local files
        )
        logging.info("connecting to with database..")
        
        if connection.is_connected():
            print("Connected to MySQL Server")
            logging.info("Successfully connected with database!")
            return connection
    except Error as e:
        custom_exception = InsuranceException(e,sys)
        logging.error(custom_exception.error_message)
        raise InsuranceException(e,sys)
        return None 





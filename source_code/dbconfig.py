import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
from source_code.logger import logging
from source_code.exception import InsuranceException 
import pandas as pd
from pymongo import MongoClient
import os , sys 
import typing
load_dotenv()


def connect_to_mongodb(mongodb_connection_string:str):
    try:
        client = MongoClient(mongodb_connection_string) 
        print("successfully connected with mongodb")
        return client 
    except Exception as e: 
        custom_exception = InsuranceException(e,sys)
        logging.error(f'Unable to connect with mongodb : {custom_exception.error_message}')
        raise InsuranceException(e,sys)
        return None 

def connect_to_mysql(mysql_user:str,mysql_password:str,mysql_database_name:str):
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





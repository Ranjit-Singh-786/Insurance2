from source_code.logger import logging 
from source_code.exception import InsuranceException 
from source_code.dbconfig import connect_to_mongodb 
from source_code.dbconfig import connect_to_mysql
from source_code.entity import config_entity,artifact_entity
import os, sys 


class DataIngestion:
    def __init__(self,dataingestionconfig_obj:config_entity.DataInegestinConfig):
        try:
            self.dataingestion_config_obj = dataingestionconfig_obj
        except Exception as e:
            raise InsuranceException(e,sys)

    
    ### loading the dataset 
    def loading_datasets(self):
        """
        This is used to load the dataset form the multiple database, like 
        (Mysql and Mongodb).
        """
        try:
            mongo_connection = connect_to_mongodb(mongodb_connection_string=self.dataingestion_config_obj.mongodb_connection_string) 
            logging.info("connected with mongodb")

            mongodb_database = mongo_connection[self.dataingestion_config_obj.mongodb_database_name]
            mongo_collection = mongodb_database[self.dataingestion_config_obj.mongodb_collection_name]
            mongo_documents = mongo_collection.find()
            logging.info("Succssfully loaded data from mongodb atlas")


             # mysql_connection = connect_to_mysql(mysql_user=self.dataingestion_obj.mysql_user,
            #                                     mysql_password=self.dataingestion_obj.mysql_password,
            #                                     mysql_database_name=self.dataingestion_obj.myssql_databse_name)



        except Exception as e:
            raise InsuranceException(e,sys)
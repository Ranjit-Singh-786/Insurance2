from source_code.logger import logging 
from source_code.exception import InsuranceException 
from datetime import datetime
import os, sys 

class TrainingPipelineConfig:
    def __init__(self):
        try:
            logging.info("initialaizing training pipeline aritfact directory.")
            self.artifact_dir = os.path.join(os.getcwd(),"Artifacts",datetime.now().strftime("%d-%m-%y %H-%M-%S"))
        except Exception as e:
            obj = InsuranceException(e,sys)
            logging.info(obj.error_message)
            raise InsuranceException(e,sys) 

class DataInegestinConfig:
    def __init__(self,traininng_pipeline_config_obj:TrainingPipelineConfig):
        try:
            logging.info("Intialaizing DataInegestinConfig variables..")
            self.dataingestion_dir =  os.path.join(traininng_pipeline_config_obj.artifact_dir,"data ingestion")
            self.dataset_path = os.path.join(self.dataingestion_dir,"Dataset")
            self.mongodb_connection_string = os.getenv("mongodb_connection_string")
            self.mongodb_database_name = os.getenv("mongodb_database_name") 
            self.mongodb_collection_name = os.getenv("mongodb_collection_name")
            self.test_size = 0.2
            self.dataset_file_name = "insurance.csv"
            self.train_set_file_name = "train.csv"
            self.test_set_file_name = "test.csv"


            self.host = os.getenv('mysql_host')
            self.mysql_user = os.getenv('mysql_user')
            self.mysql_password = os.getenv('mysql_user_password')
            self.mysql_database= os.getenv('mysql_database')
        except Exception as e:
            raise InsuranceException(e,sys)

class DataValidataionConfig:
    def __init__(self,traininng_pipeline_config_obj:TrainingPipelineConfig):
        try:
            self.datavalidation_dir = os.path.join(traininng_pipeline_config_obj.artifact_dir,"Data validation") 
            self.valid_data_file_path = os.path.join(self.datavalidation_dir,"valid_data.csv")
            self.invalid_data_file_path = os.path.join(self.datavalidation_dir,"invalid_row_data.csv")

        except Exception as e:
            raise InsuranceException(e,sys)


        

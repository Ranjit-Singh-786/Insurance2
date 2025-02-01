from source_code.logger import logging
from source_code.exception import InsuranceException 
from source_code.entity import config_entity
from source_code.dbconfig import connect_to_mongodb
from source_code.utils import is_mongo_connected
from source_code.components.data_ingestion import DataIngestion
from source_code.components.data_validation import DataValidation
import  sys,os 


training_pipelinge_obj = config_entity.TrainingPipelineConfig()
dataingestion_config_obj = config_entity.DataInegestinConfig(traininng_pipeline_config_obj=training_pipelinge_obj)

dataingestion_obj = DataIngestion(dataingestionconfig_obj=dataingestion_config_obj)

data_ingestion_artifact =  dataingestion_obj.loading_datasets()

data_validation_config =  config_entity.DataValidataionConfig(traininng_pipeline_config_obj=training_pipelinge_obj)
datavalidation_obj =  DataValidation(data_validation_config=data_validation_config,
                        data_ingestion_artifact=data_ingestion_artifact)


data_validation_artifact =  datavalidation_obj.initiate_datavalidation()
print("successfully done your data validation")
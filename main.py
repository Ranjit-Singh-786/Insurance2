from source_code.logger import logging
from source_code.exception import InsuranceException 
from source_code.entity import config_entity
from source_code.dbconfig import connect_to_mongodb
from source_code.utils import is_mongo_connected
from source_code.components.data_ingestion import DataIngestion
import  sys,os 
# logging.info("hii this is ranjit")
# logging.debug("hii this is ranjit")
# logging.warning("hii this is ranjit")
# logging.error("hii this is ranjit")
# logging.critical("hii this is ranjit")


# try:
#     10/0
# except Exception as e:
#     obj =  InsuranceException(error_message=e,error_detail=sys)
#     logging.info("hii this is ranjit")
#     logging.warning(obj.error_message)
#     print(obj.error_message)

training_pipelinge_obj = config_entity.TrainingPipelineConfig()
dataingestion_config_obj = config_entity.DataInegestinConfig(traininng_pipeline_config_obj=training_pipelinge_obj)

dataingestion_obj = DataIngestion(dataingestionconfig_obj=dataingestion_config_obj)

mongo_connection = connect_to_mongodb(mongodb_connection_string=dataingestion_config_obj.mongodb_connection_string) 

if is_mongo_connected(mongo_connection):
    dataingestion_obj.loading_datasets()
else:
    print("not connected")


# os.makedirs(training_pipelinge_obj.artifact_dir,exist_ok=True)


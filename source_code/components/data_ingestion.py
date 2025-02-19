from source_code.logger import logging 
from source_code.exception import InsuranceException 
from source_code.dbconfig import connect_to_mongodb 
from source_code.dbconfig import connect_to_mysql
from source_code.entity import config_entity,artifact_entity
import os, sys 
import pandas as pd 
from sklearn.model_selection import train_test_split 
import warnings 
warnings.filterwarnings('ignore')


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
        (Mysql and Mongodb) and saved the data at a local directory in artifact.
        """
        try:

            sql_connection=connect_to_mysql(host=self.dataingestion_config_obj.host,
                                            mysql_user=self.dataingestion_config_obj.mysql_user,
                                            mysql_password=self.dataingestion_config_obj.mysql_password,
                                            mysql_database_name=self.dataingestion_config_obj.mysql_database)

            logging.info("connect with mysql ")

            query = "SELECT * FROM insurance_data_sql"
            df1 = pd.read_sql(query, sql_connection)
            df1.drop('id',axis=1,inplace=True)

            mongo_connection = connect_to_mongodb(mongodb_connection_string=self.dataingestion_config_obj.mongodb_connection_string) 
            logging.info("connected with mongodb")

            mongodb_database = mongo_connection[self.dataingestion_config_obj.mongodb_database_name]
            mongo_collection = mongodb_database[self.dataingestion_config_obj.mongodb_collection_name]
            mongo_documents = mongo_collection.find()
            logging.info("Succssfully loaded data from mongodb atlas")
            documents = list(mongo_documents)
            documents_df = pd.DataFrame(documents) 
            documents_df.drop('_id',axis=1,inplace=True)


        ## merging  sql  and mongdb database 
            merged_df = pd.concat([df1,documents_df])
            logging.info(f"collected mongodb : {documents_df.shape}")
            logging.info(f"collected sql data : {df1.shape}")
            logging.info(f"Your sql and  mongodb data is merged : {merged_df.shape}")

            train_df , test_df = train_test_split(merged_df,
                        test_size=self.dataingestion_config_obj.test_size,
                        random_state=42)

            os.makedirs(self.dataingestion_config_obj.dataingestion_dir,exist_ok=True)

            os.makedirs(self.dataingestion_config_obj.dataset_path,exist_ok=True)

    ## 
            dataset_file_path = os.path.join(self.dataingestion_config_obj.dataset_path,
                                self.dataingestion_config_obj.dataset_file_name)

            train_file_path = os.path.join(self.dataingestion_config_obj.dataset_path,
                                            self.dataingestion_config_obj.train_set_file_name)
            test_file_path = os.path.join(self.dataingestion_config_obj.dataset_path,
                                        self.dataingestion_config_obj.test_set_file_name)

            merged_df.to_csv(dataset_file_path,index=False)
            logging.info(f"successfully saved dataset : {dataset_file_path}")

            train_df.to_csv(train_file_path,index=False)
            logging.info(f"successfully saved train data : {train_file_path}")

            test_df.to_csv(test_file_path,index=False)
            logging.info(f"successfully saved test data : {test_file_path}")


            dataingestion_artifact =  artifact_entity.DataIngestArtifact(
                                    Dataset_file_path=dataset_file_path,
                                    Train_file_path=train_file_path,
                                    Test_file_path=test_file_path
            )
            return dataingestion_artifact


        except Exception as e:
            raise InsuranceException(e,sys)
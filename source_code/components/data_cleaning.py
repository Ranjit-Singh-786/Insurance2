from source_code.logger import logging 
from source_code.exception import InsuranceException 
from source_code.entity import config_entity,artifact_entity
import  os , sys 
import pandas as pd 

class DataCleaning:
    def __init__(self,datacleaning_config_obj:config_entity.DataCleaningConfig,
                datavalidation_artifact:artifact_entity.DataValidationArtifact):
        try:
            self.datacleaning_config_obj = datacleaning_config_obj 
            self.datavalidation_artifact = datavalidation_artifact
        except Exception as e:
            raise InsuranceException(e,sys)


    def load_validate_data(self)->pd.DataFrame:
        try:
            df = pd.read_csv(self.datavalidation_artifact.valid_data_file_path)
            return df 
        except Exception as e:
            raise InsuranceException(e,sys)
            return None 


    # Remove Irrelevant Features already  done
    # Remove Duplicate Data 
    def remove_duplicate(self,validate_df)->pd.DataFrame:
        try:
            total_duplicated_rows = validate_df.duplicated().sum()
            if total_duplicated_rows >0:
                logging.info(f"Duplicate row found : {total_duplicated_rows}")
                validate_df.drop_duplicates(inplace=True) 
                logging.info("Duplicate row removed.")
                return validate_df
            else:
                logging.info("No duplicates row found")
                return validate_df 
        except Exception as e:
            raise InsuranceException(e,sys)


    # Handle Missing Data 
    def missing_value_handle(self,df:pd.DataFrame)->pd.DataFrame:
        try:
            missing_value_report =  df.isnull().sum().to_dict()
            logging.info(f"Missing value report : {missing_value_report}")
            total_no_of_missing_record = df.isnull().sum().sum()
            total_no_of_missing_rows = df.isnull().any(axis=1).sum() 

            # REMOVE MISSING RECORDS
            df.dropna(inplace=True)
            # IMPUTATION MISSING RECORDS 
            return df 
        except Exception as e:
            raise InsuranceException(e,sys)
        
    # Handle Outliers 
    # def outlier_handle(self,df: pd.DataFrame)->pd.DataFrame:
    #     try:
    #         numerical_col_df = df.select_dtypes(exclude='O') 

    #     except Exception as e:
    #         raise InsuranceException(e,sys)

    # Handle Inconsistent Data already done 

    def DataCleaningInitiate(self)->artifact_entity.DataCleaningArtifact:
        try:
            df = self.load_validate_data() 
            df = self.remove_duplicate(validate_df=df)
            df = self.missing_value_handle(df=df)
            logging.info("Data cleaning done")

            # save the clean data 
            os.makedirs(self.datacleaning_config_obj.data_cleaning_dir,exist_ok=True)
            df.to_csv(self.datacleaning_config_obj.clean_data_file_path,index=False) 


            datacleaning_artifact = artifact_entity.DataCleaningArtifact(
                clean_data_file_path=self.datacleaning_config_obj.clean_data_file_path
            )
            return datacleaning_artifact

        except Exception as e:
            raise InsuranceException(e,sys)




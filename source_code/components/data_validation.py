from source_code.logger import logging 
from source_code.entity import config_entity,artifact_entity 
from source_code.exception import InsuranceException
import pandas as pd
import os , sys 


# ✅ Sex: Must be "male" or "female".
# ✅ BMI: Must be a positive number.
# ✅ Children: Must be a non-negative integer.
# ✅ Smoker: Must be "yes" or "no".
# ✅ Region: Must be one of "southeast", "southwest", "northeast", "northwest".
# ✅ Charges: Must be a non-negative number.

class DataValidation:
    def __init__(self,data_validation_config:config_entity.DataValidataionConfig,
                    data_ingestion_artifact:artifact_entity.DataIngestArtifact):
        try:
            self.DataValidationConfig_obj =  data_validation_config
            self.DataIngestionArtifact_obj = data_ingestion_artifact
        except Exception as e:
            raise InsuranceException(e,sys)

    # Load the dataset
    def load_data(self):
        try:
            dataset_file_path = self.DataIngestionArtifact_obj.Dataset_file_path
            df = pd.read_csv(dataset_file_path)
            logging.info("Loaded data from ingestion to perform data validation.")
            return df 
        except Exception as e:
            raise InsuranceException(e,sys)



    # Define validation functions
    @staticmethod
    def validate_sex(value):
        return value in ["male", "female"]

    @staticmethod
    def validate_bmi(value):
        return isinstance(value, (int, float)) and value > 0 and value <= 40

    @staticmethod
    def validate_children(value):
        return isinstance(value, int) and value >= 0 and value <= 5

    @staticmethod
    def validate_age(value):
        return isinstance(value, int) and value >= 0 and value <= 100

    @staticmethod
    def validate_smoker(value):
        return value in ["yes", "no"]

    @staticmethod
    def validate_region(value):
        return value in ["southeast", "southwest", "northeast", "northwest"]

    @staticmethod
    def validate_charges(value):
        return isinstance(value, (int, float)) and value >= 0



    def initiate_datavalidation(self):
        try:
            df = self.load_data()


            ## column wise validation
            expected_columns = {"age", "sex", "bmi", "children", "smoker", "region", "charges"} 
            if expected_columns == set(df.columns):
                logging.info(f"we got validated columns records : {expected_columns}")

                # row wise validation 

                invalid_rows = df[
                    (~df["age"].apply(self.validate_age)) |
                    (~df["sex"].apply(self.validate_sex)) |
                    (~df["bmi"].apply(self.validate_bmi)) |
                    (~df["children"].apply(self.validate_children)) |
                    (~df["smoker"].apply(self.validate_smoker)) |
                    (~df["region"].apply(self.validate_region)) |
                    (~df["charges"].apply(self.validate_charges))
                ]

                # Display validation results
                if invalid_rows.empty:
                    logging.info("Successfuly validated all the rows")
                    print("✅ Data is valid!")

                    os.makedirs(self.DataValidationConfig_obj.datavalidation_dir,exist_ok=True)
                    df.to_csv(self.DataValidationConfig_obj.valid_data_file_path,index=False)

                    
                else:
                    logging.info(f"found invalid rows are : {invalid_rows.shape[0]}")
                    # save the invalid record

                    os.makedirs(self.DataValidationConfig_obj.datavalidation_dir,exist_ok=True)
                    invalid_rows.to_csv(self.DataValidationConfig_obj.invalid_data_file_path,index=False)


                    df.drop(invalid_rows.index,inplace=True)
                    df.to_csv(self.DataValidationConfig_obj.valid_data_file_path,index=False)

                    # save the invalid data

            else:
                # ## if anyone is not avaialble or extra or with renamed then
                logging.info(f"no got it expected columns, we recieved {set(df.columns)} and expecting {expected_columns}")
                df = df[list(expected_columns)]

                invalid_rows = df[
                    (~df["age"].apply(self.validate_age))
                    (~df["sex"].apply(self.validate_sex)) |
                    (~df["bmi"].apply(self.validate_bmi)) |
                    (~df["children"].apply(self.validate_children)) |
                    (~df["smoker"].apply(self.validate_smoker)) |
                    (~df["region"].apply(self.validate_region)) |
                    (~df["charges"].apply(self.validate_charges))]

                 # Display validation results
                if invalid_rows.empty:
                    logging.info("Successfuly validated all the rows")
                    print("✅ Data is valid!")

                    os.makedirs(self.DataValidationConfig_obj.datavalidation_dir,exist_ok=True)
                    df.to_csv(self.DataValidationConfig_obj.valid_data_file_path,index=False)

                    
                else:
                    logging.info(f"found invalid rows are : {invalid_rows.shape[0]}")
                    # save the invalid record

                    os.makedirs(self.DataValidationConfig_obj.datavalidation_dir,exist_ok=True)
                    invalid_rows.to_csv(self.DataValidationConfig_obj.invalid_data_file_path,index=False)


                    df.drop(invalid_rows.index,inplace=True)
                    df.to_csv(self.DataValidationConfig_obj.valid_data_file_path,index=False)
            

            ### returning data validatiaon artifact 
            data_validation_artifact = artifact_entity.DataValidationArtifact(
                valid_data_file_path=self.DataValidationConfig_obj.valid_data_file_path,
                invalid_data_file_path=self.DataValidationConfig_obj.invalid_data_file_path
            )
            logging.info(f"Successfully Done the data validation part, validation data available : {self.DataValidationConfig_obj.datavalidation_dir}")
            return data_validation_artifact

                
        except Exception as e:
            obj = InsuranceException(e,sys)
            logging.info(f"Data validation error : {obj.error_message}")
            raise InsuranceException(e,sys)

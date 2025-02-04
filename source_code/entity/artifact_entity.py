from dataclasses import dataclass 

@dataclass
class DataIngestArtifact:
    Dataset_file_path:str 
    Train_file_path:str 
    Test_file_path:str 

@dataclass
class DataValidationArtifact:
    valid_data_file_path:str 
    invalid_data_file_path:str 


@dataclass
class DataCleaningArtifact:
    clean_data_file_path:str 

@dataclass
class DataTransFormArtifact:
    onehot_data_file_path:str 
    onehot_encoder_model_path:str 

    scaler_data_file_path:str 
    scaler_model_path:str 




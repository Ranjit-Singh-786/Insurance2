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


from dataclasses import dataclass 

@dataclass
class DataIngestArtifact:
    Dataset_file_path:str 
    Train_file_path:str 
    Test_file_path:str 

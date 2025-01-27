from source_code.logger import logging 
from source_code.exception import InsuranceException 
 


def is_mongo_connected(mongo_connection):
    if mongo_connection is not None:
        return True
    else:
        return False

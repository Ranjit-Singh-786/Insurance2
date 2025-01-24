from source_code.logger import logging
from source_code.exception import InsuranceException 
import  sys 
# logging.info("hii this is ranjit")
# logging.debug("hii this is ranjit")
# logging.warning("hii this is ranjit")
# logging.error("hii this is ranjit")
# logging.critical("hii this is ranjit")


try:
    10/0
except Exception as e:
    obj =  InsuranceException(error_message=e,error_detail=sys)
    logging.info("hii this is ranjit")
    logging.warning(obj.error_message)
    print(obj.error_message)



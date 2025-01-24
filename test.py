import sys 
try:
    10/0 
except Exception as e:
    error_class,error_mesage,exc_tb = sys.exc_info()
    print(error_class)
    print(error_mesage)
    print(exc_tb)
    print()
    line_number = exc_tb.tb_frame.f_lineno  # to get line number 
    file_name = exc_tb.tb_frame.f_code.co_filename
    print(line_number)   
    print(file_name)
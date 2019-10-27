
import logging


def get_logger(name):
    log_format = '[%(levelname)s] %(asctime)s [%(name)s] %(message)s'

    formatter = logging.Formatter(fmt=log_format)               

    file_handler = logging.FileHandler('logs/migration.log', mode='a+')          
    file_handler.setLevel(logging.DEBUG)                                            
    file_handler.setFormatter(formatter)                                            
    
    logging.basicConfig(level=logging.INFO,format=log_format)
    logger = logging.getLogger(name)
    logger.addHandler(file_handler)
    return logger
    

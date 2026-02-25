import logging
import os

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

def get_logger(distributor):
    logger = logging.getLogger(distributor)
    if not logger.handlers:
        file_handler = logging.FileHandler(os.path.join(LOG_DIR, f"update_distributors_{distributor.lower()}.log"), encoding="utf-8")
        file_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
        logger.addHandler(file_handler)
        
        # Добавляем консольный обработчик только если он есть
        root_logger = logging.getLogger()
        if root_logger.handlers:
            logger.addHandler(root_logger.handlers[0])
        
        logger.setLevel(logging.INFO)
    return logger

def get_distributor_logger(distributor):
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f"update_distributors_{distributor.lower()}.log")
    logger = logging.getLogger(f"{distributor}_file")
    if not logger.handlers:
        handler = logging.FileHandler(log_file, encoding='utf-8')
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger 
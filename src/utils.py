import logging
import configparser
import os

def setup_logging():
    os.makedirs("logs", exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(module)s - %(message)s',
        handlers=[
            logging.FileHandler("logs/sigtap_rpa.log", mode='a', encoding='utf-8'),
            logging.StreamHandler()
        ]
    )

def get_config():
    config = configparser.ConfigParser()
    # Lê o arquivo de forma padrão, sem forçar encoding
    config.read('config.ini')
    return config
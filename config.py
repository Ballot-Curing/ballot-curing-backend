import os
import configparser

def load_config():
    config_file = os.path.join(os.environ['BALLOT_CURING_PATH'], 'config.ini')

    config = configparser.ConfigParser()

    if not config.read(config_file):
        raise Exception(f'config.ini not found in {config_file}.')

    return config

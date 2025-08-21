import os
import configparser

def _load_config(config_file=None, section1='DATABASE_PSQL', section2='LOCATION_DECRYPTION'):
    config = {}

    # Create a parser for the config file
    parser = configparser.ConfigParser()

    # Load the config file if provided
    if config_file:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        # Construct the full path to the config file
        config_file = os.path.join(current_dir, '..', '..', 'config', config_file)
        print(f"Loading config file: {config_file}")
        
        if os.path.exists(config_file):
            parser.read(config_file)
        else:
            raise FileNotFoundError(f"Config file not found: {config_file}")
        
        # Load DATABASE_PSQL section from config file or environment variables
        config['user'] = parser.get(section1, 'user', fallback=os.getenv('user'))
        config['password'] = parser.get(section1, 'password', fallback=os.getenv('password'))
        config['host'] = parser.get(section1, 'host', fallback=os.getenv('host'))
        config['port'] = parser.get(section1, 'port', fallback=os.getenv('port'))
        
        # Load LOCATION_DECRYPTION section from config file or environment variables
        config['key1'] = parser.get(section2, 'key1', fallback=os.getenv('key1'))
        config['key2'] = parser.get(section2, 'key2', fallback=os.getenv('key2'))
    
    else:
        # Load DATABASE_PSQL
        config['user'] = os.getenv('user')
        config['password'] = os.getenv('password')
        config['host'] = os.getenv('host')
        config['port'] = os.getenv('port')

        # Load LOCATION_DECRYPTION
        config['key1'] = os.getenv('key1')
        config['key2'] = os.getenv('key2')

    return config

def get_db_connection_params(config_file):
    config = _load_config(config_file)
    if not all(config.values()):
        raise ValueError("One or more database connection parameters are missing.")
    return {k: v for k, v in config.items() if k.startswith('db_')}
    
def get_decryption_key(config_file, key='key2'):
    config = _load_config(config_file)
    if key not in config:
        raise KeyError(f"Key not found in config file {config_file}: {key}")
    return config[key]

# Example usage
if __name__ == '__main__':
    config_file = 'config.ini'  # Adjust the path if needed
    print("Database config:", get_db_connection_params(config_file))
    print("Decryption key (key2):", get_decryption_key(config_file, 'key2'))
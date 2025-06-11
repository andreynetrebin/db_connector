import jaydebeapi
import threading
import json
import pandas as pd
from logger_config import get_logger

logger = get_logger('DatabaseConnector')

class SingletonMeta(type):
    """
    Потокобезопасный Singleton метакласс.
    """
    _instances = {}
    _lock = threading.Lock()

    def __call__(cls, *args, **kwargs):
        with cls._lock:
            if cls not in cls._instances:
                instance = super().__call__(*args, **kwargs)
                cls._instances[cls] = instance
        return cls._instances[cls]

class DatabaseConnector(metaclass=SingletonMeta):
    """
    Коннектор к СУБД через jaydebeapi с конфигурацией из JSON файла.
    """

    def __init__(self, config_path: str, db_name: str):
        self.load_config(config_path, db_name)
        self.connection = None
        self.connect()

    def load_config(self, config_path: str, db_name: str):
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            db_config = config['databases'][db_name]
        except KeyError:
            logger.error(f"Database config '{db_name}' not found in {config_path}")
            raise
        except Exception as e:
            logger.error(f"Error loading config file '{config_path}': {e}")
            raise

        self.db_type = db_config['db_type']
        self.driver_class = db_config['driver_class']
        driver_path = db_config['driver_path']
        self.driver_path = driver_path if isinstance(driver_path, list) else [driver_path]
        self.url = db_config['url']
        self.user = db_config['user']
        self.password = db_config['password']
        logger.info(f"Config loaded for database '{self.db_type}'")

    def connect(self):
        if self.connection:
            try:
                self.connection.jconn.getAutoCommit()
                logger.info(f"[{self.db_type}] Existing connection is active.")
                return
            except Exception:
                logger.warning(f"[{self.db_type}] Connection lost, reconnecting.")
                try:
                    self.connection.close()
                except Exception:
                    pass
                self.connection = None

        try:
            self.connection = jaydebeapi.connect(
                self.driver_class,
                self.url,
                [self.user, self.password],
                jars=self.driver_path
            )
            logger.info(f"[{self.db_type}] Connection established successfully.")
        except Exception as e:
            logger.error(f"[{self.db_type}] Failed to connect: {e}")
            raise

    def execute_query(self, query: str, params=None):
        cursor = self.connection.cursor()
        try:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            try:
                result = cursor.fetchall()
                logger.info(f"[{self.db_type}] Query executed successfully, fetched {len(result)} rows.")
            except jaydebeapi.DatabaseError:
                result = None
                logger.info(f"[{self.db_type}] Query executed successfully, no data to fetch.")
            cursor.close()
            return result
        except Exception as e:
            cursor.close()
            logger.error(f"[{self.db_type}] Query execution error: {e}")
            raise

    def execute_query_df(self, query: str, params=None):
        cursor = self.connection.cursor()
        try:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns=columns)
            cursor.close()
            logger.info(f"[{self.db_type}] Query executed successfully, DataFrame created with {len(df)} rows.")
            return df
        except Exception as e:
            cursor.close()
            logger.error(f"[{self.db_type}] Query execution error: {e}")
            raise

    def close_connection(self):
        if self.connection:
            try:
                self.connection.close()
                logger.info(f"[{self.db_type}] Connection closed.")
            except Exception as e:
                logger.error(f"[{self.db_type}] Error closing connection: {e}")
            finally:
                self.connection = None


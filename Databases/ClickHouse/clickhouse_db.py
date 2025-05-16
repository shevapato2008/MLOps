import pandas as pd
import threading
# from loguru import logger
import logging

from clickhouse_driver import Client as ClickHouseClient

import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ClickHouseDB:
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        """
        - Uses __new__ to ensure only one instance is created
        - Implements thread-safety with a lock
        - Separates instance creation from initialization
        """
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(ClickHouseDB, cls).__new__(cls)
                cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        self.class_name = self.__class__.__name__
        if self._initialized:
            return
            
        try:
            # Log connection attempt without sensitive details
            logger.info(
                f"[{self.class_name}] Connecting to ClickHouse at {config.CLICKHOUSE_HOST}:{config.CLICKHOUSE_PORT}")

            self.client = ClickHouseClient(
                host=config.CLICKHOUSE_HOST,
                port=config.CLICKHOUSE_PORT,
                user=config.CLICKHOUSE_USERNAME,
                password=config.CLICKHOUSE_PASSWORD,
                database='default',
            )
            logger.info(f"[{self.class_name}] Connection established successfully")
        except Exception as e:
            logger.error(f"[{self.class_name}] Failed to connect: {str(e)}")
            raise
            
        self._initialized = True
        
    @classmethod
    def get_instance(cls):
        """
        Make it explicit that it's a singleton
        """
        return cls()  # This calls __new__ which returns the singleton
    
    def fetch_data(self, query, columns=None):
        try:
            result_set = self.client.execute(query)
            return pd.DataFrame(result_set, columns=columns)
        except Exception as e:
            logger.error(f"[{self.class_name}] Query execution failed: {str(e)}")
            raise
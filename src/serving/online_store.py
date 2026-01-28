from abc import ABC, abstractmethod
import logging

logger = logging.getLogger(__name__)

class OnlineStore(ABC):
    @abstractmethod
    def batch_write(self, records: list):
        """
        Writes a batch of records (dicts) to the online store.
        """
        pass

class MockRedisStore(OnlineStore):
    """
    Simulates a Redis Key-Value Store.
    Key: user_id
    Value: serialized feature dict
    """
    def __init__(self):
        self.data = {}
        
    def batch_write(self, records: list):
        # In production: utilize pipeline() for Redis or bulk_put for HBase
        for record in records:
            # Assuming 'user_id' is the entity key
            key = record.get("user_id")
            if key:
                self.data[key] = record
                # logger.info(f"Redis SET {key} -> {record}")
        
        logger.info(f"Successfully wrote {len(records)} records to MockRedis.")

    def get(self, key):
        return self.data.get(key)

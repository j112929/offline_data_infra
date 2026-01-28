from abc import ABC, abstractmethod
from pyspark.sql import DataFrame

class FeatureGroup(ABC):
    """
    Abstract base class for a group of features.
    """
    
    @property
    @abstractmethod
    def name(self) -> str:
        pass
    
    @property
    @abstractmethod
    def description(self) -> str:
        pass
        
    @abstractmethod
    def compute(self, source_df: DataFrame) -> DataFrame:
        """
        Transforms raw source data into features.
        """
        pass

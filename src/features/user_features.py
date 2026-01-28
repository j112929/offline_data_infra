from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from src.features.base_feature import FeatureGroup

class UserFeatures(FeatureGroup):
    @property
    def name(self) -> str:
        return "user_features_daily"

    @property
    def description(self) -> str:
        return "Daily user activity aggregates"

    def compute(self, source_df: DataFrame) -> DataFrame:
        """
        Expects source_df to have columns: user_id, timestamp, event_type, value
        Returns: user_id, date, event_count, total_value
        """
        return source_df \
            .withColumn("date", F.to_date("timestamp")) \
            .groupBy("user_id", "date") \
            .agg(
                F.count("*").alias("event_count"),
                F.sum("value").alias("total_value")
            )

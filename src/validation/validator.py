from pyspark.sql import DataFrame
import logging

logger = logging.getLogger(__name__)

class Validator:
    @staticmethod
    def validate_quality(df: DataFrame, checks: dict) -> bool:
        """
        Run basic quality checks.
        checks: dict of {column: requirements}
        """
        for col, requirement in checks.items():
            if requirement == "not_null":
                null_count = df.filter(df[col].isNull()).count()
                if null_count > 0:
                    logger.error(f"Validation FAILED: Column {col} has {null_count} nulls.")
                    return False
        
        logger.info("Validation PASSED.")
        return True

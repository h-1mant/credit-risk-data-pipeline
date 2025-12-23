from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, when, lit, to_timestamp, round, concat
import pytest

@pytest.fixture(scope="module")
def spark():
    """Initialize a Spark Session for Testing"""
    spark = SparkSession \
        .builder \
        .appName("pyspark-unit-test") \
        .master("local[*]") \
        .getOrCreate()
    
    return spark


def test_transactions_processing(spark):
    """Test transaction processing logic"""

    # 1. Define Sample Transactions Data
    transactions_schema = StructType([
        StructField("transaction_id", StringType(), False),
        StructField("cardholder_id", StringType(), False),
        StructField("merchant_id", StringType(), False),
        StructField("merchant_name", StringType(), False),
        StructField("merchant_category", StringType(), False),
        StructField("transaction_amount", DoubleType(), False),
        StructField("transaction_currency", StringType(), False),
        StructField("transaction_timestamp", StringType(), False),
        StructField("transaction_status", StringType(), False),
        StructField("fraud_flag", BooleanType(), False),
        StructField("device_type", StringType(), False),
        StructField("merchant_location", StringType(), False)
    ])

    transactions_data = [
        ("T001", "CH001", "M001", "Walmart", "Groceries", 2705.02, "USD", "2025-02-04T10:00:00Z", "SUCCESS", False, "Mobile", "New York, USA"),
        ("T002", "CH002", "M002", "Expedia", "Travel", 9500.75, "USD", "2025-02-04T12:30:00Z", "PENDING", True, "Web", "Toronto, Canada"),
        ("T003", "CH003", "M003", "Amazon", "Shopping", 15402.81, "USD", "2025-02-04T15:45:00Z", "FAILED", True, "Web", "San Francisco, USA"),
    ]

    transactions_df = spark.createDataFrame(transactions_data, schema=transactions_schema)

    # 2. Define Sample Cardholders Data
    cardholders_schema = StructType([
        StructField("cardholder_id", StringType(), False),
        StructField("customer_name", StringType(), False),
        StructField("reward_points", IntegerType(), False),
        StructField("risk_score", DoubleType(), False),
    ])

    cardholders_data = [
        ("CH001", "John Doe", 450, 0.15),
        ("CH002", "Jane Smith", 100, 0.35),
        ("CH003", "Ali Khan", 80, 0.10),
    ]

    cardholders_df = spark.createDataFrame(cardholders_data, schema=cardholders_schema)

    # 3. Apply Data Transformations (Simulating Production Logic)
    transactions_df = (
    transactions_df 
        .withColumn("transaction_category", 
                    when(col("transaction_amount") <= 5000, lit("Low"))
                    .when( (col("transaction_amount") > 5000) & (col("transaction_amount") <= 10000), lit("Medium"))
                    .otherwise("High")
                )
        .withColumn("transaction_timestamp", 
                    to_timestamp(col("transaction_timestamp")))
        .withColumn("high_risk", 
                    (col("fraud_flag") == True) | (col("transaction_category") == "High")
                )
        .withColumn("merchant_info", 
                    concat(col("merchant_name"), lit("-"), col("merchant_location"))
                )
    )

    # 4. Perform Join (Simulating Enrichment with Cardholders Data)
    enriched_df = transactions_df.join(cardholders_df, on="cardholder_id", how="left")

    # 5. Update Reward Points (Earn 1 point per $10 spent)
    enriched_df = enriched_df.withColumn(
        "updated_reward_points", col("reward_points") + round(col("transaction_amount") / 10)
    )

    # 6. Calculate Fraud Risk Level
    enriched_df = (
    enriched_df.withColumn(
        "fraud_risk_level",
        when(col("high_risk") == True, lit("Review"))
        .when( (col("risk_score") > 0.4) | (col("fraud_flag") == True), lit("High"))
        .otherwise(lit("Low"))
        )
    )

    #7. Assertions: Verify Transformations
    result = enriched_df.select("transaction_id", "transaction_category", "high_risk", "fraud_risk_level", "updated_reward_points").collect()

    assert result[0]["transaction_category"] == "Low"     
    assert result[1]["transaction_category"] == "Medium"    
    assert result[2]["transaction_category"] == "High"     

    assert result[0]["high_risk"] == False
    assert result[1]["high_risk"] == True  
    assert result[2]["high_risk"] == True

    print("All Unit Tests Passed")








    

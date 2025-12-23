from pyspark.sql import SparkSession
import pyspark.sql.functions as F

#Initialize Spark Session
spark = SparkSession.builder.appName("credit-risk-processing-session").getOrCreate()

#Load Static Cardholders Data
BQ_PROJECT = "airflow-hrm293"
BQ_DATASET = "credit_card"
BQ_CARDHOLDERS_TABLE = f"{BQ_PROJECT}.{BQ_DATASET}.cardholders"

cardholders_df = spark.read.format("bigquery").option("table",BQ_CARDHOLDERS_TABLE).load()


#Load Transaction Data
json_file_path = "gs://credit-card-data-analysis-hrm2934/transactions/transactions_*.json"

transactions_df = spark.read.format("json").option("multiLine",True).json(json_file_path)


#Data Validation
transactions_df = transactions_df.filter(
    (F.col("transaction_id").isNotNull()) &
    (F.col("cardholder_id").isNotNull()) &
    (F.col("merchant_id").isNotNull())
    (F.col("transaction_amount") > 0) 
)   


#Data Transformations
transactions_df = (
  transactions_df 
    .withColumn("transaction_category", 
                F.when(F.col("transaction_amount") <= 5000, F.lit("Low"))
                .when( (F.col("transaction_amount") > 5000) & (F.col("transaction_amount") <= 10000), F.lit("Medium"))
                .otherwise("High")
              )
    .withColumn("transaction_timestamp", 
                F.to_timestamp(F.col("transaction_timestamp")))
    .withColumn("high_risk", 
                (F.col("fraud_flag") == True) | (F.col("transaction_category") == "High")
              )
    .withColumn("merchant_info", 
                F.concat(F.col("merchant_name"), F.lit("-"), F.col("merchant_location"))
              )
)

#Enrich Transactions with Cardholders Data 
enriched_df = transactions_df.join(cardholders_df,on="cardholder_id",how="left")

#Update Reward Points (Earn 1 point per $10 Spent)
enriched_df = enriched_df.withColumn(
  "updated_reward_points", F.col("reward_points") + F.round(F.col("transaction_amount") / 10)
)

#Calculate Fraud-Risk Level
enriched_df = (
  enriched_df.withColumn(
    "fraud_risk_level",
    F.when(F.col("high_risk") == True, F.lit("Review"))
    .when( (F.col("risk_score") > 0.4) | (F.col("fraud_flag") == True), F.lit("High"))
    .otherwise(F.lit("Low"))
  )
)

#Save as Table in BigQuery
BQ_TRANSACTIONS_TABLE = f"{BQ_PROJECT}.{BQ_DATASET}.transactions"

enriched_df.write \
    .format("bigquery") \
    .option("table",BQ_TRANSACTIONS_TABLE) \
    .option("writeMethod","direct") \
    .mode("append") \
    .save()

print(f"Successfully processed file: {json_file_path}")







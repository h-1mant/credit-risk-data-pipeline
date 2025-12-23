1. create GCS customers/cardholders.csv
2. create BQ dataset credit-analysis -> cardholders table (upload cardholders.csv into static table)
3. create airflow cluster -> attach to service account
4. CODE
	1. define airflow DAG 
	-> Task 1: GCSFileSensor for transactions
	-> Task 2: Run Batch Job on DataProcServerless for PySpark code
	-> Task 3: move transactions to archive folder
	
	2. define pyspark code
	-> perform validation on transactions data
	-> basic transformations, derive new columns, bucketing etc.
	-> save as bq table, append mode, writemethod (Direct)
	
5. CI-CD
	1. write unit tests under tests/test_transactions_processing.py
	2. write .github/workflows/ci-cd.yaml
		-> on push to dev 
			-> checkout code 
			-> setup python 
			-> run unit tests
		-> on push to main 
			-> checkout code 
			-> authenticate to GCP 
			-> setup Gcloud SDK (use secrets where needed)
			-> upload pyspark code to GCS
			-> upload airflow DAG to Cloud Composer

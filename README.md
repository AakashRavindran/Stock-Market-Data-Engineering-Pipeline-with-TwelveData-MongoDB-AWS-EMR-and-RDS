Stock Market Data Engineering Pipeline with Twelve Data, MongoDB, AWS EMR and RDS

Problem Statement:
Build a scalable and automated data pipeline that fetches stock price data every 5 minutes for companies like Apple, Google, Tesla, Microsoft, and Nvidia from TwelveData API. The pipeline should store raw data in MongoDB, transform it into Parquet using PySpark on AWS EMR, aggregate it using AWS Glue, and store final data in RDS for analytics.
Business Use Cases:
Real-time stock trend monitoring for financial analysts
Historical stock data archiving for research and reporting
Automated price alerts for trading bots
Scalable infrastructure for financial data aggregation

Tools Used:
Initial data Ingestion: TwelveData API, Python
Staging Layer: MongoDB Atlas
Data Processing: AWS EMR with PySpark, AWS Glue
Data Pipeline Orchestration: Apache Airflow
Monitoring: Apache Airflow, Slack Notifications
Security: AWS Secrets Manager, IAM Roles
Version Control: GitHub
Host & Run Airflow and initial Ingestion Code: AWS EC2 instance
Programming Languages Used: Python, SQL, Pyspark API

Data Set Info:
Source: TwelveData API
Format: JSON
Variables:
symbol: Stock symbol (e.g., AAPL, TSLA)
datetime: Timestamp
open, high, low, close: Stock prices
 volume: Volume of trade

Data Set Explanation:
•	Each API call gives OHLCV (Open, High, Low, Close, Volume) data at 5-minute
intervals.
•	The data must be validated (e.g., non-null prices, chronological order).
•	MongoDB will be the staging layer before processing.
•	PySpark will convert JSON → DataFrame → Parquet (with schema).
•	S3 stores Parquet files partitioned by Company.
•	Glue jobs summarize the data.
•	RDS holds the cleaned, aggregated output.

Furthes steps are mentioned in the project_desc word document

# Stock Market Data Engineering Pipeline with Twelve Data, MongoDB, AWS EMR, and RDS

![image](https://github.com/user-attachments/assets/3b3d9cfb-0cb5-46c1-bfed-e6c1725698f0)


## Problem Statement:
Build a scalable and automated data pipeline that fetches past 20 days stock price data for every 5 minutes for companies like Apple, Google, Tesla, Microsoft, and Nvidia from the TwelveData API. The pipeline should store raw data in MongoDB, transform it into Parquet using PySpark on AWS EMR, aggregate it using AWS Glue, and store final data in RDS for analytics.

## Business Use Cases:
- **Real-time stock trend monitoring** for financial analysts
- **Historical stock data archiving** for research and reporting
- **Automated price alerts** for trading bots
- **Scalable infrastructure** for financial data aggregation

## Tools Used:
- **Initial data Ingestion**: TwelveData API, Python
- **Staging Layer**: MongoDB Atlas
- **Data Processing**: AWS EMR with PySpark, AWS Glue
- **Data Pipeline Orchestration**: Apache Airflow
- **Monitoring**: Apache Airflow, Slack Notifications
- **Security**: AWS Secrets Manager, IAM Roles
- **Version Control**: GitHub
- **Host & Run Airflow and Initial Ingestion Code**: AWS EC2 instance
- **Programming Languages Used**: Python, SQL, PySpark API

## Data Set Info:
- **Source**: TwelveData API
- **Format**: JSON
- **Variables**:
  - `symbol`: Stock symbol (e.g., AAPL, TSLA)
  - `datetime`: Timestamp
  - `open`, `high`, `low`, `close`: Stock prices
  - `volume`: Volume of trade

## Data Set Explanation:
- Each API call returns OHLCV (Open, High, Low, Close, Volume) data at 5-minute intervals.
- The data must be validated (e.g., non-null prices, chronological order).
- MongoDB will be the staging layer before processing.
- PySpark will convert JSON → DataFrame → Parquet (with schema).
- S3 stores Parquet files partitioned by Company.
- AWS Glue jobs summarize the data.
- RDS holds the cleaned, aggregated output.


## Project Flow:
1. **Data Ingestion**:
   - Use the TwelveData API to fetch real-time stock data every 5 minutes for selected stock symbols (Apple, Google, Tesla, Microsoft, Nvidia).
   - Python scripts handle the API calls, data validation, and store raw JSON data in MongoDB.

2. **Data Processing**:
   - PySpark on AWS EMR transforms the raw data from JSON format to Parquet.
   - The Parquet files are stored in S3, partitioned by company symbol for scalability.

3. **Data Aggregation**:
   - AWS Glue jobs perform the necessary transformations and aggregations on the Parquet data.
   - Cleaned and summarized data is loaded into Amazon RDS for analytics.

4. **Orchestration & Automation**:
   - Apache Airflow orchestrates the data pipeline, scheduling tasks like data fetching, transformation, and loading.
   - Slack notifications are used to alert the team in case of failures or critical events.

## Security:
- **AWS Secrets Manager** is used to securely manage API keys and other sensitive information.
- **IAM Roles** ensure proper permissions are granted to access the AWS services in a secure manner.


## Project Flow:

1. **EC2 Instance Setup for Apache Airflow**:
   - Use an **AWS EC2 instance** to host Apache Airflow and the Python code responsible for initial data ingestion from the **TwelveData API**.
   - Optionally, **AWS Lambda** can be used for executing Python code for ingestion.
   - Ensure the EC2 instance has the required **IAM roles** to interact with AWS services like **EMR**, **S3**, **Glue**, **RDS**, and **Secrets Manager**.

2. **VPC and Subnet Configuration**:
   - Deploy the EC2 instance in the correct **VPC** and **Subnet**.
   - Ensure that the **Security Group** allows inbound traffic on port 8080 for accessing the Apache Airflow Web UI.

3. **MongoDB Atlas Setup**:
   - Set up a **MongoDB Atlas** database and create the required collections as part of the Python code execution (this is done automatically, rather than manually).
   
4. **S3 Bucket for Code and Bootstrapping**:
   - Store the **PySpark code** for data processing in AWS **EMR** and the **requirements.sh** file (for EMR bootstrapping) in an **S3 bucket**.

5. **AWS Glue Catalog and Crawler**:
   - Update the **AWS Glue Catalog** with the location of the EMR output and **RDS** database tables.
   - Use an **AWS Glue Crawler** to automate metadata discovery for the Parquet files in S3 and the RDS tables.

6. **JDBC Connector Setup**:
   - Create a **JDBC Connector** in AWS Glue to connect to the **AWS RDS database** for data loading and aggregation.

7. **VPC Configuration for Glue**:
   - Ensure that **AWS Glue** and the JDBC connector are hosted within the same **VPC** as **AWS RDS** for secure communication.

8. **AWS Glue ETL Job Setup**:
   - Set up the **AWS Glue ETL job** using the Glue Visual ETL interface to perform data aggregation and transformation.
  
     ![image](https://github.com/user-attachments/assets/20d8180f-3639-436d-98cb-5c2e7a1a9873)


9. **AWS RDS Database Setup**:
   - Create the required **AWS RDS** database tables to store the final results of the ETL process.

10. **AWS EMR Cluster Configuration**:
    - The **AWS EMR** cluster will be automatically created and terminated as part of the Airflow **DAG**.
    - Ensure that **EMR** has the proper permissions to fetch data from **MongoDB Atlas** for processing.

11. **Apache Airflow Installation and Setup**:
    - Install **Apache Airflow** on the EC2 instance.
    - Set up the Airflow **webserver** and **scheduler** to manage the Directed Acyclic Graphs (**DAGs**) for the data pipeline.

12. **Airflow Connection Configurations**:
    - Set up an **SSH connection** in Airflow to your EC2 instance for accessing MongoDB.
    - Set up an **AWS connection** in Airflow to interact with **EMR**, **RDS**, and **Glue**.
    - Set up a **Slack connection** in Airflow to send notifications about task statuses.

13. **Define Airflow Tasks**:
    - Define all the tasks in the provided **DAG** file based on the data pipeline process (data ingestion, processing, aggregation, and storage).
    - Ensure proper configuration of **task dependencies** to control execution order.

14. **Schedule the DAG**:
    - Schedule the **DAG** to run at the desired interval (e.g., every 5 minutes for data ingestion), or trigger it manually depending on your use case.

15. **Additional Resources**:
    - For more detailed information on specific configurations and setup steps, refer to the **requirements.txt** or related documentation provided in each subdirectory of the project.





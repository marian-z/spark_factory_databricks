# Apple Customer Purchase Analysis Project

This project leverages Databricks Community Edition and Apache Spark to demonstrate a modular ETL pipeline architecture utilizing the Factory Design Pattern. The goal is to process and analyze data related to Apple customers, products, and transactions, identifying specific customer purchasing patterns. This pipeline is structured to allow flexibility in reading from various data formats and loading data to multiple storage options.

# Project Structure

- apple_analysis.ipynb: The main ETL pipeline orchestrator, calling on specific transformations and loading workflows.
- reader_factory.ipynb: Contains a reader factory to support reading data from multiple formats (e.g., CSV, Parquet, Delta).
- extractor.ipynb: Implements logic to use the reader factory to extract data from a CSV storage source.
- transform.ipynb: Defines transformations for:
  - Use Case 1: Identifying customers who purchased AirPods immediately after buying an iPhone.
  - Use Case 2: Identifying customers whose purchasing history consists only of iPhones and AirPods.
- loader_factory.ipynb: Supports multiple data loading methods:
  - Simple storage in DBFS.
  - Partitioned storage in DBFS.
  - Storage in a Delta table.
- loader.ipynb: Uses the loader factory to store transformed data in specified formats.
  
# Project Workflow

1. Data Extraction:
  - Data is read using extractor.ipynb with flexible data source handling, facilitated by the reader_factory.ipynb.
2. Transformation:
  - AirpodsAfterIphoneTransformer: Filters for customers who bought AirPods immediately after an iPhone purchase.
  - OnlyAirpodsAndIphoneTransformer: Identifies customers with a purchasing history of only iPhones and AirPods.
3. Data Loading:
  - Processed data is stored in DBFS or Delta format as specified, using loader_factory.ipynb and loader.ipynb.

# Usage

Clone the repository and import it into Databricks.
Upload your data files into DBFS as per the reader configuration.
Open and run apple_analysis.ipynb, which calls each component notebook to execute the ETL pipeline end-to-end.

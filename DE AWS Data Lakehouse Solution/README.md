# AWS Data Lakehouse Solution

This repository contains the code and resources for an AWS Data Lakehouse solution, developed as part of Udacity's Data Engineering with AWS Nanodegree program. The project demonstrates how to build a data lakehouse using AWS services, with the goal of curating data for machine learning model training.

## Project Overview

The STEDI Step Trainer is a device designed to help users practice balance exercises. It collects sensor data to train a machine learning model to detect steps, and this project aims to:

- Extract data from the STEDI device sensors and mobile app.
- Curate the data into a data lakehouse on AWS.
- Enable data scientists to use this curated data for training machine learning models.

### Data Sources

- **Customer Data**: Information from the STEDI website and fulfillment system, including customer consent details for research data usage.
- **Step Trainer Data**: Motion sensor readings from the Step Trainer.
- **Accelerometer Data**: Movement data captured by the mobile app.

The data is stored in an S3 bucket, with fields such as `serialNumber`, `distanceFromObject`, `timeStamp`, and more.

## Repository Contents

The repository contains the following files:

- SQL scripts:
  - `accelerometer_landing.sql`
  - `customer_landing.sql`
  - `step_trainer_landing.sql`
- Python scripts:
  - `accelerometer_landing_to_trusted.py`
  - `customer_landing_to_trusted.py`
  - `step_trainer_trusted.py`
  - `customer_trusted_to_curated.py`
  - `machine_learning_curated.py`
- `.png` screenshots of tables in AWS Athena.

## Architecture

The project follows a three-zone architecture for managing the data:

1. **Landing Zone**: Raw data is ingested into this zone without transformation. 
2. **Trusted Zone**: Data is cleaned, validated, and filtered to include only trusted records, such as customers who consented to share their data for research.
3. **Curated Zone**: Data is further processed, combined, and prepared for specific use cases, such as machine learning.

The process uses AWS services such as:
- **S3**: For data storage.
- **AWS Glue**: For running ETL jobs.
- **Athena**: For querying data.

## Project Workflow

1. **Run SQL Scripts**: Create landing tables in Athena for the customer, accelerometer, and step trainer data.
2. **Run Python Scripts**: Move data between zones (landing, trusted, curated) using AWS Glue.
    - `customer_landing_to_trusted.py`: Transfers customer data from landing to trusted.
    - `accelerometer_landing_to_trusted.py`: Transfers accelerometer data to trusted.
    - `customer_trusted_to_curated.py`: Filters customer data and moves it to curated.
    - `step_trainer_trusted.py`: Transfers step trainer data to trusted.
    - `machine_learning_curated.py`: Combines curated data to create a dataset for machine learning.
  
## How to Run

### Prerequisites

Ensure you have:
- AWS S3 bucket to store the data in landing, trusted, and curated zones.
- IAM permissions for S3, Glue, and Athena.
- A database created in Glue for storing tables.

### Steps

1. **Create Landing Zones**:
   - Run the SQL scripts in Athena to create `customer_landing`, `accelerometer_landing`, and `step_trainer_landing` tables.
   
2. **Create Trusted and Curated Zones**:
   - Use the provided Python scripts in AWS Glue to process and move data to the respective zones:
     - `Customer_Landing_to_Trusted.py`
     - `Accelerometer_Landing_to_Trusted.py`
     - `Customer_Trusted_to_Curated.py`
     - `Step_Trainer_Landing_to_Curated.py`
     - `Machine_Learning_Curated.py`

## Technical Discussion

The three-zone architecture ensures:
- **Landing Zone**: Raw data is collected and remains unprocessed.
- **Trusted Zone**: Data is cleaned and validated to ensure only trusted information is used.
- **Curated Zone**: Data is transformed to meet the needs of specific use cases, such as training machine learning models.

This structured approach improves data quality and provides a single source of truth, making the data ready for analysis.

## Business Discussion

This solution allows STEDI to:
- Scale data storage efficiently using Amazon S3.
- Prepare high-quality data for analytics and machine learning using AWS Glue.
- Provide data scientists with reliable, structured data for building machine learning models.
  
The data lakehouse architecture supports STEDI’s goal of making data-driven decisions, enhancing product offerings, and delivering a superior customer experience.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.


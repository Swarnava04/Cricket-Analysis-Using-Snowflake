**Cricket Analysis Project**

📖 Overview

This project focuses on building a data pipeline and analytics architecture for cricket match data using Snowflake.
The pipeline consists of multiple layers, starting from raw JSON data ingestion to clean, transformed datasets, and finally generating insightful dashboards.

📂 Repository Contents

1️⃣ json_files
This folder contains raw JSON files, which serve as the source data for the raw layer in Snowflake.

2️⃣ sql_files
This folder includes SQL scripts for implementing the following layers in the data pipeline:

Raw Layer: Loading JSON data into staging tables.

Clean Layer: Cleaning and standardizing the data.

Consumption Layer: Finalizing data for analytics and dashboards.

3️⃣ images

Visual representations of the project include:
<img width="800" alt="particular_match_analysis" src="https://github.com/user-attachments/assets/8d56ff13-0b9a-48f4-81d3-76e8a1736987">

![task_flow_diagram](https://github.com/user-attachments/assets/0cc2bea1-8eea-4ffc-9d56-49b60b34639d)

![er_diagram_2](https://github.com/user-attachments/assets/5e14260c-08d7-4fe5-a09f-6829ee92b4a9)

![player_clean_tbl](https://github.com/user-attachments/assets/03c50096-8ad5-46fa-86ca-dd93592ded02)


🛠️ Tools

Snowflake: Used for building and managing the data pipeline, including tasks and transformations.

DBeaver: Utilized for creating and visualizing ER diagrams.

🏗️ Architecture

Data Pipeline Flow
Internal/External Stage → Landing Layer → Raw Layer → Clean Layer → Consumption Layer → Visualization

Database Schema
Visualize the schema using the ER diagram provided in the images folder.

🚀 Key Features

Recursive CTEs: Handle missing date ranges for date_dim.

Dynamic Tasks: Automate data ingestion and updates using Snowflake tasks.

Comprehensive Insights: Analyze cricket match details, scores, and outcomes with structured dashboards.

🛠️ How to Use

Prerequisites

Set up a Snowflake warehouse and configure appropriate roles and privileges.

Clone the repository and upload JSON files into Snowflake using the provided SQL scripts.

Steps

Use sql_files to create tables and load data for each pipeline layer.

Visualize insights by integrating the consumption layer tables with your BI tool of choice.

📝 Notes

1. Recursive logic was implemented to populate the date_dim table dynamically for missing dates.
2. Snowflake tasks ensure the automation of data ingestion and processing.
3. The repository showcases a complete implementation of a cricket analysis pipeline.

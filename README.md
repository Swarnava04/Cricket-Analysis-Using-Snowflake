<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Cricket Analysis Project</title>
</head>
<body>
    <h1>Cricket Analysis Project</h1>

    <h2>📖 Overview</h2>
    <p>
        This project focuses on building a <b>data pipeline and analytics architecture</b> for cricket match data using <b>Snowflake</b>. 
        The pipeline consists of multiple layers, starting from raw JSON data ingestion to clean, transformed datasets, 
        and finally generating insightful dashboards.
    </p>

    <h2>🔧 Tools Used</h2>
    <ul>
        <li><b>Snowflake:</b> For data ingestion, processing, and analysis.</li>
        <li><b>DBeaver:</b> For creating and visualizing ER diagrams.</li>
    </ul>

    <h2>📂 Repository Contents</h2>
    <ul>
        <li>
            <b>json_files:</b> Contains raw JSON files, which serve as the source data for the <b>raw layer</b> in Snowflake.
        </li>
        <li>
            <b>sql_files:</b> Includes SQL scripts for implementing the following layers:
            <ul>
                <li><b>Raw Layer:</b> Loading JSON data into staging tables.</li>
                <li><b>Clean Layer:</b> Cleaning and standardizing the data.</li>
                <li><b>Consumption Layer:</b> Finalizing data for analytics and dashboards.</li>
            </ul>
        </li>
        <li>
            <b>images:</b> Visual representations of the project, including:
            <ul>
                <li><b>ER diagrams</b> for database schema.</li>
                <li><b>Table structures</b> for each layer.</li>
                <li><b>Dashboards</b> showcasing the final analytical insights.</li>
            </ul>
        </li>
    </ul>

    <h2>🏗️ Architecture</h2>
    <h3>Data Pipeline Flow</h3>
    <ol>
        <li><b>Raw Layer:</b> Ingest raw JSON files.</li>
        <li><b>Clean Layer:</b> Clean and transform data using recursive CTEs and joins.</li>
        <li><b>Consumption Layer:</b> Add necessary aggregations and relationships.</li>
        <li><b>Dashboarding:</b> Generate insights with tools like Power BI/Tableau (or equivalent).</li>
    </ol>
    <p>
        <img src="images/data_pipeline_architecture.png" alt="Data Pipeline Architecture" width="600">
    </p>

    <h3>Database Schema</h3>
    <p>
        <img src="images/er_diagram.png" alt="ER Diagram" width="600">
    </p>

    <h2>🚀 Key Features</h2>
    <ul>
        <li><b>Recursive CTEs:</b> Handle missing date ranges for <code>date_dim</code>.</li>
        <li><b>Dynamic Tasks:</b> Automate data ingestion and updates using Snowflake tasks.</li>
        <li><b>Comprehensive Insights:</b> Analyze cricket match details, scores, and outcomes with structured dashboards.</li>
    </ul>

    <h2>🛠️ How to Use</h2>
    <h3>Prerequisites</h3>
    <ul>
        <li>Set up a <b>Snowflake warehouse</b> and configure appropriate roles and privileges.</li>
        <li>Clone the repository and upload JSON files into Snowflake using the provided SQL scripts.</li>
    </ul>

    <h3>Steps</h3>
    <ol>
        <li>Use <code>sql_files</code> to create tables and load data for each pipeline layer.</li>
        <li>Visualize insights by integrating the <b>consumption layer</b> tables with your BI tool of choice.</li>
    </ol>

    <h2>📊 Sample Dashboard</h2>
    <p>
        <img src="images/dashboard_sample.png" alt="Dashboard Sample" width="600">
    </p>

    <h2>📝 Notes</h2>
    <ul>
        <li>Recursive logic was implemented to populate the <code>date_dim</code> table dynamically for missing dates.</li>
        <li>Snowflake tasks ensure the automation of data ingestion and processing.</li>
        <li>The repository showcases a complete implementation of a cricket analysis pipeline.</li>
    </ul>
</body>
</html>

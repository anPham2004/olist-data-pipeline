# Olist E-commerce Data Pipeline and Dashboard

![Python](https://img.shields.io/badge/Python-3.10-blue)
![Docker](https://img.shields.io/badge/Docker-Enabled-2496ED)
![ClickHouse](https://img.shields.io/badge/Warehouse-ClickHouse-FFCC00)
![Streamlit](https://img.shields.io/badge/Frontend-Streamlit-FF4B4B)

## Overview

This project is an End-to-End Data Engineering solution designed to analyze the **Olist Brazilian E-commerce Public Dataset**.

The goal was to build a scalable Data Warehouse using **ClickHouse** to handle large datasets efficiently and provide actionable insights via an interactive **Streamlit** dashboard. The system follows the **Star Schema** modeling to optimize analytical queries (OLAP).

## Architecture

The pipeline follows the Extract-Transform-Load (ETL) pattern, fully containerized with Docker:

![The overall architecture of the ETL data pipeline.](images/pipeline_architecture.png)

* **Key Components:**
    * **Source:** Olist Dataset (9 CSV files) containing Customers, Orders, Products, Sellers, etc.
    * **ETL Processor:** Python scripts (Pandas) to clean data, handle types and logic (e.g., merging payments/sales).
    * **Data Warehouse:** ClickHouse for high-speed aggregation.
    * **Data Modeling:** Star Schema with 4 Dimensions and 2 Fact tables.
    * **Visualization:** Interactive Dashboard built with Streamlit and Plotly.

## Data Model (Star Schema)

The Data Warehouse is designed based on **Star Schema** principles to optimize analytical performance:

![The overall star schema of the data warehouse.](images/star_schema.png)

* **Fact Tables:**
    * `fact_sales`: Contains transactional data (revenue, freight, dates, foreign keys).
    * `fact_payments`: Detailed payment records (installments, payment types).
* **Dimension Tables:**
    * `dim_customer`: Customer demographics and geolocation (State/City).
    * `dim_product`: Product attributes and categories.
    * `dim_seller`: Seller profiles.
    * `dim_date`: Time dimension for temporal analysis (Year, Month, Quarter, Weekend flags).

## Project Structure

```bash
olist_etl_pipeline/
├── dashboard/               # Frontend Application
│   └── app.py               # Streamlit Dashboard Source Code
├── data/                    # Data Storage (Volume Mapped)
│   └── raw/                 # Raw CSV files (Git ignored)
├── pipeline/                # ETL Logic (Backend)
│   ├── Dockerfile           # Environment definition
│   ├── requirements.txt     # Python Dependencies
│   └── transform_*.py       # Scripts for Dims and Facts tables
├── docker-compose.yml       # Orchestration
├── .env.example             # Environment Variables Template
└── README.md                # Project Documentation
```

## How To Run

* **Prerequisites:**
    * Docker and Docker Compose installed
    * Git installed

* **Step 1: Clone the repository**
```bash
git clone [https://github.com/anPham2004/olist-data-pipeline.git](https://github.com/anPham2004/olist-data-pipeline.git)
cd olist-data-pipeline
```

* **Step 2: Download data** <br />
Download the dataset from [Kaggle Olist Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) and place the `.csv` files into the `data/raw/` folder.

* **Step 3: Setup environment** <br />
Create a `.env` file from the example:
```bash
cp .env.example .env
```

* **Step 4: Start the pipeline** <br />
Run the following command, the system will automatically: <br />
    * 1. Wait for the database to initialize <br />
    * 2. Create the `olist_dw` database <br />
    * 3. Run the full ETL pipeline <br />
    * 4. Launch the dashboard
```bash
docker-compose up --build
```

* **Step 5: Access dashboard** <br />
Once you see "Streamlit" logs in the terminal, go to: http://localhost:8501

## Dashboard Demo

* **1. Sales Performance & KPIs**

* **2. Geographic & Payment Analysis**

## Author

* **Pham Thai An**
    * **LinkedIn:**
    * **GitHub:** 
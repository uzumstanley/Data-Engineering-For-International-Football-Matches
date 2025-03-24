# International Football Matches Data Engineering Project

## Overview
This project focuses on processing and analyzing international football matches data using big data technologies. The dataset includes historical match results, shootout details, and goal scorers from 1872 to 2024.

This project was implemented in two different ways:

1. **Using Jupyter Notebook**: The implementation is available in the `notebooks/` folder.
2. **Using Apache Airflow**: The workflow is defined in the `scripts/` and `dags/` folders.

> 📝 **Note:** _This project was executed twice—once without Airflow using Jupyter Notebook in the `notebooks/` folder, and once with Airflow in the `scripts/` and `dags/` folders._  

---


## Technologies Used
- **Apache Spark** (Local Mode) - for distributed data processing
- **Hadoop & HDFS** - for storage and data management
- **Jupyter Notebook** - for interactive data exploration and processing
- **Google Cloud Storage (GCS)** - for cloud-based storage
- **BigQuery** - for querying large datasets efficiently
- **Looker** - for data visualization and reporting
- **Apache Airflow** - for workflow automation and orchestration
## Dataset [International Football Results (1872-2017) - Kaggle](https://www.kaggle.com/datasets/martj42/international-football-results-from-1872-to-2017/data)

The dataset contains three main CSV files:

### 1. `results.csv`
This file contains historical international football match results, including:
- `date`: Match date
- `home_team`: Name of the home team
- `away_team`: Name of the away team
- `home_score`: Home team score
- `away_score`: Away team score
- `tournament`: Tournament name
- `city`: City where the match was played
- `country`: Country where the match was played
- `neutral`: Whether the match was played at a neutral venue

### 2. `shootouts.csv`
This file includes penalty shootout details, with columns:
- `date`: Match date
- `home_team`: Home team name
- `away_team`: Away team name
- `winner`: Winner of the shootout
- `first_shooter`: Team that shot first in the penalty shootout

### 3. `goalscorers.csv`
This file contains details of goals scored in international matches, including:
- `date`: Match date
- `home_team`: Home team name
- `away_team`: Away team name
- `team`: Team that scored the goal
- `scorer`: Player who scored the goal
- `minute`: Minute in which the goal was scored
- `own_goal`: Whether it was an own goal
- `penalty`: Whether the goal was a penalty

## Project Goals
1. **Data Cleaning & Transformation**: Load, clean, and transform the data into a structured format.
2. **Schema Design**: Design a star schema with fact and dimension tables.
3. **ETL Pipelines**: Build pipelines to process, store, and query the data efficiently.
4. **Big Data Processing**: Utilize Spark and Hadoop for scalable processing.
5. **Cloud Integration**: Store and query data using GCS and BigQuery.
6. **Data Visualization**: Create interactive dashboards using Looker.
7. **Workflow Automation**: Orchestrate ETL processes with Apache Airflow.







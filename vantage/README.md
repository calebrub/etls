# ETL Pipeline for Report API Data
This project is an ETL (Extract, Transform, Load) pipeline that:

- Fetches report data from an API for multiple customers
- Transforms the JSON into .dat files
- Loads the .dat files into a PostgreSQL database using SQL script

`pip install -r requirements.txt`

# CREATE POSTGRES |DB
- psql -U postgres -d reveloop -f psql-create.sql

Run ETL 
- $ python reportAPI.py

- .\venv\Scripts\activate 


from sqlalchemy import create_engine
import os
import pandas as pd

# Define database connection parameters
db_user = os.environ.get('DB_USER')
db_password = os.environ.get('DB_PASSWORD')
db_host = os.environ.get('DB_HOST')
db_port = os.environ.get('DB_PORT')
db_name = os.environ.get('DB_NAME')

# Path to your CSV file
csv_file_path = 'data/Warehouse_and_Retail_Sales.csv'

# Target table name
table_name = 'warehouse_and_retail_sales'

# Read the CSV file into a DataFrame
df = pd.read_csv(csv_file_path)
df.columns = df.columns.str.replace(' ', '_')
df.columns = df.columns.str.strip()
df.columns = df.columns.str.lower()
df['created_at'] = pd.Timestamp.now()
df.rename(columns={'year': 'calendar_year', 'month': 'calendar_month'}, inplace=True)

# Create a connection string
connection_string = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
engine = create_engine(connection_string)

# Load DataFrame into PostgreSQL (replace table if exists)
df.to_sql(table_name, engine, if_exists='replace', index=False)

print(f"✅ Loaded {len(df)} rows into '{table_name}'")
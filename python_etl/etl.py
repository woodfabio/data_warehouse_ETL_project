import os
import pandas as pd
import numpy as np
from pymongo import MongoClient
from sqlalchemy import create_engine
import psycopg2
from time import sleep
import json

# Give a time to the other containers to build
sleep(10)

# ==================================================================================================
# Databases configuration:

# MongoDB configuration
mongo_host = "mongodb"
mongo_port = 27017
mongo_db_name = "ecommerce"
mongo_collection_name = "order_reviews"
mongo_username = "user2"
mongo_password = "secret2"

# PostgreSQL configuration
postgres_host = "postgres" 
postgres_port = 5432
postgres_db_name = "db"
postgres_user = "user"
postgres_password = "secret"

# ==================================================================================================
# Read CSV files:

try:
    # Load data from CSV files
    csv_files = [
        "olist_customers_dataset.csv",
        "olist_order_items_dataset.csv",
        "olist_order_payments_dataset.csv",
        "olist_orders_dataset.csv",
        "olist_products_dataset.csv"
    ]

    input_dir = "/app/input"

    print("-"*20)
    print("input_dir:")
    print(input_dir)

    # Store DataFrames for CSV files
    dfs = {}
    for file in csv_files:
        # Construct the absolute file path for the CSV file
        csv_file_path = os.path.join(input_dir, file)
        print("-"*20)
        print("csv file path:")
        print(csv_file_path)
        df_name = file.replace("_dataset.csv", "")
        df_name = df_name.replace("olist_", "")
        dfs[df_name] = pd.read_csv(f"{csv_file_path}")

    # Add primary key column to "payments" table:
    npayments = dfs["order_payments"].shape[0]
    dfs["order_payments"].insert(0, "payment_id", range(1, (npayments+1)))    
    
    # Crete engine to load data into PostgreSQL
    engine = create_engine(
        f"postgresql://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_db_name}"
    )

    # Iterate over the DataFrames and load them into PostgreSQL
    for table_name, df in dfs.items():
        df.to_sql(table_name, engine, if_exists="replace", index=False)
    

    print("-"*20)
    print("CSV files operation successful.")

except Exception as e:

    print("-"*20)
    print("Exception at CSV files operation:")
    print(f"Error: {e}")

finally:

    if engine:
        engine.dispose()
        
    print("-"*20)
    print("CSV files operation try-except terminated.")
    print("="*20)

# ==================================================================================================
# Read MongoDB file "init.js":

try:

    # Construct MongoDB connection string
    mongo_uri = f"mongodb://{mongo_username}:{mongo_password}@{mongo_host}:{mongo_port}/{mongo_db_name}?authSource=admin"

    # Connect to MongoDB with the connection string
    mongo_client = MongoClient(mongo_uri)
    mongo_db = mongo_client[mongo_db_name]
    mongo_collection = mongo_db[mongo_collection_name]

    # Load data from MongoDB
    mongo_data = list(mongo_collection.find())

    # Convert MongoDB data to pandas DataFrame
    reviews_dataset = pd.DataFrame(mongo_data)

    # Modify the DataFrame to convert ObjectId to string
    reviews_dataset['_id'] = reviews_dataset['_id'].astype(str)

    # Create engine to load data into PostgreSQL
    engine = create_engine(
        f"postgresql://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_db_name}"
    )

    # Load the table "reviews_dataset" into PostgreSQL
    reviews_dataset.to_sql("reviews", engine, if_exists="replace", index=False)

    print("Content of reviews_dataset:")
    print(reviews_dataset.head())  # Print the first few rows


    print("-"*20)
    print("MongoDB data extraction a upload successful.")

except Exception as e:

    print("-"*20)
    print("Exception at mongoDB operation:")
    print(f"Error: {e}")

finally:

    if mongo_client:
        mongo_client.close()

    if engine:
        engine.dispose()        

    print("-"*20)
    print("MongoDB operation try-except terminated.")
    print("="*20)


# ==================================================================================================
# PostgreSQL connection and tables upload :

try:

    # Establish a connection to PostgreSQL
    conn = psycopg2.connect(
        host=postgres_host,
        port=postgres_port,
        database=postgres_db_name,
        user=postgres_user,
        password=postgres_password
    )

    cur = conn.cursor()
    
    # Create table "customers":
    create_table_sql = """
    CREATE TABLE sales (
        order_id text,
        customer_id text,
        payment_id text,
        review_id text
    );
        
    INSERT INTO sales (order_id, customer_id, payment_id, review_id)
        SELECT o.order_id, o.customer_id, op.payment_id, r.review_id
        FROM orders AS o
        LEFT JOIN order_payments AS op ON o.order_id = op.order_id
        LEFT JOIN reviews AS r ON o.order_id = r.order_id;
     """
    
    # Execute the DDL command to create the table    
    cur.execute(create_table_sql)

    # Commit the transaction
    conn.commit()    

    print("-"*20)
    print("PostgreSQL upload successful.")

except psycopg2.Error as e:

    print("-"*20)
    print("Exception at PostgreSQL operation:")
    print(f"Error: {e}")

finally:

    if conn:
        # Close the connection
        conn.close()
    
    if cur:
        # close the cursor:        
        cur.close()

    print("-"*20)
    print("PostgreSQL files upload try-except terminated.")
    print("="*20)
        


print("-"*20)
print("ETL process completed.")
print("="*20)

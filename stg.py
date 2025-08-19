from sqlalchemy import create_engine
from sqlalchemy.engine import URL
import pandas as pd
import pyodbc
import os
import time


database = os.getenv('SQLServerSTGDB')
user = os.getenv('SQLServerUser')
password = os.getenv('SQLServerPassword')
print(user)

# Connection details
conn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=DESKTOP-NA5KNSI;'
    f'DATABASE={database};'
    f'UID={user};'
    f'PWD={password};'
    'Trusted_Connection=yes;'
)


def read_data_from_stg(tbl):
    # Read data
    query = f"SELECT * FROM {tbl};"
    df = pd.read_sql(query, conn)

    print(df)
    conn.close()


# read_data_from_stg('DimCustomer')

# load the data
folder_path = "Datasets"
# Get all file names
file_names = os.listdir(folder_path)
# Filter only files (ignoring directories)
file_names = [f for f in file_names if os.path.isfile(os.path.join(folder_path, f))]


# load customer data to SQL server
def load_customer():
    df = pd.read_csv(f"{folder_path}/{file_names[1]}")
    cursor = conn.cursor()
    for index, row in df.iterrows():
        cursor.execute("""
            INSERT INTO your_table (Customer_ID, DOB, Gender, City_Code)
            VALUES (?, ?, ?, ?)""", 
            row['customer_Id'], row['DOB'], row['Gender'], row['city_code']
        )
    conn.commit()
    cursor.close()
    conn.close()


def load_prod_cat():
    df = pd.read_csv(f"{folder_path}/{file_names[1]}")
    cursor = conn.cursor()
    for index, row in df.iterrows():
        cursor.execute("""
            INSERT INTO your_table (Prod_Cat_Code, Prod_Cat, Prod_Sub_Cat_Code, Prod_Subcat)
            VALUES (?, ?, ?, ?)""", 
            row['prod_cat_code'], row['prod_cat'], row['prod_sub_cat_code'], row['prod_subcat']
        )
    conn.commit()
    cursor.close()
    conn.close()


def load_transaction():
    df = pd.read_csv(f"{folder_path}/{file_names[2]}")
    cursor = conn.cursor()
    for index, row in df.iterrows():
        cursor.execute("""
            INSERT INTO your_table (Transaction_ID, Cust_ID, Tran_Date, Prod_Subcat_Code, 
            Prod_Cat_Code, Qty, Rate, Tax, total_Amt, Store_Type)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", 
            row['transaction_id'], row['cust_id'], row['tran_date'], row['prod_subcat_code'], row['prod_cat_code'],
            row['Qty'], row['Rate'], row['Tax'], row['total_amt'], row['Store_type']
        )
    conn.commit()
    cursor.close()
    conn.close()


def load_date():
    df = pd.read_csv(f"{folder_path}/{file_names[2]}")
    df['tran_date'] = df['tran_date'].str.replace('/', '-')
    df['tran_date'] = pd.to_datetime(df['tran_date'])
    df['year'] = df['tran_date'].dt.year
    df['month'] = df['tran_date'].dt.month
    df['day'] = df['tran_date'].dt.day
    df['weekday'] = df['tran_date'].dt.day_name() 
    
    cursor = conn.cursor()
    
    for index, row in df.iterrows():
        cursor.execute("""
            INSERT INTO your_table (Date_ID, Year, Month, Day, Weekday)
            VALUES (?, ?, ?, ?, ?)""", 
            row['transaction_id'], row['year'], row['month'], row['day'], row['weekday']
        )
    conn.commit()
    cursor.close()
    conn.close()


def load_all_data():
   # Load Excel file
   load_customer()
   time.sleep(1)
   load_prod_cat()
   time.sleep(1)
   load_transaction()
   time.sleep(1)
   load_date()



df = pd.read_csv(f"{folder_path}/{file_names[2]}")
df['tran_date'] = df['tran_date'].str.replace('/', '-')
df['tran_date'] = pd.to_datetime(df['tran_date'])
df['year'] = df['tran_date'].dt.year
df['month'] = df['tran_date'].dt.month
df['day'] = df['tran_date'].dt.day
df['weekday'] = df['tran_date'].dt.day_name() 
 
# print(df.head())


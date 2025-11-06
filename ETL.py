import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from sqlalchemy import create_engine, text
import sqlalchemy

# ===============================================================
# Load & Basic Setup
# ===============================================================
file_path = 'Mr.D/Housing/Luxury_Housing_Bangalore.csv'
df = pd.read_csv(file_path)

# Rename columns for consistency
df.rename(columns={
    'Ticket_Price_Cr': 'Amount',
    'Purchase_Quarter': 'Purchase_Date',
    'Unit_Size_Sqft': 'Area'
}, inplace=True)

# ===============================================================
# Date Processing
# ===============================================================
df['Purchase_Date'] = pd.to_datetime(df['Purchase_Date'], format='%Y-%m-%d', errors='coerce')
df['Year'] = df['Purchase_Date'].dt.year
df['Month'] = df['Purchase_Date'].dt.month_name()
df['Month_num'] = df['Purchase_Date'].dt.month
df['Quarter'] = df['Purchase_Date'].dt.quarter

# ===============================================================
# Clean & Standardize
# ===============================================================
if 'Micro_Market' in df.columns:
    df['Micro_Market'] = (
        df['Micro_Market']
        .astype(str)
        .str.strip()
        .str.lower()
        .str.replace(r'[^a-z\s]', '', regex=True)
        .str.title()
    )

# ===============================================================
# Numeric Cleaning (Safe Conversion)
# ===============================================================
for col in ['Amount', 'Area']:
    if col in df.columns:
        df[col] = (
            df[col]
            .astype(str)
            .str.replace(r'[^\d\.]', '', regex=True)
            .replace('', None)
        )
        df[col] = pd.to_numeric(df[col], errors='coerce')
        df.loc[df[col] == 0, col] = pd.NA
        

# ===============================================================
# Treat '1' in Area as garbage
# ===============================================================
if 'Area' in df.columns:
    df.loc[df['Area'] == 1, 'Area'] = pd.NA  # Treat 1 as invalid


# ===============================================================
# Fill Missing Values (Mean Imputation in Crores)
# ===============================================================
group_cols = ['Micro_Market', 'Developer_Name', 'Configuration', 'Month', 'Year']

for col in ['Area', 'Amount', 'Amenity_Score']:
    if col in df.columns:
        df[col] = df[col].fillna(df.groupby(group_cols)[col].transform('mean'))
        df[col] = df[col].fillna(df[col].mean())

# ===============================================================
# Create Exact Amount (in Rupees)
# ===============================================================
df["Exact_Amount"] = df["Amount"] * 1e7  # 1 Crore = 10,000,000 Rupees

# ===============================================================
# Final Validation
# ===============================================================
remaining_nulls = df.isnull().sum()
remaining_nulls = remaining_nulls[remaining_nulls > 0]

if not remaining_nulls.empty:
    print("\n⚠️ Remaining Nulls:")
    print(remaining_nulls)
else:
    print("\n✅ No missing values remaining!")

print("\n✅ Data cleaning & enrichment completed successfully!")

# ===============================================================
# Database Connection Setup
# ===============================================================
DB_USER = "root"               
DB_PASSWORD = "123456"  
DB_HOST = "localhost"
DB_PORT = "3306"
DB_NAME = "luxury_housing"
TABLE_NAME = "luxury_properties"

try:
    # Step 1: Connect without specifying DB (to create it if missing)
    root_engine = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/")

    with root_engine.connect() as conn:
        conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}"))
        print(f"✅ Database '{DB_NAME}' verified or created successfully!")

    # Step 2: Connect to the new DB
    engine = create_engine(f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

    # Step 3: Upload DataFrame
    df.to_sql(
        TABLE_NAME,
        con=engine,
        if_exists='replace',  # 'fail', 'replace', 'append'
        index=False,
        chunksize=1000,
        method='multi'
    )

    print(f"\n✅ Data successfully loaded into MySQL database '{DB_NAME}', table '{TABLE_NAME}'!")

except Exception as e:
    print("\n❌ Error while loading data into database:")
    print(e)


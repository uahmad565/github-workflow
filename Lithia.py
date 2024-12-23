
import pandas as pd
# New imports Start
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import URL
import pypyodbc # pip install pypyodbc
# New imports End
from concurrent.futures import ThreadPoolExecutor
import time
import requests
from datetime import datetime
import os
import re

# Headers for Lithia's API
headers = {
    'accept': '*/*',
    'accept-encoding': 'gzip, deflate, br, zstd',
    'accept-language': 'en-US,en;q=0.9,es-US;q=0.8,es;q=0.7,es-419;q=0.6',
    'cookie': 'locale=en_US; activeSession=true; __ssoid=1909f33ee6964fb4a69680adc7a6ded4; optimizelyEndUserId=oeu1722345989425r0.811100224970501; _gtm_group=false; _gcl_au=1.1.1625549624.1722345990; pxa_id=QQ9VdITqvX4JJzE07qh2pN54; __gclid=null; abc=lrIhN5IcPt77RzCPXpS2y1fW; abc_3rd_party=lrIhN5IcPt77RzCPXpS2y1fW; pixall_cookie_sync=true; pxa_at=true; _tt_enable_cookie=1; _ttp=Q-CXWF8tzmnBN3bHmp9hWhakAna; _ga_last=GA1.1.617850077.1722345990; __ggtruid=1722345991700.876f844c-e7ff-6b66-0f70-1bd402190223; ddc_abc_cache=lrIhN5IcPt77RzCPXpS2y1fW; /popups/entry.htm=true; DDC.postalCityState=BUENOSAIRES%2C%20%2C%20AR; DDC.userCoordinates=-34.59%2C-58.67; DDC.postalCode=; ddc_diag_akam_clientIP=191.82.15.134; ddc_diag_akam_currentTime=1722437034; ddc_diag_akam_requestID=3c3d7899; ddc_diag_akam_ghostIP=2.23.164.217; ddc_diag_akam_fullPath=/akam-sw-policy.json; callTrackingSessionId=kau6zh1ya6plz9yje02; _gid=GA1.2.1828756227.1722437035; _dc_gtm_UA-59371802-1=1; _dc_gtm_UA-49254695-1=1; AKA_A2=A; _ga=GA1.1.617850077.1722345990; bm_mi=86457BE8010D33F851CC7157D4CE13F6~YAAQ2fcSArzasQKRAQAAmGk/CRiy8Cs/bdQb9OvXorRUPNKqo4pBNAmaRsdS54n29UmGeMn/qJtFqHpea/FOmUd5an5p4K+lLK1VfVFaHcfaHl3MPAYdVlvz1JfXnyX3+1cWNym/pT3RY4PDiU7I4bgLNiWdC2gYqz20ga0VwrGer6vVm3IFXdAW+X8A7PrB/Sjl2doMCJ9TM4QTjKMYuLux6TCqg4Wkz3n4nRwoVKlcGUc2KYekeZn0eOSbpl5LI4eQhZdMsZkmqXJsvJj2jgxyH9ACqNMiTt1hHUkXf8ldwZy+5Yq/8ZPeKdEUwE5itjH1mu6PNrGSFs66hKxUy5MutdVRDoPgYkM=~1; pxa_ipv4=191.82.15.134; _uetsid=504148704f4b11efa4bc8305ad3fc368; _uetvid=54d93f804e7711efa3d0396281d5d2f6; _clck=1pfthrf%7C2%7Cfnx%7C0%7C1672; ddc_abcg_cache=; ddc_abcamm_cache=; ddc_abcc_cache=; ak_bmsc=78B81CD439FA0ECDD56FC0E792D7B790~000000000000000000000000000000~YAAQ2fcSAo/bsQKRAQAA+24/CRiXoE/gHONIVGrsDzD+BW/+J3CvBXuN3Cwo5P03AJr4taPzszej60xwH4waYcfeRZYRkVUy2wTfTSHKgVuQ2aEAzvNXB2x4ZS6Ap5VJS4qTiLQcT1qG/QLuJRHMXeYdA8WYGTfEgwW7pMtbHo4T5lFLfQPbl6EtK53guEEBxjK9bgxQ8zlqQfTFfzeMbtxSlzmj1jJsBjS72T5ynyKd4zoJFeyH8q5yFfl6ABKaIN/yb2CJLbo8F4kdaaHNPRS282JY6cwG8iGw1nJ51Jf7kLmBWfDXhZxSEgKtua5xkcB93h3ARbVOBCV1zKYo/kcmxC/b22h2YnmhKBiGhcxJ3VhAwSiENOkk4OK7z8PuJ1oYrGxXxXKytLyoBtnolgVFP831C8MUtfSK4PU0VgAvtUkBAWNmxOPVYM4+W3JNPLkQoB+vKjo2mKFuIUNG2pRIHEVaUzrliKIN6Ad4kjBFkNhCaa/2MR8J+ivJOkg=; __ggtrses=1; _clsk=1vpqy9b%7C1722437037097%7C1%7C1%7Cz.clarity.ms%2Fcollect; _ga_DRYFC644X2=GS1.1.1722437035.3.0.1722437044.0.0.0; akaalb_pixall_prod=1722438845~op=ddc_ana_pixall_prod:eng_ana_pixall_prod-pico-us-west-2|~rv=9~m=eng_ana_pixall_prod-pico-us-west-2:0|~os=6aafa3aac97a52a58cd06655a170720e~id=79251eda7cf9e81577ea1a5a6c4adf94; bm_sv=37D90A2183C16532BA5DAF11A5594900~YAAQ2fcSAjTjsQKRAQAAQac/CRhhJoHSbpatCixHifr1uGz35xsJk+uLR6OrYEWyMVTP37asoAkpScFXiGW7hdhdFEeiPWOBaYMN5PJ1QyuiopyoEBCxlO7rp1NtiIeGuuCvgmyaDvVSJv/2yngmU8aL0SVp5ngwE+QdhkXm9yChPDl+FMgVztAKPJR04ElcwnSJo1CD/60OoQFYWP5TYusF3GgZx4/o/Ig8I2rrnKy3GQ9QBQu8YDeTR/DwqCRWJ3qieLW9~1; RT="z=1&dm=www.hendrickcars.com&si=510ba677-ae38-484a-bc73-f35b6af8c4f9&ss=lz9yjb4x&sl=1&tt=6c7&rl=1&ld=6ci&ul=f0s&hd=fjp"; _ga_D88QPVDLZ3=GS1.1.1722437035.2.0.1722437051.44.0.0',
    'dnt': '1',
    'referer': 'https://www.lithia.com/new-inventory/index.htm?geoRadius=0&referrer=%2F&saveFacetState=true&geoZip=',
    'sec-ch-ua': '"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'no-cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
}


def get_data(start):
    global response
    max_retries = 5
    retries = 0

    while retries < max_retries:
        try:
            payload = {
                'geoRadius': 0,
                'referrer': '/',
                'saveFacetState': True,
                'geoZip': '',
                'start': start
            }
            response = requests.get(
                'https://www.lithia.com/apis/widget/INVENTORY_LISTING_DEFAULT_AUTO_ALL:inventory-data-bus1/getInventory',
                headers=headers,
                params=payload
            )
            response.raise_for_status()
            response_text = response.text
            jd = response.json().get('inventory', [])
            print(f"Start {start} contains {len(jd)} items.")

            # Regex to find the mileage (odometer) from the response text
            mileage_pattern = r'"odometer":\s*(\d+)'
            mileages = re.findall(mileage_pattern, response_text)  # Find all mileage values
            
            # Apply the found mileages to the items
            for i, item in enumerate(jd):
                item['mileage'] = int(mileages[i]) if i < len(mileages) else None  # Safely assign mileage or None if unavailable


            # Extract postal code for each item
            tracking_data = response.json().get('pageInfo', {}).get('trackingData', [])
            if tracking_data:
                for item in jd:
                    item['location'] = tracking_data[0]['address'].get('postalCode', None)
            else:
                for item in jd:
                    item['location'] = None  # Set location to None if tracking data is empty

           

            DATA.extend(jd)
            time.sleep(1)  # Introduce a delay between requests
            return DATA  # Return collected data
        except requests.exceptions.RequestException as e:
            retries += 1
            print(f"Retry {retries} for start {start} due to error: {e}")
            time.sleep(2 ** retries)  # Exponential backoff
    return []  # Return empty list if all retries fail

DATA = []
start = 0
items_per_page = 18  # Number of items per page
max_pages = 2000  # Maximum number of pages to fetch
max_items = items_per_page * max_pages  # Maximum number of items to fetch

with ThreadPoolExecutor() as executor:
    while start < max_items:
        results = list(executor.map(get_data, range(start, start + items_per_page * 5, items_per_page)))
        if all(res == 0 for res in results):
            break
        start += items_per_page * 5


# Convert to DataFrame and handle missing keys
df = pd.json_normalize(DATA)


# Get current date
current_date = datetime.now().strftime('%Y-%m-%d')
df['run_date'] = current_date  # Add run date column

# Add dealership column
df['dealership'] = 'lithia'  # Fill dealership with 'lithia'

# Extract pricing information
df['price'] = df['pricing.retailPrice']

# Rename postalCode to location
df.rename(columns={'postalCode': 'location'}, inplace=True)

# Add or create required columns, filling missing ones with None
required_columns = ['dealership', 'run_date', 'vin', 'year', 'make', 'model', 'price', 'condition', 'fuelType', 'mileage', 'location', 'classification', 'bodyStyle']
for col in required_columns:
    if col not in df.columns:
        df[col] = None  # Create the column if it doesn't exist

# Clean up the price column
df['price'] = df['price'].replace({r'\$': '', ',': ''}, regex=True).astype(float)



# Reorder the DataFrame to match the desired column order
df = df[required_columns]


#Data cleaning
# Dictionary to map bodyStyle categories to reduced categories
category_mapping = {
    'SUV': 'SUV',
    'SUVs': 'SUV',
    'Trucks': 'Truck',
    'Pickup': 'Truck',
    'Truck Crew Cab': 'Truck',
    'Cars': 'Car',
    'Sport Utility': 'SUV',
    'Hatchback': 'Car',
    'Coupe': 'Coupe',
    'Convertible': 'Convertible',
    'Crew Cab': 'Truck',
    'Van': 'Van',
    'Truck SuperCrew Cab': 'Truck',
    'Truck Double Cab': 'Truck',
    'Vans': 'Van',
    'Compact': 'Car',
    'Gran Coupe': 'Coupe',
    'Truck SuperCrew': 'Truck',
    'Truck CrewMax': 'Truck',
    'Minivan': 'Van',
    'Cargo Van': 'Van',
    'Wagon': 'Wagon',
    'Van Passenger Van': 'Van',
    'Minivan/Van': 'Van',
    'Sportback': 'Coupe',
    'Truck Regular Cab': 'Truck',
    'Crossover': 'SUV',
    '5-door': 'Car',
    'Double Cab Short Bed': 'Truck',
    'Van Cargo Van': 'Van',
    'Cabriolet': 'Convertible',
    'Passenger Van': 'Van',
    'Truck Quad Cab': 'Truck',
    'CROSSOVERS': 'SUV',
    'Car': 'Car',
    'CrewMax Extra Short Bed': 'Truck',
    'Truck King Cab': 'Truck',
    'Truck CrewMax Cab': 'Truck',
    'Quad Cab': 'Truck',
    'Sports Activity Coupe': 'Coupe',
    'Truck Super Cab': 'Truck',
    'Specialty': 'Other',
    'Truck Access Cab': 'Truck',
    'Sport Utility Vehicle': 'SUV',
    'Truck SuperCab': 'Truck',
    'LARIAT 4WD Crew Cab 6.75 Box': 'Truck',
    'Double Cab Long Bed': 'Truck',
    'Van Extended Cargo Van': 'Van',
    'CUV': 'SUV',
    'Mega Cab': 'Truck',
    'Van Medium Roof Van': 'Van',
    'Roadster': 'Convertible',
    'Regular Cab': 'Truck',
    'Truck Mega Cab': 'Truck',
    'CrewMax Short Bed': 'Truck',
    'SAV': 'SUV',
    'Truck Extended Cab': 'Truck',
    'Long Bed': 'Truck',
    'Sportshatch': 'Car',
    'Laramie 4x4 Mega Cab 64 Box': 'Truck',
    'Van Low Roof Van': 'Van',
    'Truck': 'Truck',
    'Van High Roof Ext. Van': 'Van',
    'Sedan': 'Car'
}

# Dictionary to map fuelType categories to reduced categories
fuel_type_mapping = {
    'Gasoline': 'Gasoline',
    'Gasoline Fuel': 'Gasoline',
    'Hybrid': 'Hybrid',
    'Electric': 'Electric',
    'Diesel': 'Diesel',
    'Hybrid Fuel': 'Hybrid',
    'Plug-In Hybrid': 'Hybrid',
    'Other': 'Other',
    'Diesel Fuel': 'Diesel',
    'Electric Fuel System': 'Electric',
    'Flexible': 'Flex Fuel',
    'Gas/Electric Hybrid': 'Hybrid',
    'Gasoline/Mild Electric Hybrid': 'Hybrid',
    'Plug-In Electric/Gas': 'Hybrid',
    'Flex Fuel Capability': 'Flex Fuel',
    'Flex Fuel': 'Flex Fuel',
    'G': 'Gasoline',
    'Hydrogen Fuel': 'Other',
    'Gas Regular Unleaded': 'Gasoline',
    'Plug-In Electric/Hydrogen': 'Hybrid',
    'Hydrogen Fuel Cell': 'Other',
    'Hybrid/Electric': 'Hybrid',
    'Natural Gas': 'Other',
    'Gas Premium Unleaded,Gas Regular Unleaded': 'Gasoline',
    'Gasoline Fueled by': 'Gasoline',
    'Natural Gas Fuel': 'Other'
}



def data_formatting(df):
    # Remap the bodyStyle column
    df['bodyStyle'] = df['bodyStyle'].map(category_mapping).fillna('Other')
    
    # Remap the fuelType column
    df['fuelType'] = df['fuelType'].map(fuel_type_mapping).fillna('Other')
    
    # Convert price, mileage, and year columns to numeric, forcing non-numeric to NaN
    df['price'] = pd.to_numeric(df['price'], errors='coerce')
    df['mileage'] = pd.to_numeric(df['mileage'], errors='coerce')
    df['year'] = pd.to_numeric(df['year'], errors='coerce')
    
    # Set mileage to 0 if condition is new/New and mileage is NaN
    df.loc[(df['condition'].str.lower() == 'new') & (df['mileage'].isna()), 'mileage'] = 0
    
    # Remove the top and bottom 1% of price
    price_upper_limit = df['price'].quantile(0.99)
    price_lower_limit = df['price'].quantile(0.01)
    df = df[(df['price'] <= price_upper_limit) & (df['price'] >= price_lower_limit)]
    
    # Remove the top 1% of mileage, but keep NaN and cars with condition new/New
    mileage_upper_limit = df['mileage'].quantile(0.99)
    df = df[(df['mileage'].isna()) | (df['mileage'] <= mileage_upper_limit)]
    
    # Remove the bottom 1% of mileage, but keep NaN and cars with condition new/New
    mileage_lower_limit = df['mileage'].quantile(0.01)
    df = df[
        (df['mileage'].isna()) | 
        (df['mileage'] >= mileage_lower_limit) | 
        (df['condition'].str.lower() == 'new')
    ]
    
    # Remove duplicate rows based on the 'vin' column
    df = df.drop_duplicates(subset='vin', keep='first')
    
    return df

def create_table_from_dataframe(df, engine, table_name):
    """
    Create table in SQL Server based on the DataFrame schema.
    """
    ddl = pd.io.sql.get_schema(df, table_name, con=engine)
    # Create the table using the DDL script 
    with engine.connect() as connection: 
        connection.execute(text(ddl)) 


def upload_file_to_mssql(connection_string, df, table_name):    
    connection_url = URL.create('mssql+pyodbc', query={'odbc_connect': connection_string})
    engine = create_engine(connection_url, module=pypyodbc)
    inspector = inspect(engine)
     # Check if the table already exists
    if inspector.has_table(table_name):
        # Fetch existing data from the table
        existing_df = pd.read_sql(f"SELECT * FROM {table_name}", engine)

        # Find new rows (i.e., rows that don't already exist)
        df_new = df[~df.isin(existing_df)].dropna()

        # Insert only new data into the table
        df_new.to_sql(table_name, engine, if_exists='append', index=False)
    else:
        print("Table does not exist, going to create a new table.")
        create_table_from_dataframe(df, engine, TABLE_NAME)
        print("Table created successfully.")
        df.to_sql(table_name, engine, if_exists='append', index=False)
        



df = data_formatting(df)

# upload dataframe to sql server
SERVER_NAME = os.environ['SERVER_NAME']
DATABASE_NAME = os.environ['DATABASE_NAME']
TABLE_NAME = os.environ['TABLE_NAME']
USERNAME = os.environ['USERNAME']
PASSWORD = os.environ['PASSWORD']

connection_string = f"""
    DRIVER={{ODBC Driver 17 for SQL Server}};
    SERVER={SERVER_NAME};
    DATABASE={DATABASE_NAME};
    UID={USERNAME};
    PWD={PASSWORD};
"""
try:
    upload_file_to_mssql(connection_string, df, TABLE_NAME)
    print(f"Data successfully stored in table '{TABLE_NAME}'")
except Exception as e:
    print(f"Upload sql server error: {e}")

exit()
import pandas as pd
import numpy as np
from pathlib import Path
import logging

# 1. Setup Logging for Debugging
# Change level to logging.DEBUG to see more details, or logging.INFO for a cleaner output
logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')

def standardize_and_export(df, item_name, output_filename):
    """Helper function to apply standard columns and export to CSV"""
    logging.info(f"Finalizing {item_name} with {len(df)} records.")
    
    df['StateID'] = '54'
    df['BeginDate'] = '01/01/2025'
    df['ValueDate'] = df.get('ValueDate', '')
    df['ValueNumeric'] = df.get('ValueNumeric', '')
    df['ValueText'] = ''
    df['DataItem'] = item_name
    
    # Ensure column order matches standard FHWA format if needed here
    df.to_csv(f'{output_filename}.csv', sep='|', index=False)
    logging.info(f"Saved -> {output_filename}.csv\n")

# 2. File Ingestion
mypath_sample = Path('../samples/samples_2026/2025_sample_limits.csv',dtype={'SampleId':'str','RouteID':'str'})
mypath = Path('./2026_raw_district_data')
year_last_base = Path('Year_first_doc_RoadwayHistory.csv')

# NOTE: The original code read this CSV but didn't assign it. 
df_sample = pd.read_csv(mypath_sample,sep = '|')
year_last = pd.read_csv(year_last_base)
sample_dict = dict(zip(df_sample['SampleId'],df_sample['RouteID']))
year_last_dict = dict(zip(year_last['RouteID'],year_last['Year'])) 

logging.info(f"Reading raw excel files from {mypath}...")
excel_files = [f for f in mypath.iterdir() if f.is_file() and f.suffix in ['.xls', '.xlsx']]

df_list = []
for file in excel_files:
    logging.debug(f"Reading file: {file.name}")
    df_list.append(pd.read_excel(file,dtype={'RouteID':'str','SampleId':'str'}))

df = pd.concat(df_list, ignore_index=True)
print(df['SampleId'].unique())
df['Comments'] = ''
df['SampleId'] = df['SampleId'].str.zfill(13)
df['RouteID'] = df['RouteID'].str.zfill(13)
print(df['SampleId'].unique())
df['RouteID'] = df['RouteID'].astype(str) # Standardize RouteID early



for key,value in sample_dict.items():
    df.loc[df['SampleId'] == key,'test'] = sample_dict.get(key)

for key,value in year_last_dict.items():
    df.loc[df['RouteID'] == key,'Year Last Construction'] = year_last_dict.get(key)

for key,value in year_last_dict.items():
    df.loc[(df['RouteID'] == key) & (df['Year Last Improvement'].isna()),'Year Last Improvement'] = year_last_dict.get(key)


df = df.loc[(df['RouteID'] != '2040009030000') & (df['BMP'] != 3.4900) &(df['EMP'] != 6.7600)]
df = df.loc[(df['RouteID'] != '47202190000NB') & (df['BMP'] != 27.8000) &(df['EMP'] != 27.9000)]
logging.info(f"Total raw records loaded: {len(df)}")

# 3. Process Columns
column_list = [
    'Year Last Improvement', 'Year Last Construction', 'Last Overlay Thickness',
    'Thickness Rigid', 'Thickness Flexible', 'Base Type', 'Base Thickness'
]

for col in column_list:
    logging.info(f"--- Processing {col} ---")
    
    # Isolate relevant columns and drop NAs / Zero-length segments
    di_df = df[['RouteID', 'BMP', 'EMP', col, 'Comments']].copy()
    di_df = di_df.dropna(subset=['RouteID', 'BMP', 'EMP', col])
    di_df = di_df[di_df['BMP'] != di_df['EMP']]
    di_df = di_df[di_df[col] != '*']

    if col == 'Base Type':
        # Base Type Manipulation
        replace_dict = {
            'Asphalt': 3, 'Asphalt ': 3, 'HMA': 5, 'Concrete': 6, 'PCC': 8,
            'Base II': 2, 'Base 2': 2, ' ': 25, 'Unknown': 26, 'Superpave TY 25': 27,
            'Superpave': 28, 'by D10': 29, 'by the ': 30, '2/3': 3, '5/7': 7,
            '2/5': 5, '2/3/5': 2, '2/5/7': 2, '2/7': 2
        }
        di_df[col] = di_df[col].replace(replace_dict)
        di_df[col] = pd.to_numeric(di_df[col], errors='coerce')
        
        di_df.rename(columns={col: 'ValueNumeric'}, inplace=True)
        
        # Filtering
        di_df = di_df[di_df['ValueNumeric'].isin([1, 2, 3, 4, 5, 6, 7, 8])]
        di_df = di_df[di_df['RouteID'] != '940003000000']
        di_df = di_df.drop_duplicates(['RouteID', 'BMP', 'EMP'])
        
        standardize_and_export(di_df, 'BASE_TYPE', col)

    elif col == 'Base Thickness':
        # Base Thickness Manipulation
        invalid_vals = ['Unknown', 0, 'D1 Responsibility', 'Non-State Road', 'Non-State']
        di_df = di_df[~di_df[col].isin(invalid_vals)]
        
        # Clean strings and convert to numeric
        di_df[col] = di_df[col].astype(str).str.rstrip(' "')
        di_df[col] = pd.to_numeric(di_df[col], errors='coerce')
        
        di_df.rename(columns={col: 'ValueNumeric'}, inplace=True)
        di_df = di_df.dropna(subset=['ValueNumeric'])
        di_df = di_df[di_df['ValueNumeric'] != 0]
        di_df = di_df.drop_duplicates(['RouteID', 'BMP', 'EMP'])
        
        standardize_and_export(di_df, 'BASE_THICKNESS', col)

    elif col == 'Thickness Flexible':
        # 1. Handle known fractions/special cases FIRST
        di_df[col] = di_df[col].astype(str).replace({'2 / 1.5': '1.33'})
        
        # 2. Extract only the numerical part from the string (e.g., "3.5 inches" -> "3.5")
        di_df[col] = di_df[col].str.extract(r'(\d+\.?\d*)', expand=False)
        
        # 3. Now safely convert to numeric; anything that didn't contain a number becomes NaN
        di_df[col] = pd.to_numeric(di_df[col], errors='coerce')
        
        di_df.rename(columns={col: 'ValueNumeric'}, inplace=True)
        
        # 4. Filter out NaNs and 0s
        di_df = di_df.dropna(subset=['ValueNumeric'])
        di_df = di_df[di_df['ValueNumeric'] > 0]
        
        di_df = di_df.drop_duplicates(['RouteID', 'BMP', 'EMP'])
        
        standardize_and_export(di_df, 'THICKNESS_FLEXIBLE', col)

    elif col == 'Thickness Rigid':
        # Thickness Rigid Manipulation
        di_df = di_df[di_df[col] != 0]
        di_df[col] = di_df[col].astype(str).str.rstrip('"')
        di_df[col] = pd.to_numeric(di_df[col], errors='coerce')
        
        di_df.rename(columns={col: 'ValueNumeric'}, inplace=True)
        di_df = di_df.dropna(subset=['ValueNumeric'])
        di_df = di_df.drop_duplicates(['RouteID', 'BMP', 'EMP'])
        
        standardize_and_export(di_df, 'THICKNESS_RIGID', col)

    elif col == 'Last Overlay Thickness':
        # 1. Handle known fractions/special cases FIRST
        di_df[col] = di_df[col].astype(str).replace({'1.5 / 2': 0.75})
        
        # 2. Extract only the numerical part from the string
        di_df[col] = di_df[col].str.extract(r'(\d+\.?\d*)', expand=False)
        
        # 3. Safely convert to numeric
        di_df[col] = pd.to_numeric(di_df[col], errors='coerce')
        di_df.rename(columns={col: 'ValueNumeric'}, inplace=True)
        
        # 4. Filter out NaNs and enforce your >= 0.5 rule
        di_df = di_df.dropna(subset=['ValueNumeric'])
        di_df = di_df[di_df['ValueNumeric'] >= 0.5]
        
        # 5. Clean up RouteIDs and deduplicate
        di_df = di_df[~di_df['RouteID'].isin(['940003000000', '940011000000'])]
        di_df = di_df.drop_duplicates(['RouteID', 'BMP', 'EMP'])
        
        standardize_and_export(di_df, 'LAST_OVERLAY_THICKNESS', col)

    elif col == 'Year Last Construction':
        # Year Last Construction Manipulation
        invalid_vals = ['Unknown', 'nan', '2','226']
        di_df = di_df[~di_df[col].astype(str).isin(invalid_vals)]
        
        # String splitting
        di_df[col] = di_df[col].astype(str).str.split('-').str[0].str.split('.').str[0]
        
        di_df.rename(columns={col: 'ValueDate'}, inplace=True)
        
        # Check and replace 2026
        if (di_df['ValueDate'] == '2026').any():
            logging.warning("Found '2026' in Year Last Construction. Coercing to '2025'.")
        di_df['ValueDate'] = di_df['ValueDate'].replace('2026', '2025')
        


        di_df = di_df[di_df['ValueDate'].str.strip() != '']
        di_df = di_df.dropna(subset=['ValueDate'])
        di_df = di_df.drop_duplicates(['RouteID', 'BMP', 'EMP'])
        
        standardize_and_export(di_df, 'YEAR_LAST_CONSTRUCTION', col)

    elif col == 'Year Last Improvement':
        # 1. Initial cleanup: grab the first chunk of data before dashes, dots, or slashes
        di_df[col] = di_df[col].astype(str).str.split('-').str[0].str.split('.').str[0].str.split(' /').str[0].str.strip()
        
        di_df.rename(columns={col: 'ValueDate'}, inplace=True)
        
        # 2. Force coercion: anything that isn't a number becomes NaN
        di_df['ValueDate'] = pd.to_numeric(di_df['ValueDate'], errors='coerce')

        
        # 3. Drop the NaNs (this instantly eliminates 'Turnpike', 'Unknown', 'No info', etc.)
        di_df = di_df.dropna(subset=['ValueDate'])
        
        # 4. Strict bounds checking (Change 1900 to whatever your earliest valid WVDOT year is)
        di_df = di_df[(di_df['ValueDate'] >= 1900) & (di_df['ValueDate'] <= 2026)]
        
        # 5. Convert back to a clean string format for FHWA ('2015.0' -> 2015 -> '2015')
        di_df['ValueDate'] = di_df['ValueDate'].astype(int).astype(str)
        
        # 6. Apply your specific 2026 -> 2025 coercion rule
        if (di_df['ValueDate'] == '2026').any():
            logging.warning("Found '2026' in Year Last Improvement. Coercing to '2025'.")
        di_df['ValueDate'] = di_df['ValueDate'].replace('2026', '2025')
        
        # Final filtering and deduplication
        di_df = di_df[~di_df['RouteID'].isin(['940003000000', '940011000000'])]
        di_df = di_df.drop_duplicates(['RouteID', 'BMP', 'EMP'])
        
        standardize_and_export(di_df, 'YEAR_LAST_IMPROVEMENT', col)

    else:
        logging.error(f"Unrecognized column/item: {col}")

logging.info("Processing complete.")
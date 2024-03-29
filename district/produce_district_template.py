import os 
import pandas as pd
import numpy as np
from datetime import date

df1 = pd.read_excel('district/district_template.xlsx', dtype={'Sample_ID':str})
samples = df1.Sample_ID.unique()

lrs_surf_dict = {0:0, 1:1, 2:1, 3:1, 4:1, 5:1, 6:6, 7:6, 8:6, 9:6, 10:6, 11:6, 12:3, 13:11}
pave_surf_dict_24 = {'':0, 'JCP': 3,'CRC':3, 'ASP': 6,'BRI':11,'OTH':11}

district_dict={1:7,2:5,3:1,4:7,5:6,6:2,7:3,8:1,9:4,10:9,11:7,12:5,13:9,14:5,15:6,16:5,17:4,18:3,19:5,20:1,21:7,22:2,23:2,24:10,25:4,26:6,27:1,28:10,29:5,30:2,31:4,32:9,33:5,34:9,35:6,36:8,37:3,38:8,39:4,40:1,41:10,42:8,43:3,44:3,45:9,46:4,47:8,48:6,49:7,50:2,51:7,52:6,53:3,54:3,55:10}


pave_lrs = {
    1:'PRIMITIVE ROAD',
    2:'UNIMPROVED ROAD',
    3:'GRADED AND DRAINED ROAD',
    4:'SOIL SURFACE ROAD - DIRT',
    5:'GRAVEL OR STONE ROAD',
    6:'BIT. SURFACE TREATED ROAD',
    7:'MIXED BITUMINOUS ROAD LESS THAN 7" COMBINED THICKNESS',
    8:'MIXED BITUMINOUS ROAD 7" OR MORE COMBINED THICKNESS',
    9:'BITUMINOUS PENET. ROAD LESS THAN 7" COMBINED THICKNESS',
    10:'BITUMINOUS PENET. ROAD 7" OR MORE COMBINED THICKNESS',
    11:'ASPHALTIC CONCRETE ROAD',
    12:'CONCRETE',
    13:'BRICK'
}
# df_pav = pd.read_excel('pavement_output_3_4_24/2023_COMBINED_ROUTES_DATA_ALL_3_4_24.xlsx')
# df_pav.to_csv('pavement_output_3_4_24/2023_COMBINED_ROUTES_DATA_ALL_3_4_24.csv', index=False)

# df = pd.read_excel('district/district_template.xlsx')

# print(df.columns)
# print(df['MBI_SURFACE_TYPE'].unique())
# print(df['District'].unique())
#  lrsops rhoverlay -l "2,35,70" --carry_json '{"70":["SURFACE_TYPE_ID"],"2":["AADT_YEAR"]}' -o AADT_SURFACE_FSYSTEM.csv

# os.system(f'lrsops rhoverlay -l "70,97,49" -o district/Surface_type.csv')

# os.system(f'lrsops overlay -b pavement_output_3_4_24/2023_COMBINED_ROUTES_DATA_ALL_3_4_24.csv -s district/Surface_type.csv -c 70_SURFACE_TYPE -o district/compare_surfaceType.csv')
os.system(f'lrsops overlay -b district/Samples.csv -s district/2023_COMBINED_ROUTES_DATA_ALL_3_4_24.csv -c SURF_TYPE -o district/temp.csv')
os.system(f'lrsops overlay -b district/temp.csv -s district/Surface_type.csv -c 70_SURFACE_TYPE -o district/temp.csv')

df = pd.read_csv('district/temp.csv', dtype={'29_HPMS_SAMPLE_NO':str})
df.rename(columns={'29_HPMS_SAMPLE_NO':'Sample_ID', 'SURF_TYPE':'FUGRO_SURFACE_TYPE', '70_SURFACE_TYPE':'LRS_SURFACE_TYPE'}, inplace=True)


today = date.today()
df = df[df['Sample_ID'].notna()]

df['Year_Record'] = today.year -1
df['State_Code'] = 54
df['county'] = df['RouteID'].str.slice(0,2).astype(int)
print(np.sort(df.county.unique()))

df['District'] = df.county.map(district_dict).astype(int)
print(df['District'].unique())

df['LRS_SURFACE_TYPE'] = df['LRS_SURFACE_TYPE']
df[df['Sample_ID'].isin(samples)]
del df['county']
df['Section_Length'] = round(abs(df.EMP - df.BMP),3)

'''
### Result columns
   - Year_Record	
   - State_Code	
   - District	
   - Section_Length	
   - Sample_ID	
   - MBI_SURFACE_TYPE	(obs: pavement file)
   - LRS_SURFACE_TYPE	
   - RouteID	
   - BMP	
   - EMP	
   - Year Last Improvement	
   - Year Last Constructed	
   - Last Overlay Thickness	
   - thickness Rigid	
   - Thickness Flex	
   - Base Type	
   - Base Thickness

'''
df = df[['Year_Record','State_Code','District','Section_Length','Sample_ID','FUGRO_SURFACE_TYPE','LRS_SURFACE_TYPE','RouteID','BMP','EMP']]
df['Year Last Improvement'] = ''
df['Year Last Constructed'] = ''
df['Last Overlay Thickness'] = ''
df['Thickness Rigid'] = ''
df['Thickness Flex'] = ''	
df['Base Type'] = ''	
df['Base Thickness'] = ''

print(df)

df.to_excel('district/District_template_2024.xlsx', index=False)

# test sampples


print(samples)

print(df[df['Sample_ID'].isin(samples)])
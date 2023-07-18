from combine_items import combine_errors  
import geopandas as gpd 
import pandas as pd 
import os

joined = gpd.read_file(r"C:\Users\E025205\Downloads\FullSpatialJoin_FGDB\FullSpatialJoin_54_2023.gdb")
joined.drop(['geometry'],axis=1,inplace=True)
joined.to_csv('full_spat.csv',index=False)


errors = gpd.read_file(r'C:\Users\E025205\Downloads\HPMS_RoadEventValidationResults_GDB_8728193c-6465-4559-a0e5-5f803642ff9c\Non-Conformances_2023.gdb')
df = combine_errors(errors,'full_spat.csv',dtype={'UrbanIdVn':str,'SampleId':str})

df.to_csv('full_spat_with_error_columns.csv',index=False)
os.remove('full_spat.csv')
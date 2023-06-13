import pandas as pd
import os


data_items = {
    'F_System': ['DataItem1_F_System.csv', 'FsystemVn'],
    'Ownership': ['DataItem6_Ownership.csv', 'OwnershipVn']
    }


for data_item, tmp in data_items.items():
    submitted = pd.read_csv(tmp[0], sep='|')

    validations = pd.read_excel(r'spatial_inventory_val_filtered.xlsx')

    submitted = submitted.rename(columns={'Route_ID': 'ROUTEID', 'Begin_Point': 'BMP', 'End_Point':'EMP'})
    validations = validations.rename(columns={'RouteId':'ROUTEID', 'BeginPoint':'BMP', 'EndPoint':'EMP'})

    submitted.to_csv('submitted.csv', index=False)
    validations.to_csv('validations.csv', index=False)

    os.system(f'lrsops overlay -b submitted.csv -s validations.csv -c "{tmp[1]},ValidationDescription" -o tmp.csv')
    os.system(f'lrsops overlay -b tmp.csv -s LRSE_SURFACE_TYPE_evw.csv -c "SURFACE_TYPE" -o {tmp[1]}_compare_6_8_2023.csv')
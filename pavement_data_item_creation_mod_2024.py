import pandas as pd
import numpy as np
import os




def overlay(f1, f2, cols,prefix,output):
    os.system(f'lrsops overlay -b {f1} -s {f2}  -c "{cols}" --prefix "{prefix}" -o {output}')


data_cols = ['RUT_MEAN', 'FAULT_AVG', 'HPMS_Cracking_Percent', 'IRI_MEAN', 'SURF_TYPE'] #use this for the excel file verson of the pavement data
# data_cols = ['RouteID', 'BMP', 'EMP', '125_PVMT_SHLD_TYPE', '125_RUT_MEAN','125_IRI_MEAN', '125_PVMT_SURFACE_TYPE', '125_COND_YEAR','125_HPMS_Cracking_Percent', '125_FAULT_AVG'] #use this for the csv lrs dump given by fernanda
route_cols = ['ROADNAME', 'BEG_MP', 'END_MP', 'COND_YEAR'] # use this for the excel version of the pavement data.
# route_cols = ['RouteID','BMP','EMP','125_COND_YEAR'] #use this for the csv lrs dump given by fernanda
rename_dict = {
    'ROADNAME': 'RouteID',
    'BEG_MP': 'BeginPoint',
    'END_MP':'EndPoint',
    'RUT_MEAN':'RUTTING',
    'FAULT_AVG':'FAULTING',
    'HPMS_Cracking_Percent':'CRACKING_PERCENT',
    'IRI_MEAN': 'IRI',
    'SURF_TYPE': 'SURFACE_TYPE',
    'SHLD_TYPE': 'SHOULDER_TYPE',
    'COND_YEAR': 'ValueDate'
} #use this for the excel version
# rename_dict = {
#     'RouteID': 'RouteID',
#     'BMP': 'BeginPoint',
#     'EMP':'EndPoint',
#     '125_RUT_MEAN':'RUTTING',
#     '125_FAULT_AVG':'FAULTING',
#     '125_HPMS_Cracking_Percent':'CRACKING_PERCENT',
#     '125_IRI_MEAN': 'IRI',
#     '125_PVMT_SURFACE_TYPE': 'SURFACE_TYPE',
#     '125_PVMT_SHLD_TYPE': 'SHOULDER_TYPE',
#     '125_COND_YEAR': 'ValueDate'
# } #use this for the csv version

data_number = {
    'RUTTING': '50',
    'FAULTING': '51',
    'CRACKING_PERCENT': '52',
    'IRI': '47',
    'SURFACE_TYPE': '49',
    'SHOULDER_TYPE': '37'
}


rename_dict2 = {
    'BMP':'BeginPoint',
    'EMP':'EndPoint',
    '125_RUTTING':'RUTTING',
    '125_FAULTING':'FAULTING',
    '125_CRACKING_PERCENT':'CRACKING_PERCENT',
    '125_IRI': 'IRI',
    '125_SURFACE_TYPE': 'SURFACE_TYPE',
    '125_ValueDate': 'ValueDate',
    '125_SHOULDER_TYPE': 'SHOULDER_TYPE'
}


data_items = ['RUTTING', 'FAULTING', 'CRACKING_PERCENT', 'IRI', 'SURFACE_TYPE']
# master = pd.read_excel(f'pavement_output_3_4_24\\2023_COMBINED_ROUTES_DATA_ALL_3_4_24.xlsx', usecols=data_cols + route_cols) # last year's pavement data
master = pd.read_excel('./pavement_output_3_4_24/WV25_Distress_MergedDelivery_HPMS.xlsx', usecols=data_cols + route_cols) # the excel version of the pavement data
# master = pd.read_csv(f'pavement_output_3_4_24\\pavementData_lrs_pull_040925.csv', usecols=data_cols + route_cols) # outdated version
# master = pd.read_csv(f'pavement_output_3_4_24\\lrs_pavement_data_03_26_26.csv', usecols=data_cols + route_cols)
# master = master[master['SHLD_TYPE'].notna()]
# master = master[master['125_PVMT_SHLD_TYPE'].notna()]
# master = master[master['SHLD_TYPE']!='CURB']
# master = master[master['SHLD_TYPE']!='Curb']
# master = master[master['IRI_MEAN']!='0.0']
master = master[master['IRI_MEAN']!=0.0]
# master['125_IRI_MEAN'] = master['125_IRI_MEAN'].round()
# master['125_FAULT_AVG'] = master['125_FAULT_AVG'].round(2)
# master['PERCENT_CRACKING'] = master['PERCENT_CRACKING'].round(2)
master.rename(columns=rename_dict, inplace=True)
master['supp_code'] = master['RouteID'].str.slice(9,11)
master = master[master['supp_code']!= '18']
master.drop(columns=['supp_code'],inplace=True)
# master.drop(master[(master["RouteID"] == "1740707000000") & (master["BeginPoint"] == 0.9) & (master["EndPoint"] == 0.91)].index,inplace=True,)
# master.drop(master[(master["RouteID"] == "4130041000000") & (master["BeginPoint"] == 1.69) & (master["EndPoint"] == 1.7)].index,inplace=True,)
master.drop_duplicates(subset = ['RouteID','BeginPoint','EndPoint'],keep = False,inplace = True)
# test = ( (master["RouteID"] == "41200190000NB") & (master["BeginPoint"] == 16.9) & (master["EndPoint"] == 16.925))
# test2 = ( (master["RouteID"] == "4130041000000") & (master["BeginPoint"] == 1.7) & (master["EndPoint"] == 1.8))
# master.loc[test, ["EndPoint"]] = 16.92
# master.loc[test2, ["BeginPoint"]] = 1.763
# master = master[master['RouteID'].str[2] == '1']

master.to_csv('./temp_pav/WV25_Distress_MergedDelivery_HPMS.csv', index=False)
print(master.columns)

overlay(r'./pavement_output_3_4_24/arnold_26.csv', r'./temp_pav/WV25_Distress_MergedDelivery_HPMS.csv', 'RUTTING,FAULTING,CRACKING_PERCENT,IRI,SURFACE_TYPE,ValueDate', '125_', './temp_pav/april_data_items_overlay.csv')
def convert_date(x):
    day,month,year = x.split('/')
    out = '/'.join([month,day,year])
    return out


def load_defaults(df):
    df['BeginDate'] = '01/01/2025'
    df['StateID'] = '54'
    df['Comments'] = ''
    df['ValueDate'] = '06/24/2025'
    return df


def sort_cols(df):
    df = df[['BeginDate', 'StateID', 'RouteID', 'BeginPoint', 'EndPoint', 'DataItem', 'ValueNumeric', 'ValueText', 'ValueDate', 'Comments']]
    return df


def bmp_emp_map(row):
    if row['EndPoint'] < row['BeginPoint']:
        a = row['BeginPoint']
        b = row['EndPoint']
        row['EndPoint'] = a
        row['BeginPoint'] = b
    return row

def create_data_item(df, data_item):
    df = df[['RouteID', 'BeginPoint', 'EndPoint', f'{data_item}', 'ValueDate']]
    df = load_defaults(df)
    df.rename(columns={f'{data_item}':'ValueNumeric'}, inplace=True)
    df = df[df['ValueNumeric'] != -1]
    df['DataItem'] = f'{data_item}'
    df['ValueText'] = ''
    df = sort_cols(df)
    df = df.apply(bmp_emp_map,axis = 1)
    return df

master2 = pd.read_csv('./temp_pav/april_data_items_overlay.csv')
master2.rename(columns=rename_dict2, inplace=True)
master2 = master2[['RouteID', 'BeginPoint', 'EndPoint', 'RUTTING', 'FAULTING', 'CRACKING_PERCENT', 'IRI', 'SURFACE_TYPE', 'ValueDate','OBJECTID']]
master2 = master2[(master2['ValueDate'].notna()) & (master2['OBJECTID'].notna())]
print(master2.columns)
data_item_dict = {}
for i in data_items:
    data_item_dict[i] = create_data_item(master2, i)
    # --- Fixing the values for IRI, if IRI < 31, set the value to 31. If the value > 399 set the value 399.
    if(i == 'IRI'):
        data_item_dict[i]['ValueNumeric'] = np.where(data_item_dict[i]['ValueNumeric'].astype(int) <31, 31, data_item_dict[i]['ValueNumeric'].astype(int))
        data_item_dict[i]['ValueNumeric'] = np.where(data_item_dict[i]['ValueNumeric'].astype(int) >399, 399, data_item_dict[i]['ValueNumeric'].astype(int))

surf_dict = {'JCP': 3,'CRC':5, 'ASP': 6,'BRI':11,'OTH':11}
data_item_dict['SURFACE_TYPE']['ValueNumeric'] = data_item_dict['SURFACE_TYPE']['ValueNumeric'].map(lambda x: surf_dict[x])


def shoulder_mapper(x):
    if int(x) == 7:
        return 1
    else:
        return x
    



shld_dict = {'COMBO':5,'EARTH':6,'GRAVEL':4,'NONE':1,'NULL':1,'PAVED':2,'Curb':1,'CURB':1}
# data_item_dict['SHOULDER_TYPE']['ValueNumeric'] = data_item_dict['SHOULDER_TYPE']['ValueNumeric'].map(lambda x : shld_dict[x])
# data_item_dict['SHOULDER_TYPE'] = data_item_dict['SHOULDER_TYPE'].loc[data_item_dict['SHOULDER_TYPE']['ValueNumeric'].astype('string') != '-1']


for k,v in data_item_dict.items():
    print(k, '\n', v, '\n\n\n')
    v.to_csv(f'pavement_output/DataItem{data_number[k]}_{k}.csv', index=False, sep='|')




# def create_rutting(df):
#     df = df[['RouteID', 'BeginPoint', 'EndPoint', 'RUTTING', 'ValueDate']]
#     df.rename(columns={'RUTTING': 'ValueNumeric'}, inplace=True)
#     df['DataItem'] = 'RUTTING'
#     df['ValueText'] = ''
#     return df


# def create_faulting(df):
#     df = df[['RouteID', 'BeginPoint', 'EndPoint', 'FAULTING', 'ValueDate']]
#     df.rename(columns={'FAULTING': 'ValueNumeric'}, inplace=True)
#     df['DataItem'] = 'FAULTING'
#     df['ValueText'] = ''
#     return df




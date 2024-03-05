import pandas as pd

data_cols = ['RUT_MEAN', 'FAULT_AVG', 'PERCENT_CRACKING', 'IRI_MEAN', 'SURF_TYPE', 'SHLD_TYPE']
route_cols = ['ROADNAME', 'BEG_MP', 'END_MP', 'DATE']
rename_dict = {
    'ROADNAME': 'RouteID',
    'BEG_MP': 'BeginPoint',
    'END_MP':'EndPoint',
    'RUT_MEAN':'RUTTING',
    'FAULT_AVG':'FAULTING',
    'PERCENT_CRACKING':'CRACKING_PERCENT',
    'IRI_MEAN': 'IRI',
    'SURF_TYPE': 'SURFACE_TYPE',
    'SHLD_TYPE': 'SHOULDER_TYPE',
    'DATE': 'ValueDate'
}

data_number = {
    'RUTTING': '50',
    'FAULTING': '51',
    'CRACKING_PERCENT': '52',
    'IRI': '47',
    'SURFACE_TYPE': '49',
    'SHOULDER_TYPE': '37'
}


data_items = ['RUTTING', 'FAULTING', 'CRACKING_PERCENT', 'IRI', 'SURFACE_TYPE', 'SHOULDER_TYPE']
master = pd.read_excel(r'C:\Users\e104200\Documents\PythonTest\Voltron\district_chrystal_report_website\hpms-validation\pavement_output_3_4_24\2023_COMBINED_ROUTES_DATA_ALL_3_4_24.xlsx', usecols=data_cols + route_cols)
master = master[master['SHLD_TYPE'].notna()]
master.rename(columns=rename_dict, inplace=True)
# master = master[master['RouteID'].str[2] == '1']

def convert_date(x):
    day,month,year = x.split('/')
    out = '/'.join([month,day,year])
    return out


def load_defaults(df):
    df['BeginDate'] = '01/01/2023'
    df['StateID'] = '54'
    df['Comments'] = ''
    df['ValueDate'] = '06/24/2023'
    return df


def sort_cols(df):
    df = df[['BeginDate', 'StateID', 'RouteID', 'BeginPoint', 'EndPoint', 'DataItem', 'ValueNumeric', 'ValueText', 'ValueDate', 'Comments']]
    return df


def create_data_item(df, data_item):
    df = df[['RouteID', 'BeginPoint', 'EndPoint', f'{data_item}', 'ValueDate']]
    df = load_defaults(df)
    df.rename(columns={f'{data_item}':'ValueNumeric'}, inplace=True)
    df = df[df['ValueNumeric'] != -1]
    df['DataItem'] = f'{data_item}'
    df['ValueText'] = ''
    df = sort_cols(df)
    return df


data_item_dict = {}
for i in data_items:
    data_item_dict[i] = create_data_item(master, i)


surf_dict = {'JCP': 3,'CRC':3, 'ASP': 6,'BRI':11,'OTH':11}
data_item_dict['SURFACE_TYPE']['ValueNumeric'] = data_item_dict['SURFACE_TYPE']['ValueNumeric'].map(lambda x: surf_dict[x])


def shoulder_mapper(x):
    if int(x) == 7:
        return 1
    else:
        return x
    



shld_dict = {'COMBO':5,'EARTH':6,'GRAVEL':4,'NONE':1,'NULL':1,'PAVED':2,'Curb':99,'CURB':99}
data_item_dict['SHOULDER_TYPE']['ValueNumeric'] = data_item_dict['SHOULDER_TYPE']['ValueNumeric'].map(lambda x : shld_dict[x])
# data_item_dict['SHOULDER_TYPE'] = data_item_dict['SHOULDER_TYPE'].loc[data_item_dict['SHOULDER_TYPE']['ValueNumeric'].astype('string') != '-1']


for k,v in data_item_dict.items():
    print(k, '\n', v, '\n\n\n')
    v.to_csv(f'pavement_output_3_4_24/DataItem{data_number[k]}_{k}.csv', index=False, sep='|')




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




import pandas as pd

data_cols = ['RUT_MEAN', 'Fault_Avg', 'Percent_Cracking', 'IRI_MEAN', 'SURF_TYPE', 'SHLD_TYPE']
route_cols = ['ROUTEID', 'BEG_MP', 'END_MP', 'Date']
rename_dict = {
    'ROUTEID': 'RouteID',
    'BEG_MP': 'BeginPoint',
    'END_MP':'EndPoint',
    'RUT_MEAN':'RUTTING',
    'Fault_Avg':'FAULTING',
    'Percent_Cracking':'CRACKING_PERCENT',
    'IRI_MEAN': 'IRI',
    'SURF_TYPE': 'SURFACE_TYPE',
    'SHLD_TYPE': 'SHOULDER_TYPE',
    'Date': 'ValueDate'
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
master = pd.read_excel('hpms_data_items/pavement/2023_submission_pavement_data.xlsx', usecols=data_cols + route_cols)
master.rename(columns=rename_dict, inplace=True)
master = master[master['RouteID'].str[2] == '1']

def convert_date(x):
    day,month,year = x.split('/')
    out = '/'.join([month,day,year])
    return out


def load_defaults(df):
    df['BeginDate'] = '01/01/2022'
    df['StateID'] = '54'
    df['Comments'] = ''
    df['ValueDate'] = df['ValueDate'].map(convert_date)
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


surf_dict = {'CON': 3, 'ASP': 6}
data_item_dict['SURFACE_TYPE']['ValueNumeric'] = data_item_dict['SURFACE_TYPE']['ValueNumeric'].map(lambda x: surf_dict[x])


def shoulder_mapper(x):
    if int(x) == 7:
        return 1
    else:
        return x

data_item_dict['SHOULDER_TYPE']['ValueNumeric'] = data_item_dict['SHOULDER_TYPE']['ValueNumeric'].map(shoulder_mapper)
# data_item_dict['SHOULDER_TYPE'] = data_item_dict['SHOULDER_TYPE'].loc[data_item_dict['SHOULDER_TYPE']['ValueNumeric'].astype('string') != '-1']


for k,v in data_item_dict.items():
    print(k, '\n', v, '\n\n\n')
    v.to_csv(f'hpms_data_items/pavement/interstate/DataItem{data_number[k]}_{k}.csv', index=False, sep='|')




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




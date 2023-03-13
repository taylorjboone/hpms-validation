import pandas as pd

data_cols = ['RUT_MEAN', 'Fault_Avg', 'Percent_Cracking', 'IRI_MEAN']
route_cols = ['ROUTEID', 'BEG_MP', 'END_MP', 'Date']
rename_dict = {
    'ROUTEID': 'RouteID',
    'BEG_MP': 'BeginPoint',
    'END_MP':'EndPoint',
    'RUT_MEAN':'RUTTING',
    'Fault_Avg':'FAULTING',
    'Percent_Cracking':'CRACKING_PERCENT',
    'IRI_MEAN': 'IRI',
    'Date': 'ValueDate'
}

data_number = {
    'RUTTING': '50',
    'FAULTING': '51',
    'CRACKING_PERCENT': '52',
    'IRI': '47'
}

data_items = ['RUTTING', 'FAULTING', 'CRACKING_PERCENT', 'IRI']
master = pd.read_excel('hpms_data_items/pavement/2023_submission_pavement_data.xlsx', usecols=data_cols + route_cols)
master.rename(columns=rename_dict, inplace=True)


def load_defaults(df):
    df['BeginDate'] = '01/01/2022'
    df['StateID'] = '54'
    df['Comments'] = ''
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

for k,v in data_item_dict.items():
    print(k, '\n', v, '\n\n\n')
    v.to_csv(f'pavement_data_items/DataItem{data_number[k]}_{k}.csv', index=False, sep='|')




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




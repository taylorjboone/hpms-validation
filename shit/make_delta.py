import pandas as pd
import os 
data_cols = ['RUT_MEAN', 'Fault_Avg', 'Percent_Cracking', 'IRI_MEAN', 'SURF_TYPE', 'SHLD_TYPE','Comments']
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

master.to_csv('a.csv',index=False)

os.system('lrsops overlay --operations op.json')

df = pd.read_excel('out.xlsx')
df = df.applymap(lambda x: pd.NA if x == -1 else x)
data_cols = ['RUT_MEAN', 'Percent_Cracking', 'IRI_MEAN', 'SURF_TYPE', 'SHLD_TYPE']
tmp = df[df.NHS_Value_Numeric.notna()&(df.FSYSTEM_Value_Numeric==1)&df[data_cols].isna().apply(lambda x:x.any(),axis=1)]



import pandas as pd
input_file = 'ril_7_6_2022.xlsx'
df = pd.read_excel(input_file, usecols=['RouteID', 'BMP', 'EMP', '65_STATE_FUNCTIONAL_CLASS','30_IRI_VALUE','21_FAULTING','14_CRACKING_PERCENT','52_RUTTING'])
print('Dataframe loaded')

def pm2_spatial_join(df):
    # checks for section length and creates section length if not found
    if not 'section_length' in df.columns.tolist():
        df['section_length'] = df['EMP'] - df['BMP']
    #initialize df['section_length_errors]
    df['section_length_errors'] = ''
    # itterates through section length column then returns false if the condition is met
    for data in df['section_length']:
        data = round(data,2)
        if data > 0.11:
            df['section_length_errors'] = 'More than 0.11'
        else:
            return df['section_length_errors']
    # for index,row in df.iterrows():
    #     print(row)
    #     if ((row['IRI_VALUE'] != row['FAULTING']) and (row['IRI_VALUE'] != row['CRACKING_PERCENT']) and (row['IRI_VALUE'] != row['RUTTING'])):
    #         print(row['RouteID'],row['BMP'],row['EMP'])
    errors = df.loc[((df['30_IRI_VALUE'] != df['21_FAULTING']) & (df['30_IRI_VALUE'] != df['14_CRACKING_PERCENT']) & (df['30_IRI_VALUE'] != df['52_RUTTING']))]
    return errors.to_excel('pm2_errors.xlsx',index=False)

# group = pm2_spatial_join(df)

        

            
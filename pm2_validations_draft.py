import pandas as pd
input_file = r'G:\.shortcut-targets-by-id\129JU9Dw85cayEM9V_oKEkrAogePup1hS\hpms_website_project\WV_DOT_HPMS_2022_June_Submission\ril_7_6_2022.xlsx'
df = pd.read_excel(input_file, usecols=['RouteID', 'BMP', 'EMP', '65_STATE_FUNCTIONAL_CLASS', '20_FACILITY', 'Label', '39_OWNERSHIP', '83_URBAN_CODE', '36_NHS', '115_STRAHNET', '17_DES_TRUCK_ROUTE'])
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
    for rid in df['routeid']:
        if rid == rid +1:
            
            
            
        
            
            
            
            print('meh')


        

            
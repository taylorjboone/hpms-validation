from operator import truediv
import pandas as pd
import geopandas as gpd

# arnold = gpd.read_file(r'G:\.shortcut-targets-by-id\129JU9Dw85cayEM9V_oKEkrAogePup1hS\hpms_website_project\WV_DOT_HPMS_2022_June_Submission\Dominant_Routes_12_31_2021')
# comb_df = pd.read_csv(r'G:\.shortcut-targets-by-id\129JU9Dw85cayEM9V_oKEkrAogePup1hS\hpms_website_project\WV_DOT_HPMS_2022_June_Submission\wv.csv')
input_file = r'G:\.shortcut-targets-by-id\129JU9Dw85cayEM9V_oKEkrAogePup1hS\hpms_website_project\WV_DOT_HPMS_2022_June_Submission\ril_7_6_2022.xlsx'
df = pd.read_excel(input_file, usecols=['RouteID', 'BMP', 'EMP', '65_STATE_FUNCTIONAL_CLASS', '20_FACILITY', 'Label', '39_OWNERSHIP', '83_URBAN_CODE', '36_NHS', '115_STRAHNET', '17_DES_TRUCK_ROUTE'])
print('Dataframe loaded')




def inventory_spatial_validation():
    # If RouteNumber column is missing, adds and populates RouteNumber pulled from RouteID
    if not 'RouteNumber' in df.columns.tolist():
        df['RouteNumber'] = df['RouteID'].str[3:7]
        df['RouteNumber'] = df['RouteNumber'].map(lambda x: x.lstrip('0'))
        print('Added RouteNumber column')

    # If RouteSigning column is missing, adds and populates RouteSigning pulled from RouteID
    if not 'RouteSigning' in df.columns.tolist():
        df['RouteSigning'] = df['RouteID'].str[2]
        print('Added RouteSigning column')

    # If RouteQualifier column is missing, adds and populates RouteSigning pulled from RouteID
    qualifier_dict = {'00':1,'01':2, '02':1, '03':5, '04':1, '05':1, '06':1, '07':1, '08':3, '09':3, '10':3, '11':3, '12':3, '13':9, '14':4, '15':6, '16':10, '17':10, '18':10, '19':10, '20':1, '21':10, '22':10, '23':10, '24':7, '25':10, '26':10, '27':10, '28':10, '51':10, '99':10}
    if not 'RouteQualifier' in df.columns.tolist():
        df['RouteQualifier'] = df['RouteID'].str[9:11]
        df['RouteQualifier'] = df['RouteQualifier'].map(lambda x: qualifier_dict[x])


    # Creates a column for each rule, outputs a False for each row that doesn't pass the rule for a column
    df['sji01'] = True
    df['sji01'].mask((df['65_STATE_FUNCTIONAL_CLASS'].isna()) | ((df['65_STATE_FUNCTIONAL_CLASS'].notna()) & (df['Label'].isna())), other=False, inplace=True)
    print('SJI01 complete')

    df['sji02'] = True
    df['sji02'].mask((df['20_FACILITY'].isna()) | ((df['20_FACILITY'].notna()) & (df['Label'].isna())), other=False, inplace=True)
    print('SJI02 complete')

    df['sji03'] = True
    df['sji03'].mask((df['39_OWNERSHIP'].isna()) | ((df['39_OWNERSHIP'].notna()) & (df['Label'].isna())), other=False, inplace=True)
    print('SJI03 complete')

    df['sji04'] = True
    df['sji04'].mask((df['83_URBAN_CODE'].isna()) | ((df['83_URBAN_CODE'].notna()) & (df['Label'].isna())), other=False, inplace=True)
    print('SJI04 complete')

    df['sji05'] = True
    df['sji05'].mask((df['20_FACILITY'].notna()) & (df['65_STATE_FUNCTIONAL_CLASS'].isna()), other=False, inplace=True)
    print('SJI05 complete')

    df['sji06'] = True
    df['sji06'].mask(((df['65_STATE_FUNCTIONAL_CLASS'].notna()) & (df['20_FACILITY'].isna())), other=False, inplace=True)
    print('SJI06 complete')

    df['sji07'] = True
    df['sji07'].mask((df['65_STATE_FUNCTIONAL_CLASS'] == 1) & (~df['20_FACILITY'].isin([1, 2])) | ((df['65_STATE_FUNCTIONAL_CLASS'] == 1) & (df['RouteNumber'].isna())), other=False, inplace=True)
    print('SJI07 complete')


    df['sji08'] = True
    df['sji08'].mask(((df['65_STATE_FUNCTIONAL_CLASS'] == 1) & (df['36_NHS'] != 1)), other=False, inplace=True)
    print('SJI08 complete')

    df['sji09'] = True
    df['sji09'].mask((df['RouteNumber'].notna()) & (df['RouteSigning'].isna()), other=False, inplace=True)

    df['sji10'] = True
    df['sji10'].mask((df['RouteNumber'].notna()) & (df['RouteQualifier'].isna()), other=False, inplace=True)

    df['sji11'] = True
    df['sji11'].mask((df['65_STATE_FUNCTIONAL_CLASS']==1) & ((df['115_STRAHNET']==1) | (df['115_STRAHNET']==2)), other=False, inplace=True)
    print('SJI11 complete')

    df['sji12'] = True
    df['sji12'].mask((df['36_NHS']==1) & ((df['115_STRAHNET']==1) | (df['115_STRAHNET']==2)), other=False, inplace=True)
    print('SJI12 complete')

    df['sji13'] = True
    df['sji13'].mask((df['65_STATE_FUNCTIONAL_CLASS']==1) & (df['17_DES_TRUCK_ROUTE']==1), other=False, inplace=True)
    print('SJI13 complete')


    # Prints the type and number of errors present in console
    errorlist = ['sji01', 'sji02', 'sji03', 'sji04', 'sji05', 'sji06', 'sji07', 'sji08', 'sji09', 'sji10', 'sji11', 'sji12', 'sji13']
    for error in errorlist:
        if False in df[error].unique().tolist():
            print(error + ': ' + '%d' % df[error].loc[df[error] == False].count())


    # Exports created error dataframe to excel file
    filename = r'C:\PythonTest\Voltron\hpms_website_project\errors.xlsx'
    df.to_excel(filename, index=False)
    print('Errors Dataframe exported to %s' % filename)

def pm2_spatial_join():
    #is section length column does not exist, create one
    if not 'section_length' in df.columns:
        df['section_length']=df['EMP']-df['BMP']

    df['pm2_section_length']=True
    df['pm2_section_length'].mask( (df['section_length'] < 0.11 ) | (df['section_length'].isna()) , other=False, inplace=True)
    print('pme2_section_length complete')

    df['iri_value_numeric_validation']=''
    if df['iri_value_numeric_validation'] < 95:
        df['iri_value_numeric_validation'] == 'good'
    elif ( (df['iri_value_numeric_validation'] < 170) & ( df['iri_value_numeric_validation'] > 95 ) ):
        df['iri_value_numeric_validation'] == 'fair'
    elif df['iri_value_numeric_validation'] > 170:
        df['iri_value_numeric_validation'] == 'poor'
    else:
        df['iri_value_numeric_validation']== 'No Data Found'

    df['rutting_value_numeric_validation'] = True
    df['rutting_value_numeric_validation'].mask( ( df['rutting_value_numeric_validation'] >= 0 ) & (df['rutting_value_numeric_validation'] < 1), other = False, inplace=True )
    
    df['faulting_value_numeric_validation'] = True
    df['faulting_value_numeric_validation'].mask( ( df['faulting_value_numeric_validation'] >= 0 ) & (df['faulting_value_numeric_validation'] < 1), other = False, inplace=True )
    
    df['cracking_percent_value_numeric_validation']=True
    df['cracking_percent_value_numeric_validation'].mask(df['cracking_percent_value_numeric_validation'] < 50 , other = False , inplace = True )
    
    df['iri_milepoint_comparison']=True
    df['iri_milepoint_comparison'].mask(df['iri_milepoint_comparison'], other = False , inplace = True) #use bennett code to do comparison between the bmp and emp of all pavement#

def traffic_spatial_join():
    # Creates a column for each rule, outputs a False for each row that doesn't pass the rule for a column
    df['sjt01']=True
    df['sjt01'].mask(df['aadt_single_unit'] < ( df['aadt'] * 0.4 ) , other = False , inplace = True )
    
    df['sjt02']=True
    df['sjt02'].mask((df['aadt'] > 500 ) & (df['aadt_single_unit'] > 0))
    
    df['sjt03']=True
    df['sjt03'].mask( ( (df['aadt_single_unit']) + (df['aadt_combination']) < (df['aadt'] * 0.8) ), other=False, Inplace=True )
    
    df['sjt04']=True
    df['sjt04'].mask( ( df['aadt_single_unit'] * 0.01 ) < ( df['aadt'] * ( df['pct_dh_single_unit'] / 100 ) ) < ( df['aadt_single_unit'] * 0.05 ) , other= False , inplace=True )
    
    df['sjt05']=True
    df['sjt05'].mask( ( df['pct_dh_single_unit'] > 0 ) & ( df['pct_dh_single_unit'] < 0.25 ) , other = False , inplace = True )
    
    df['sjt06']=True
    df['sjt06'].mask( (df['aadt_single_unit'] < 50 ) & (df['pct_dh_single_unit'] == 0 ) , other = False , inplace = True )
    
    df['sjt07']=True
    df['sjt07'].mask( ( df['aadt_combination'] > ( df['aadt'] * 0.4 ) ) , other = False , inplace = True )
    
    df['sjt08']=True
    df['sjt08'].mask( (df['addt'] > 500) & (df['aadt_combination'] > 0 ) , other = False , inplace = True )
    
    df['sjt09']=True
    df['sjt09'].mask( (df['aadt_combination'] * 0.01 ) < ( df['aadt'] * (df['pct_dh_combination'] / 100 ) ) < (df['aadt_combination'] * 0.5 ), other = False , inplace = True )
    
    df['sjt10']=True
    df['sjt10'].mask( ( df['pct_dh_combination'] > 0 ) & ( df['pct_dh_combination'] < 0.25 ), other = False , inplace = True  )
    
    df['sjt11']=True
    df['sjt11'].mask( (df['aadt_combination'] < 50) & (df['pct_dh_combination'] == 0) , other = False , inplace = True )
    
    df['sjt12']=True
    df['sjt12'].mask( ( df['k_factor'] > 4 ) & ( df['k_factor'] < 30 ) , other = False , inplace = True )
    
    df['sjt13']=True
    df['sjt13'].mask( ( df['dir_factor']==100 ) & ( df['facility_type'] == 2 ), other = False , inplace = True )
    
    df['sjt14']=True
    df['sjt14'].mask( ( df['dir_factor'] >= 50 ) & (df['dir_factor'] <= 75 ), other = False , inplace = True )
    
    df['sjt15']=True
    df['sjt15'].mask(  ( ( df['future_aadt'] > df['aadt'] ) & (df['future_aadt'] < (df['aadt'] * 4 ) ) & ( df['value_date'].isna() ) ) | ( df['future_aadt'] < (df['aadt'] * 0.2 ( df['value_date'] - df['bmp'] ) ) ), other = False , inplace = True )

def full_spatial_join():
    df['sjf01']=True
    df['sjf01'].mask(df['F_System'])
    
    
    
    
    
    
    
    
    
    
    
    
    
    print('meh')

# List of error messages to output in console
sji01_error = '''################################################################################################################################################################
Inventory Spatial Join Error SJ-I-01
F_SYSTEM ValueNumeric must not be NULL AND For every F_SYSTEM record, there should be a corresponding route on ARNOLD
################################################################################################################################################################'''
sji02_error = '''################################################################################################################################################################
Inventory Spatial Join Error SJ-I-02
FACILITY_TYPE ValueNumeric must not be null AND For every FACILITY_TYPE record, there should be a corresponding route on ARNOLD
################################################################################################################################################################'''
sji03_error = '''################################################################################################################################################################
Inventory Spatial Join Error SJ-I-03
OWNERHIP ValueNumeric must not be NULL AND For every OWNERSHIP record, there should be a corresponding route on ARNOLD
################################################################################################################################################################'''
sji04_error = '''################################################################################################################################################################
Inventory Spatial Join Error SJ-I-04
URBAN_ID ValueNumeric must not be NULL AND For every URBAN_ID record, there should be a corresponding route on ARNOLD
################################################################################################################################################################'''
sji05_error = '''################################################################################################################################################################
Inventory Spatial Join Error SJ-I-05
F_SYSTEM must exist where FACILITY_TYPE exists
################################################################################################################################################################'''
sji06_error = '''################################################################################################################################################################
Inventory Spatial Join Error SJ-I-06
FACILITY_TYPE must exist where F_SYSTEM exists
################################################################################################################################################################'''
sji07_error = '''################################################################################################################################################################
Inventory Spatial Join Error SJ-I-07
Where F_SYSTEM ValueNumeric = 1, FACILITY_TYPE ValueNumeric in (1,2) must exist and ROUTE_NUMBER ValueNumeric or ValueText must not be NULL
################################################################################################################################################################'''
sji08_error = '''################################################################################################################################################################
Inventory Spatial Join Error SJ-I-08
If F_SYSTEM ValueNumeric = 1 Then NHS must exist and NHS ValueNumeric must = 1
################################################################################################################################################################'''
sji09_error = '''################################################################################################################################################################
Inventory Spatial Join Error SJ-I-09
Where ROUTE_NUMBER ValueNumeric or ValueText is not NULL, ROUTE_SIGNING ValueNumeric must not be NULL
################################################################################################################################################################'''
sji10_error = '''################################################################################################################################################################
Inventory Spatial Join Error SJ-I-10
Where ROUTE_NUMBER ValueNumeric or ValueText is not NULL, ROUTE_QUALIFIER ValueNumeric must not be NULL
################################################################################################################################################################'''
sji11_error = '''################################################################################################################################################################
Inventory Spatial Join Error SJ-I-11
If F_SYSTEM ValueNumeric = 1 Then STRAHNET_TYPE must exist and STRAHNET_TYPE ValueNumeric must = 1
################################################################################################################################################################'''
sji12_error = '''################################################################################################################################################################
Inventory Spatial Join Error SJ-I-12
If STRAHNET_TYPE ValueNumeric in (1,2) then NHS ValueNumeric must = 1
################################################################################################################################################################'''
sji13_error = '''################################################################################################################################################################
Inventory Spatial Join Error SJ-I-13
If F_SYSTEM ValueNumeric = 1 Then NN must exist and NN ValueNumeric must = 1
################################################################################################################################################################'''

import pandas as pd
import os

# arnold = gpd.read_file(r'G:\.shortcut-targets-by-id\129JU9Dw85cayEM9V_oKEkrAogePup1hS\hpms_website_project\WV_DOT_HPMS_2022_June_Submission\Dominant_Routes_12_31_2021')
# comb_df = pd.read_csv(r'G:\.shortcut-targets-by-id\129JU9Dw85cayEM9V_oKEkrAogePup1hS\hpms_website_project\WV_DOT_HPMS_2022_June_Submission\wv.csv')
user_dir = os.path.expanduser('~')

# input_file = user_dir + r'/Downloads/ril_7_6_2022.xlsx'
# df = pd.read_excel(input_file, usecols=['RouteID', 'BMP', 'EMP', '65_STATE_FUNCTIONAL_CLASS', '20_FACILITY', 'Label', '39_OWNERSHIP', '83_URBAN_CODE', '36_NHS', '115_STRAHNET', '17_DES_TRUCK_ROUTE'])
input_file = user_dir + r'/Downloads/tmp/lrs_dump_12_2_2022.csv'
df = pd.read_csv(input_file, sep='|')

def get_f_system(value):
    if value in [1, 11]:
        return int(1)
    if value in [4, 12]:
        return int(2)
    if value in [2, 14]:
        return int(3)
    if value in [6, 16]:
        return int(4)
    if value in [7, 17]:
        return int(5)
    if value in [8, 18]:
        return int(6)
    if value in [9, 19]:
        return int(7)

def load_defaults():
    cols = df.columns.tolist()

    # If RouteNumber column is missing, adds and populates RouteNumber pulled from RouteID
    if not 'RouteNumber' in cols:
        df['RouteNumber'] = df['RouteID'].str[3:7]
        df['RouteNumber'] = df['RouteNumber'].map(lambda x: x.lstrip('0'))
        print('Added RouteNumber column')

    # If Sign System column is missing, adds and populates sign system pulled from RouteID
    if not 'RouteSigning' in cols:
        df['RouteSigning'] = df['RouteID'].str[2]
        print('Added RouteSigning column')

    if not '65_STATE_FUNCTIONAL_CLASS' in cols:
        df['65_STATE_FUNCTIONAL_CLASS'] = df['35_NAT_FUNCTIONAL_CLASS'].map(lambda x: get_f_system(x))
        print('Added State Functional Class column')

    

    # If RouteQualifier column is missing, adds and populates RouteQualifier pulled from RouteID
    qualifier_dict = {'00':1,'01':2, '02':1, '03':5, '04':1, '05':1, '06':1, '07':1, '08':3, '09':3, '10':3, '11':3, '12':3, '13':9, '14':4, '15':6, '16':10, '17':10, '18':10, '19':10, '20':1, '21':10, '22':10, '23':10, '24':7, '25':10, '26':10, '27':10, '28':10, '51':10, '99':10}
    if not 'RouteQualifier' in cols:
        df['RouteQualifier'] = df['RouteID'].str[9:11]
        df['RouteQualifier'] = df['RouteQualifier'].map(lambda x: qualifier_dict[x])




# Temporary Arnold dataframe until the real one is added
arnold = pd.DataFrame()
arnold['RouteID'] = df['RouteID']


def inventory_spatial_join():
    load_defaults()

    # Creates a column for each rule, outputs a False for each row that doesn't pass the rule for a column
    spatial_join_checks = {
        'sji01': ((df['65_STATE_FUNCTIONAL_CLASS'].notna()) & (df['RouteID'].isin(arnold['RouteID']))),
        'sji02': ((df['20_FACILITY'].notna()) & (df['RouteID'].isin(arnold['RouteID']))),
        'sji03': ((df['39_OWNERSHIP'].notna()) & (df['RouteID'].isin(arnold['RouteID']))),
        'sji04': ((df['83_URBAN_CODE'].notna()) & (df['RouteID'].isin(arnold['RouteID']))),
        'sji05': (((df['20_FACILITY'].notna()) & (df['65_STATE_FUNCTIONAL_CLASS'].notna())) | df['20_FACILITY'].isna()),
        'sji06': (((df['65_STATE_FUNCTIONAL_CLASS'].notna()) & (df['20_FACILITY'].notna())) | df['65_STATE_FUNCTIONAL_CLASS'].isna()),
        'sji07': (((df['65_STATE_FUNCTIONAL_CLASS'] == 1) & (df['20_FACILITY'].isin([1,2])) & (df['RouteNumber'].notna())) | (df['65_STATE_FUNCTIONAL_CLASS'] != 1)),
        'sji08': (((df['65_STATE_FUNCTIONAL_CLASS'] == 1) & (df['36_NHS'] == 1)) | (df['65_STATE_FUNCTIONAL_CLASS'] != 1)),
        'sji09': (((df['RouteNumber'].notna()) & (df['RouteSigning'].notna())) | (df['RouteNumber'].isna())),
        'sji10': (((df['RouteNumber'].notna()) & (df['RouteQualifier'].notna())) | (df['RouteNumber'].isna())),
        'sji11': (((df['65_STATE_FUNCTIONAL_CLASS'] == 1) & (df['115_STRAHNET'] == 1)) | (df['65_STATE_FUNCTIONAL_CLASS'] != 1)),
        'sji12': (((df['115_STRAHNET'].isin([1,2])) & (df['36_NHS'] == 1)) | (~df['115_STRAHNET'].isin([1,2]))),
        'sji13': (((df['65_STATE_FUNCTIONAL_CLASS'] == 1) & (df['17_DES_TRUCK_ROUTE'] == 1)) | (df['65_STATE_FUNCTIONAL_CLASS'] != 1))
    }

    tmp = df.copy()
    for k,v in spatial_join_checks.items():
        tmp[k] = v
    tmp2 = pd.DataFrame()
    for rule in spatial_join_checks.keys():
        tmp2 = tmp2.append(tmp[tmp[rule] == False])
    return tmp2.drop_duplicates()


def traffic_spatial_join():
    load_defaults()

    spatial_join_checks = {
        'sjt01': ((df['77_AADT_SINGLE'].isna()) | (df['77_AADT_SINGLE'] > (0.4 * df['2_AADT']))),
        'sjt02': ((df['77_AADT_SINGLE'].isna()) | (((df['2_AADT'] > 500) & (df['77_AADT_SINGLE'] > 0)) | (df['2_AADT'] <= 500))),
        'sjt03': ((df['77_AADT_SINGLE'].isna()) | ((df['77_AADT_SINGLE'] + df['77_AADT_COMBINATION']) < (0.8 * df['2_AADT']))),
        'sjt04': ((df['77_AADT_SINGLE'].isna()) | (((df['77_AADT_SINGLE'] * 0.01) < (df['2_AADT'] * (df['77_PCT_PEAK_SINGLE'] * .01))) & ((df['2_AADT'] * (df['77_PCT_PEAK_SINGLE'] * .01)) < (df['77_AADT_SINGLE'] * 0.5)))),
        'sjt05': ((df['77_PCT_PEAK_SINGLE'].isna()) | ((df['77_PCT_PEAK_SINGLE'] > 0) & (df['77_PCT_PEAK_SINGLE'] < 25))),
        'sjt06': ((df['77_PCT_PEAK_SINGLE'].isna()) | (((df['77_AADT_SINGLE'] < 50) & (df['77_PCT_PEAK_SINGLE'] == 0)) | (df['77_PCT_PEAK_SINGLE'] != 0)))
    }

    tmp = df.copy()
    for k,v in spatial_join_checks.items():
        tmp[k] = v
    tmp2 = pd.DataFrame()
    for rule in spatial_join_checks.keys():
        tmp2 = tmp2.append(tmp[tmp[rule] == False])
    return tmp2.drop_duplicates()



print(inventory_spatial_join())
print(traffic_spatial_join())





    # df['sji01'] = True
    # df['sji01'].loc[(df['65_STATE_FUNCTIONAL_CLASS'].isna()) | (df['65_STATE_FUNCTIONAL_CLASS'].notna() & df['Label'].isna())] = False

    # df['sji02'] = True
    # df['sji02'].loc[(df['65_STATE_FUNCTIONAL_CLASS'].isna() | (df['20_FACILITY'].notna() & df['Label'].isna()))] = False

    # df['sji03'] = True
    # df['sji03'].loc[(df['39_OWNERSHIP'].isna() | (df['39_OWNERSHIP'].notna() & df['Label'].isna()))]

    # df['sji04'] = True
    # df['sji04'].loc[(df['83_URBAN_CODE'].isna() | (df['83_URBAN_CODE'].notna() & df['Label'].isna()))]

    # df['sji05'] = True
    # df['sji05'].loc[(df['20_FACILITY'].notna() | df['65_STATE_FUNCTIONAL_CLASS'].isna())]

    # df['sji06'] = True
    # df['sji06'].loc[(df['65_STATE_FUNCTIONAL_CLASS'].notna() | (df['20_FACILITY'].isna()))]


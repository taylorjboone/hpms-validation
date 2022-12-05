import pandas as pd

# df = pd.read_csv(input_file, usecols=['RouteID', 'BMP', 'EMP', '65_STATE_FUNCTIONAL_CLASS','30_IRI_VALUE','21_FAULTING','14_CRACKING_PERCENT','52_RUTTING','83_URBAN_CODE','20_FACILITY'])
df = pd.read_csv('lrs_dump_12_5_2022.csv',sep='|')
df['sup_code'] = df['RouteID'].str.slice(9,11)
df['sign_system'] = df['RouteID'].str.slice(2,3)
df['section_length'] = df['EMP'] - df['BMP']
print('Dataframe loaded')
facility_list = [1,2,4,5,6]
facility_list2 = [1,2,4]
f_system_list = [1,2,3,4,5,6,7]

'lrs_dump_12_2_2022.csv'
    
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


# def produce_dir_through_lanes():
#     if len(df['RouteID']) == 13:
#         if df['section_length'] > 0:
#             if int(df['sup_code'] < 24):
#                 if df['70_SURFACE_TYPE'] !=1:
#                     if (df['97_ORIG_SURVEY_DIRECTION'].notna()) & (df['97_ORIG_SURVEY_DIRECTION'] == 0):
#                         if df['sign_system'] == 1:
#                             df['Dir_Through_Lanes'] = df['43_PEAK_LANES']
#                             return df['Dir_Through_Lanes']
#                         elif (df['97_ORIG_SURVEY_DIRECTION'].notna()) & (df['97_ORIG_SURVEY_DIRECTION'].isin(['1','A'])):
#                             df['Dir_Through_Lanes'] = df['43_COUNTER_PEAK_LANES']
#                             return df['Dir_Through_Lanes']





def load_defaults():
    cols = df.columns.tolist()
    print(cols)

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

    if not 'Dir_Through_lanes' in cols:
         if len(df['RouteID']) == 13:
            if df['section_length'] > 0:
                if int(df['sup_code']) < 24:
                    if df['70_SURFACE_TYPE'] !=1:
                        if df['sign_system'] == 1:
                            if (df['97_ORIG_SURVEY_DIRECTION'].notna()) & (df['97_ORIG_SURVEY_DIRECTION'] == 0):
                                    df['Dir_Through_Lanes'] = df['43_PEAK_LANES']
                                    return df['Dir_Through_Lanes']
                            elif (df['97_ORIG_SURVEY_DIRECTION'].notna()) & (df['97_ORIG_SURVEY_DIRECTION'].isin(['1','A'])):
                                    df['Dir_Through_Lanes'] = df['43_COUNTER_PEAK_LANES']
                                    return df['Dir_Through_Lanes']
            print('add Dir Through Lanes')

    
    
    
    
#     print('meh')

def full_spatial_join():
    load_defaults()
    error_dict = {
        'sjf01':((df['65_STATE_FUNCTIONAL_CLASS'].notna()) & (df['20_FACILITY'].isin(facility_list))),
        'sjf02':(((df['83_URBAN_CODE'].notna()) & (df['20_FACILITY'].isin(facility_list2)) & (df['65_STATE_FUNCTIONAL_CLASS'].isin(f_system_list))) | ((df[df['20_FACILITY'] == 6]) & (df['Dir_Through_Lanes'] > 0) & (df[df['65_STATE_FUNCTIONAL_CLASS'] == 1]) & (df['30_IRI_VALUE'].notna()))),
        'sjf03':((df['20_FACILITY'].notna()) & (df['65_STATE_FUNCTIONAL_CLASS'].isin(f_system_list))),
        'sjf04':'no worries',
        'sjf05':((df['65_STATE_FUNCTIONAL_CLASS'].isin([1,2,3])) & (df['20_FACILITY'].isin([1,2]))),
        'sjf06':((df['65_STATE_FUNCTIONAL_CLASS'].isin([1,2,3,4,5,6,7])) & (df['20_FACILITY'].isin([1,2,5,6]))),
        'sjf07':((df['20_FACILITY'].isin([1,2,4])) & (df['65_STATE_FUNCTIONAL_CLASS'].isin([1,2,3,4,5]))),
        'sjf08':'Do not have managed lanes',
        'sjf09':'Do not have managed lane type',
        'sjf10':(df['is_sample'].notna()),
        'sjf11':((df['is_sample'].notna()) & (df['20_FACILITY'] == 2) & ((df['83_URBAN_CODE'] < 99999) | (df['through_lanes'] >=4))),
        'sjf12':((df['is_sample'].notna())& (df['83_URBAN_CODE'] < 99999) & (df['access_control'] > 1)),
        'sjf13':((df['is_sample'].notna()) & (df['83_URBAN_CODE'] < 99999) & (df['access_control'])),
        'sjf14':((df['is_sample'].notna()) & (df['nhs'].notna())),
        'sjf15':'No Rule',
        'sjf16':(),
        'sjf17':((df['65_STATE_FUNCTIONAL_CLASS'].isin([1,2,3,4])|()))


    }
    tmp = df.copy()
    for k,v in error_dict.items():
        tmp[k] = v
    tmp2 = pd.DataFrame()
    for rule in error_dict.keys():
        tmp2 = tmp2.append(tmp[tmp[rule] == False])
    return tmp2.drop_duplicates()
    
print(full_spatial_join())

# group = full_spatial_join(df)
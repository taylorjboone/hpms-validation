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
columm_list_master = ['RouteID','BeginPoint','EndPoint','F_SYSTEM','NHS','STRAHNET','NN','NHFN','ROUTE_NUMBER','URBAN_ID','FACILITY_TYPE','STRUCTURE_TYPE',
'OWNERSHIP','COUNTY_ID','MAINTENANCE_OPERATIONS','IS_RESTRICTED','THROUGH_LANES','MANAGED_LANES_TYPE','MANAGED_LANES','PEAK_LANES',
'COUNTER_PEAK_LANES','TOLL_ID','LANE_WIDTH','MEDIAN_TYPE','MEDIAN_WIDTH','SHOULDER_TYPE',
'SHOULDER_WIDTH_R','SHOULDER_WIDTH_L','DIR_THROUGH_LANES','TURN_LANES_R','TURN_LANES_L',
'SIGNAL_TYPE','PCT_GREEN_TIME','NUMBER_SIGNALS','STOP_SIGNS','AT_GRADE_OTHER','AADT',
'AADT_SINGLE_UNIT','PCT_DH_SINGLE_UNIT','AADT_COMBINATION','PCT_DH_COMBINATION',
'K_FACTOR','DIR_FACTOR','FUTURE_AADT','ACCESS_CONTROL','SPEED_LIMIT','IRI','PSR',
'SURFACE_TYPE','RUTTING','FAULTING','CRACKING_PERCENT','YEAR_LAST_IMPROVEMENT',
'YEAR_LAST_CONSTRUCTION','LAST_OVERLAY_THICKNESS','THICKNESS_RIGID','THICKNESS_FLEXIBLE',
'BASE_TYPE','BASE_THICKNESS','SOIL_TYPE','WIDENING_POTENTIAL','CURVE_CLASSIFICATION',
'TERRAIN_TYPE','GRADE_CLASSIFICATION','PCT_PASS_SIGHT','TRAVEL_TIME_CODE']

convert_dict={
'BMP':'BeginPoint',
'EMP':'EndPoint',
'12_COUNTY':'COUNTY_ID',
'2_AADT':'AADT',
'2_FUTURE_AADT':'FUTURE_AADT',
'3_ACCESS_CONTROL':'ACCESS_CONTROL',
'4_ALT_ROUTE_NAME':'ALT_ROUTE_NAME',
'5_AT_GRADE_OTHER':'AT_GRADE_OTHER',
'6_AVE_LANE_WIDTH_FT':'LANE_WIDTH',
'14_CRACKING_PERCENT':'CRACKING_PERCENT',
'16_CURVE_CLASS':'CURVE_CLASSIFICATION',
'17_DES_TRUCK_ROUTE':'NN',
'20_FACILITY':'FACILITY_TYPE','21_FAULTING':'FAULTING',
'25_GRADE_CLASS':'GRADE_CLASSIFICATION',
'30_IRI_VALUE':'IRI',
'34_MEDIAN_WIDTH_FT':'MEDIAN_WIDTH',
'36_NHS':'NHS',
'37_NUMBER_SIGNALS':'NUMBER_SIGNALS',
'39_OWNERSHIP':'OWNERSHIP',
'41_PCT_GREEN_TIME':'PCT_GREEN_TIME',
'42_PCT_PASS_SIGHT':'PCT_PASS_SIGHT',
'43_PEAK_LANES':'PEAK_LANES',
'43_COUNTER_PEAK_LANES':'COUNTER_PEAK_LANES',
'45_PSR':'PSR',
'52_RUTTING':'RUTTING',
'56_SHOULDER_TYPE_RT':'SHOULDER_TYPE',
'57_SHOULDER_WIDTH_LFT_FT':'SHOULDER_WIDTH_L',
'58_SHOULDER_WIDTH_RT_FT':"SHOULDER_WIDTH_R",
'59_SIGNAL_TYPE':'SIGNAL_TYPE',
'63_SPEED_LIMIT_MPH':'SPEED_LIMIT',
'66_STOP_SIGNS':'STOP_SIGNS',
'70_SURFACE_TYPE':'SURFACE_TYPE',
'71_TERRAIN_TYPE':'TERRAIN_TYPE',
'74_NUM_THROUGH_LANES':'DIR_THROUGH_LANES_MAYBE',
'75_TOLL_CHARGED':'TOLL_ID',
'76_TOLL_CHARGED':'TOLL_ID',
'77_AADT_SINGLE':'AADT_SINGLE_UNIT',
'77_AADT_COMBINATION':'AADT_COMBINATION',
'77_PCT_PEAK_SINGLE':'PCT_DH_SINGLE_UNIT',
'77_PCT_PEAK_COMBINATION':'PCT_DH_COMBINATION',
'77_K_FACTOR':"K_FACTOR",
'77_DIR_FACTOR':'DIR_FACTOR',
'80_TURN_LANES_LFT':'TURN_LANES_L',
'81_TURN_LANES_R':'TURN_LANES_R',
'83_URBAN_CODE':'URBAN_ID',
'84_WIDENING_OBSTACLE':'WIDENING_POTENTIAL',
'85_WIDENING_POTENTIAL':'WIDENING_POTENTIAL',
'115_STRAHNET':'STRAHNET'
}

def check_and_rename_columns():
    column_list_unchecked = df.columns.tolist()
    for a in column_list_unchecked:
        if a in columm_list_master:
            print(a)
        else:
            print("The Column does not exist",a)













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
    check_and_rename_columns()
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
    #renames columns for use for HPMS
    df.rename(columns=convert_dict)

    
    
    
    
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
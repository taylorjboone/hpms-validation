import pandas as pd
import numpy as np
from pandarallel import pandarallel
from datetime import date,timedelta
from dateutil.relativedelta import relativedelta
import random
import numpy as np

pandarallel.initialize()

# df = pd.read_csv(input_file, usecols=['RouteID', 'BMP', 'EMP', '65_STATE_FUNCTIONAL_CLASS','30_IRI_VALUE','21_FAULTING','14_CRACKING_PERCENT','52_RUTTING','83_URBAN_CODE','20_FACILITY'])
df = pd.read_csv('test_data.csv',sep='|')
df['RouteID']=df['RouteID'].astype(str)
df['sup_code'] = df['RouteID'].str.slice(9,11)
df['BeginDate'] = date.fromisoformat('2022-12-31')
df['sign_system'] = df['RouteID'].str.slice(2,3)
df['section_length'] = df['EMP'] - df['BMP']
df['ValueDate'] =date.fromisoformat('2022-10-31')
df['ValueText'] ='A'
df['TRAVEL_TIME_CODE'] = 'asdfasdfasf'
df['YEAR_LAST_CONSTRUCTION'] =date.fromisoformat('2010-12-31') 
df['THICKNESS_RIGID']=np.random.randint(1, 20, df.shape[0])
df['THICKNESS_FLEXIBLE']=np.random.randint(1, 20, df.shape[0])
df['MAINTENANCE_OPERATIONS']=np.random.randint(1, 12, df.shape[0])
print('Dataframe loaded')
facility_list = [1,2,4,5,6]
facility_list2 = [1,2,4]
f_system_list = [1,2,3,4,5,6,7]
urban_id_list=['06139',15481,21745,36190,40753,59275,67672,93592,94726]
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
'6_AVG_LANE_WIDTH_FT':'LANE_WIDTH',
'8_BASE_TYPE':'BASE_TYPE',
'14_CRACKING_PERCENT':'CRACKING_PERCENT',
'16_CURVE_CLASS':'CURVE_CLASSIFICATION',
'17_DES_TRUCK_ROUTE':'NN',
'20_FACILITY':'FACILITY_TYPE',
'21_FAULTING':'FAULTING',
'25_GRADE_CLASS':'GRADE_CLASSIFICATION',
'29_HPMS_SAMPLE_NO':'is_sample',
'30_IRI_VALUE':'IRI',
'34_MEDIAN_WIDTH_FT':'MEDIAN_WIDTH',
'34_HPMS_MEDIAN_BARRIER_TYPE':'MEDIAN_TYPE',
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
'65_STATE_FUNCTIONAL_CLASS':'F_SYSTEM',
'66_STOP_SIGNS':'STOP_SIGNS',
'70_SURFACE_TYPE':'SURFACE_TYPE',
'71_TERRAIN_TYPE':'TERRAIN_TYPE',
'74_NUM_THROUGH_LANES':'THROUGH_LANES',
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
'88_YEAR_LAST_IMPROV':'YEAR_LAST_IMPROVEMENT',
'115_STRAHNET':'STRAHNET'
}

# def check_and_rename_columns():
#     column_list_unchecked = df.columns.tolist()
#     for a in column_list_unchecked:
#         if a in columm_list_master:
#             print(a)
#         else:
#             print("The Column does not exist",a)


def check_rule_sjf43(df):
    sum_curve = df.loc[( df['CURVE_CLASSIFICATION'].notna()),'section_length'].sum()
    sum_sample = df.loc[ (((df['is_sample'].notna())&( df['F_SYSTEM'].isin([1,2,3]) )) | ( (df['F_SYSTEM']==4) & (df['URBAN_ID']==99999) ) ),'section_length'].sum()
    if sum_curve != sum_sample:
        print('Sums are not equal, please review')
        return False
    else:
        print('Sums are equal')
        return True

def check_rule_sjf47(df):
    sum_grade = df.loc[( df['GRADE_CLASSIFICATION'].notna()),'section_length'].sum()
    sum_sample = df.loc[ (((df['is_sample'].notna())&( df['F_SYSTEM'].isin([1,2,3]) )) | ( (df['F_SYSTEM']==4) & (df['URBAN_ID']==99999) ) ),'section_length'].sum()
    if sum_grade != sum_sample:
        print('Sums are not equal, please review')
        return False
    else:
        print('Sums are equal')
        return True













# def get_f_system(value):
#     if value in [1, 11]:
#         return int(1)
#     if value in [4, 12]:
#         return int(2)
#     if value in [2, 14]:
#         return int(3)
#     if value in [6, 16]:
#         return int(4)
#     if value in [7, 17]:
#         return int(5)
#     if value in [8, 18]:
#         return int(6)
#     if value in [9, 19]:
#         return int(7)

f_system_dict = {
    1:1,
    11:1,
    4:2,
    12:2,
    2:3,
    14:3,
    6:4,
    16:4,
    7:5,
    17:5,
    8:6,
    18:6,
    9:7,
    19:7
}


def produce_dir_through_lanes(x):
    boolval = (len(x['RouteID']) == 13) and (x.section_length > 0) and (int(x.sup_code) < 24) and \
      (x['70_SURFACE_TYPE'] !=1) and  not (x['97_ORIG_SURVEY_DIRECTION']==np.isnan) and \
        (x.sign_system=='1')
    if boolval and (x['97_ORIG_SURVEY_DIRECTION'] == '0'):
        x['Dir_Through_Lanes']=x['43_PEAK_LANES']
    elif boolval and (x['97_ORIG_SURVEY_DIRECTION'] in ['1','A']):
        x['Dir_Through_Lanes']=x['43_COUNTER_PEAK_LANES']
    else:
        # print('NAN')
        pass
    return x



    # if (len(x.RouteID) == 13) and (x.section_length > 0) and (int(x.sup_code) < 24) and \
    #  (x['70_SURFACE_TYPE'] !=1) and  not (x['97_ORIG_SURVEY_DIRECTION']==np.isnan) and \
    #     (x['97_ORIG_SURVEY_DIRECTION'] == 0) and (x.sign_system==1):
    #     x['Dir_Through_lanes'] = x['43_Peak_Lanes']
    # elif(len(x.RouteID) == 13) and (x.section_length > 0) and (int(x.sup_code) < 24) and \
    #  (x['70_SURFACE_TYPE'] !=1) and  not (x['97_ORIG_SURVEY_DIRECTION']==np.isnan) and \
    #     (x['97_ORIG_SURVEY_DIRECTION'] == 0) and ( (x['97_ORIG_SURVEY_DIRECTION'] == 1) or \
    #         (x['97_ORIG_SURVEY_DIRECTION']=='A')):
    #     x['Dir_Through_Lanes'] = x['43_COUNTER_PEAK_LANES']
    # else:
    #     x['Dir_Through_Lanes'] =np.nan
    # print(x['Dir_Through_Lanes'])
    # return x

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





def load_defaults(df):
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
        df['65_STATE_FUNCTIONAL_CLASS'] = df['35_NAT_FUNCTIONAL_CLASS'].map(f_system_dict)
        print('Added State Functional Class column')

    if not 'Dir_Through_lanes' in cols:
        df=df.apply(produce_dir_through_lanes,axis=1)
        # print(df['Dir_Through_Lanes'])

    return df 



    
    
    
    
#     print('meh')

def full_spatial_join(df):
    df = load_defaults(df)
    df = df.rename(columns=convert_dict)
    print(df['LANE_WIDTH'])
    error_dict = {
        'sjf01':((df['F_SYSTEM'].notna()) & (df['FACILITY_TYPE'].isin(facility_list))),
        'sjf02':(((df['URBAN_ID'].notna()) & (df['FACILITY_TYPE'].isin(facility_list2)) & \
        (df['F_SYSTEM'].isin(f_system_list))) | \
        ((df['FACILITY_TYPE'] == 6) & \
        (df['Dir_Through_Lanes'] > 0) & \
        (df['F_SYSTEM'] == 1) & \
        (df['IRI'].notna()))),
        'sjf03':((df['FACILITY_TYPE'].notna()) & (df['F_SYSTEM'].isin(f_system_list))),
        'sjf04':True,
        'sjf05':((df['F_SYSTEM'].isin([1,2,3])) & (df['FACILITY_TYPE'].isin([1,2]))),
        'sjf06':((df['F_SYSTEM'].isin([1,2,3,4,5,6,7])) & (df['FACILITY_TYPE'].isin([1,2,5,6]))),
        'sjf07':((df['FACILITY_TYPE'].isin([1,2,4])) & (df['F_SYSTEM'].isin([1,2,3,4,5]))),
        'sjf08':True,
        'sjf09':True,
        'sjf10':(df['is_sample'].notna()),
        'sjf11':((df['is_sample'].notna()) & (df['FACILITY_TYPE'] == 2) & ((df['URBAN_ID'] < 99999) | (df['THROUGH_LANES'] >=4))),
        'sjf12':((df['is_sample'].notna())& (df['URBAN_ID'] < 99999) & (df['ACCESS_CONTROL'] > 1)),
        'sjf13':((df['is_sample'].notna()) & (df['URBAN_ID'] < 99999) & (df['ACCESS_CONTROL'])),
        'sjf14':((df['is_sample'].notna()) & (df['NHS'].notna())),
        'sjf15':True,
        'sjf16':True,
        'sjf17':(((df['F_SYSTEM'].isin([1,2,3,4])) |(df['NHS'].notna())) & (df['FACILITY_TYPE'].isin([1,2]))),#check rule, needs to be also in NHS
        'sjf18':((df['F_SYSTEM'].isin([1,2,3,4])) & (df['FACILITY_TYPE'].isin([1,2]))),#check rule, needs to be also in NHS
        'sjf19':(((df['F_SYSTEM'].isin([1,2,3,4])) | (df['F_SYSTEM'].isin(df['NHS']))) & (df['FACILITY_TYPE'].isin([1,2]))),
        'sjf20':((df['FACILITY_TYPE'].isin([1,2,4])) & ((df['F_SYSTEM'].isin([1,2,3,4,5]))  |( (df['F_SYSTEM']==6) & (df['URBAN_ID'] < 99999) ) | (df['NHS'].notna()))),
        'sjf21':( ( ( ( df['F_SYSTEM'].isin([1]) ) | ( df['NHS'].notna() ) ) & ( df['FACILITY_TYPE'].isin([1,2]) ) ) | (df['is_sample'].notna())),
        'sjf22':(df['is_sample'].notna()),
        'sjf23':( ( ( ( df['F_SYSTEM'].isin([1]) ) | ( df['NHS'].notna() ) ) & ( df['FACILITY_TYPE'].isin([1,2]) ) ) | (df['is_sample'].notna())),
        'sjf24':(df['is_sample'].notna()),
        'sjf25':(df['is_sample'].notna()),
        'sjf26':(df['is_sample'].notna()),
        'sjf27':(df['is_sample'].notna()),
        'sjf28':((df['is_sample'].notna()) & (df['URBAN_ID']!=99999) & (df['NUMBER_SIGNALS'] >= 1)),
        'sjf29':((df['is_sample'].notna()) & (df['URBAN_ID'] < 99999) & (df['NUMBER_SIGNALS'] >=1)),
        'sjf30':((df['is_sample'].notna()) & (df['SIGNAL_TYPE'].isin([1,2,3,4]))),
        'sjf31':(df['is_sample'].notna()),
        'sjf32':(df['is_sample'].notna()),
        'sjf33':(df['is_sample'].notna()),
        'sjf34':(df['is_sample'].notna()),
        'sjf35':(( df['is_sample'].notna() ) & (df['MEDIAN_TYPE'].isin([2,3,4,5,6,7]))),
        'sjf36':(df['is_sample'].notna()),
        'sjf37':((df['is_sample'].notna()) & (df['SHOULDER_TYPE'].isin([2,3,4,5,6,7]))),
        'sjf38':((df['is_sample'].notna()) & (df['SHOULDER_TYPE'].isin([2,3,4,5,6])) & (df['MEDIAN_TYPE'].isin([2,3,4,5,6,7]))),
        'sjf39':((df['is_sample'].notna()) & (df['URBAN_ID'] < 99999)),
        'sjf40':(df['is_sample'].notna()),
        'sjf41':(   ( df['CURVE_CLASSIFICATION'].notna() ) &   ( df['is_sample'].notna() ) & ((df['F_SYSTEM'].isin([1,2,3,4]) )  | (df['F_SYSTEM']==4) ) & (df['URBAN_ID'] == 99999) & (df['SURFACE_TYPE'] > 1)) , #Double check this, logic seems weird
        'sjf42':(   ( df['CURVE_CLASSIFICATION'].notna() ) &   ( df['is_sample'].notna() ) & ((df['F_SYSTEM'].isin([1,2,3,4]) )  | (df['F_SYSTEM']==4) ) & (df['URBAN_ID'] == 99999) & (df['SURFACE_TYPE'] > 1)) ,
        'sjf43': check_rule_sjf43(df),
        'sjf44':((df['is_sample'].notna()) & (df['URBAN_ID']==99999) & (df['F_SYSTEM'].isin([1,2,3,4,5]))),
        'sjf45':(   ( df['GRADE_CLASSIFICATION'].notna() ) &   ( df['is_sample'].notna() ) & ((df['F_SYSTEM'].isin([1,2,3,4]) )  | (df['F_SYSTEM']==4) ) & (df['URBAN_ID'] == 99999) & (df['SURFACE_TYPE'] > 1)) ,#Double check, similiar to previous rules
        'sjf46':(   ( df['GRADE_CLASSIFICATION'].notna() ) &   ( df['is_sample'].notna() ) & ((df['F_SYSTEM'].isin([1,2,3,4]) )  | (df['F_SYSTEM']==4) ) & (df['URBAN_ID'] == 99999) & (df['SURFACE_TYPE'] > 1)) ,
        'sjf47':check_rule_sjf47(df),
        'sjf48':( ( df['is_sample'].notna() ) & (df['URBAN_ID']==99999) & (df['THROUGH_LANES']==2) & (df['MEDIAN_TYPE'].isin([1,2])) & (df['SURFACE_TYPE'] > 1)),
        'sjf49':(df['IRI'].isna()) | ((df['IRI'].notna()) & (df['SURFACE_TYPE']>1)&((df['FACILITY_TYPE'].isin([1,2]))&((df['PSR']!='A')&((df['F_SYSTEM'].isin([1,2,3])) | (df['NHS'].notna()))|((df['is_sample'].notna())&(df['F_SYSTEM']==4)&(df['URBAN_ID']==99999)))|(df['Dir_Through_Lanes']>0))),
        'sjf50':((df['PSR'].notna())&(df['IRI'].isna())&(df['FACILITY_TYPE'].isin([1,2]))&(df['SURFACE_TYPE'])),
        'sjf51':(df['SURFACE_TYPE'].isna())|((df['SURFACE_TYPE'].notna()) & (df['FACILITY_TYPE'].isin([1,2])) & ((df['F_SYSTEM']==1) | (df['NHS'].notna()) | (df['is_sample'].notna())) | ((df['Dir_Through_Lanes'] > 0) & ((df['IRI'].notna())|(df['PSR'].notna())))),
        'sjf52':(df['RUTTING'].isna())|((df['SURFACE_TYPE'].isin([2,6,7,8]))&((df['FACILITY_TYPE'].isin([1,2]))&((df['F_SYSTEM']==1)|(df['NHS'].notna())|(df['is_sample'].notna()))|(df['Dir_Through_Lanes']>0)&((df['IRI'].notna())|(df['PSR'].notna())))),
        'sjf53':(df['FAULTING'].isna())|((df['SURFACE_TYPE'].isin([3,4,9,10]))&((df['FACILITY_TYPE'].isin([1,2]))&((df['F_SYSTEM']==1)|(df['NHS'].notna())|(df['is_sample'].notna()))|(df['Dir_Through_Lanes']>0)&((df['IRI'].notna())|(df['PSR'].notna())))),
        'sjf54':(df['CRACKING_PERCENT'].isna())|((df['SURFACE_TYPE'].isin([2,3,4,5,6,7,8,9]))&((df['FACILITY_TYPE'].isin([1,2]))&((df['F_SYSTEM']==1)|(df['NHS'].notna())|(df['is_sample'].notna()))|(df['Dir_Through_Lanes']>0)&((df['IRI'].notna())|(df['PSR'].notna())))),
        'sjf55':(((df['SURFACE_TYPE'].isin([2,3,4,5,6,7,8,9,10])) & (df['is_sample'].notna()) )| ((df['YEAR_LAST_CONSTRUCTION'].notna()) & (df['YEAR_LAST_CONSTRUCTION'] < (df['BeginDate']-relativedelta(years=20)) ) ) ),
        'sjf56':((df['SURFACE_TYPE'].isin([2,3,4,5,6,7,8,9,10])) & (df['is_sample'].notna())),
        'sjf57':((df['is_sample'].notna()) & (df['YEAR_LAST_IMPROVEMENT'].notna())),
        'sjf58':((df['SURFACE_TYPE'].isin([3,4,5,6,7,8,9,10])) & (df['is_sample'].notna())),
        'sjf59':((df['SURFACE_TYPE'].isin([2,6,7,8])) & (df['is_sample'].notna())),
        'sjf60':((df['is_sample'].notna()) & (df['SURFACE_TYPE'] > 1)),
        'sjf61':((df['BASE_TYPE'] > 1) & (df['SURFACE_TYPE'] > 1) & (df['is_sample'].notna())),
        'sjf62':True,
        'sjf63':((df['FACILITY_TYPE'].isin([1,2]) ) & ( df['F_SYSTEM'].isin([1,2,3,4,5])  |((df['F_SYSTEM']==6) & (df['URBAN_ID'] < 99999))) | (df['NHS'].notna())),
        'sjf64':((df['F_SYSTEM']==1) & (df['FACILITY_TYPE'].isin([1,2,6]) ) ),
        'sjf65':True,
        'sjf66':True,
        'sjf67':True,
        'sjf68':((df['F_SYSTEM']==1) & (df['FACILITY_TYPE']==6) & ((df['IRI'].notna()) | (df['PSR'].notna() ) ) ),
        'sjf69':((df['THROUGH_LANES'] > 1) & (df['FACILITY_TYPE']==2)),
        'sjf70':(df['COUNTER_PEAK_LANES'] + df['PEAK_LANES'] >= df['THROUGH_LANES']),
        'sjf71':((df['FACILITY_TYPE'] == 1)&(df['COUNTER_PEAK_LANES'].isna())),
        'sjf72':((df['SPEED_LIMIT']%5==0) & ((df['SPEED_LIMIT']<90) | (df['SPEED_LIMIT']==999))),
        'sjf73':(df['SIGNAL_TYPE'].isna())|((df['F_SYSTEM']==1) & (df['URBAN_ID']!=99999) & (df['SIGNAL_TYPE']==5)),
        'sjf74':((5 < df['LANE_WIDTH'])& (df['LANE_WIDTH']< 19)),
        'sjf75':((df['MEDIAN_TYPE'].isin([2,3,4,5,6])) & (df['MEDIAN_WIDTH'] > 0)),
        'sjf76':(df['MEDIAN_WIDTH'].isna())|((df['FACILITY_TYPE'].isin([1,4]))|(df['MEDIAN_TYPE']==1)),
        'sjf77':(df['SHOULDER_WIDTH_L'] < df['MEDIAN_WIDTH']),
        # Can only be tested when used with pavement file
        'sjf78':(df['IRI'].isna())|(((df['ValueDate'].notna()) & (df['BeginDate'].notna())) & ((df['is_sample'].isna())|(df['ValueText'].isna())) & (df['F_SYSTEM'] > 1) & (df['NHS'].isin([1,2,3,4,5,6,7,8,9]))),
        'sjf79':(df['RUTTING'].isna())|(((df['ValueDate'].notna()) & (df['BeginDate'].notna())) & ((df['is_sample'].isna())|(df['ValueText'].isna())) & (df['F_SYSTEM'] > 1) & (df['NHS'].isin([1,2,3,4,5,6,7,8,9]))),
        'sjf80':(df['FAULTING'].isna())|(((df['ValueDate'].notna()) & (df['BeginDate'].notna())) & ((df['is_sample'].isna())|(df['ValueText'].isna())) & (df['F_SYSTEM'] > 1) & (df['NHS'].isin([1,2,3,4,5,6,7,8,9]))), 
        'sjf81':(df['CRACKING_PERCENT'].isna())|(((df['ValueDate']) >= (df['BeginDate']-timedelta(days=365))) & ((df['is_sample'].isna())|(df['ValueText'].isna())) & (df['F_SYSTEM'] > 1) & (df['NHS'].isin([1,2,3,4,5,6,7,8,9]))),
        'sjf82':(df['IRI'].isna())|(df['RUTTING'].isna())|(df['FAULTING'].isna())|(df['CRACKING_PERCENT'].isna())|((df['ValueDate'] == df['BeginDate']) & (df['ValueText'].isna()) & (df['F_SYSTEM']==1)),
        'sjf83':(df['IRI'].isna())|(df['RUTTING'].isna())|(df['FAULTING'].isna())|((df['ValueText'].isin(['A','B','C','D','E'])) & ((df['ValueDate']!=df['BeginDate']) & (df['F_SYSTEM']==1)| (((df['ValueDate'])<(df['BeginDate'] - relativedelta(years=1))) & (df['NHS'].notna())))),
        'sjf84':(df['PSR'].isna())|(df['IRI'].isna())|(df['RUTTING'].isna())|(df['FAULTING'].isna())|(((df['ValueDate'])>=(df['BeginDate']-relativedelta(years=1))) & ((df['is_sample'].notna())|(df['F_SYSTEM']>1)) & (df['NHS'].isin([1,2,3,4,5,6,7,8,9]))),
        'sjf85':(df['PSR'].isna())|(( (df['ValueDate']) == (df['BeginDate']) ) & (df['ValueText'].notna()=='A') & (df['F_SYSTEM'].notna()==1)),
        'sjf86':( ( (df['F_SYSTEM']==1) & (df['IRI'].isna()) ) & ( (df['PSR']>0) & (df['ValueText']=='A') )),
        'sjf87':(df['RUTTING'] < 1),
        'sjf88':(df['FAULTING'] <=1),
        'sjf89':((df['SURFACE_TYPE'].isin([2,6,7,8]))), #Not sure how to write this  
        'sjf90':((df['SURFACE_TYPE'].isin([3,4,5,9,10])) & (df['CRACKING_PERCENT']<75)),
        'sjf91':(df['ValueDate']<=df['BeginDate']),
        'sjf92':((df['THICKNESS_RIGID'].isna()) & (df['SURFACE_TYPE'].isin([2,6]) ) ),
        'sjf93':((df['THICKNESS_FLEXIBLE'].isna()) & (df['SURFACE_TYPE'].isin([3,4,5,9,10]) ) ),
        'sjf94':((df['THICKNESS_FLEXIBLE'].notna()) & (df['SURFACE_TYPE'].isin([7,8]) ) ),
        'sjf95':((df['THICKNESS_RIGID'].notna()) & (df['SURFACE_TYPE'].isin([7,8]) ) ),
        'sjf96':(df['Dir_Through_Lanes']<=df['THROUGH_LANES']),
        'sjf97':(df['TRAVEL_TIME_CODE'].notna() & df['NHS'].notna()),
        'sjf98':( (df['MAINTENANCE_OPERATIONS']) != (df['OWNERSHIP']) ),
        'sjf99':('Need Help'),
        'sjf100':(df['is_sample'].isna())|((df['is_sample'].notna()) &  ( df['FACILITY_TYPE'].isin([1,2]) ) & ((df['F_SYSTEM'].isin([1,2,3,4,5]) ) |(df['F_SYSTEM']==6)) & (df['URBAN_ID']<99999)),
    }
    tmp = df.copy(deep=True)

    # Adds error columns to df iteratively
    for k,v in error_dict.items():
        print(k,v)  
        tmp[k] = v
        print('Here we go')
    # print(df)
    rules = ['sjf98']
    rules = ['sjf01', 'sjf02', 'sjf03', 'sjf04', 'sjf05', 'sjf06', 'sjf07', 'sjf08', 'sjf09', 'sjf10', 'sjf11', 'sjf12', 'sjf13', 'sjf14', 'sjf15', 'sjf16', 'sjf17', 'sjf18', 'sjf19', 'sjf20', 'sjf21', 'sjf22', 'sjf23', 'sjf24', 'sjf25', 'sjf26', 'sjf27', 'sjf28', 'sjf29', 'sjf30', 'sjf31', 'sjf32', 'sjf33', 'sjf34', 'sjf35', 'sjf36', 'sjf37', 'sjf38', 'sjf39', 'sjf40', 'sjf41', 'sjf42', 'sjf43', 'sjf44', 'sjf45', 'sjf46', 'sjf47', 'sjf48', 'sjf49', 'sjf50', 'sjf51', 'sjf52', 'sjf53', 'sjf54', 'sjf55', 'sjf56', 'sjf57', 'sjf58', 'sjf59', 'sjf60', 'sjf61', 'sjf62', 'sjf63', 'sjf64', 'sjf65', 'sjf66', 'sjf67', 'sjf68', 'sjf69', 'sjf70', 'sjf71', 'sjf72', 'sjf73', 'sjf74', 'sjf75', 'sjf76', 'sjf77', 'sjf78', 'sjf79', 'sjf80', 'sjf81', 'sjf82', 'sjf83', 'sjf84', 'sjf85', 'sjf86', 'sjf87', 'sjf88', 'sjf89', 'sjf90', 'sjf91', 'sjf92', 'sjf93', 'sjf94', 'sjf95', 'sjf96', 'sjf97', 'sjf98', 'sjf99', 'sjf100']
    # print('raw True or False Data:',tmp)
    tmp = tmp.mask(tmp[rules].all(axis='columns')).dropna(how='all')
    return tmp
    
print(full_spatial_join(df))
# full_spatial_join(df)
# group = full_spatial_join(df)d
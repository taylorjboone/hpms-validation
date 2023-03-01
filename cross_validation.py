import pandas as pd
import os
import geopandas as gpd

cols_dict = {
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
    '20_FACILITY':'FACILITY_TYPE',
    '21_FAULTING':'FAULTING',
    '25_GRADE_CLASS':'GRADE_CLASSIFICATION',
    '30_IRI_VALUE':'IRI',
    '34_MEDIAN_WIDTH_FT':'MEDIAN_WIDTH',
    '35_NAT_FUNCTIONAL_CLASS':'FUNCTIONAL_CLASS',
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
    '76_TOLL_TYPE':'TOLL_TYPE',
    '77_AADT_SINGLE':'AADT_SINGLE_UNIT',
    '77_AADT_COMBINATION':'AADT_COMBINATION',
    '77_PCT_PEAK_SINGLE':'PCT_DH_SINGLE_UNIT',
    '77_PCT_PEAK_COMBINATION':'PCT_DH_COMBINATION',
    '77_K_FACTOR':"K_FACTOR",
    '77_DIR_FACTOR':'DIR_FACTOR',
    '80_TURN_LANES_LFT':'TURN_LANES_L',
    '81_TURN_LANES_R':'TURN_LANES_R',
    '83_URBAN_CODE':'URBAN_ID',
    '84_WIDENING_OBSTACLE':'WIDENING_OBSTACLE',
    '85_WIDENING_POTENTIAL':'WIDENING_POTENTIAL',
    '115_STRAHNET':'STRAHNET'
}

def get_f_system(value):
    if value in [1, 11]:
        return int(1)
    elif value in [4, 12]:
        return int(2)
    elif value in [2, 14]:
        return int(3)
    elif value in [6, 16]:
        return int(4)
    elif value in [7, 17]:
        return int(5)
    elif value in [8, 18]:
        return int(6)
    elif value in [9, 19]:
        return int(7)

arnold = gpd.read_file(r'C:\Users\E025205\Documents\GitHub\hpms-validation\lrs_data\Route_Status_12_31_2022_WGS_84.zip')
arnold_rids = arnold['ROUTE_ID'].unique().tolist()

def load_defaults(df):
        # Standardizes columns names 
        df.rename(columns=cols_dict, inplace=True)
        cols = df.columns.tolist()

        if not 'Supp_Code' in cols:
            df['Supp_Code'] = df['RouteID'].str[9:11]

        # If RouteNumber column is missing, adds and populates RouteNumber pulled from RouteID
        if not 'RouteNumber' in cols:
            df['RouteNumber'] = df['RouteID'].str[3:7]
            df['RouteNumber'] = df['RouteNumber'].map(lambda x: x.lstrip('0'))
            print('Added RouteNumber column')

        # If Sign System column is missing, adds and populates sign system pulled from RouteID
        if not 'RouteSigning' in cols:
            df['RouteSigning'] = df['RouteID'].str[2]
            print('Added RouteSigning column')

        # Converts 1-19 F System to FHWA 1-7 F System
        if not 'F_SYSTEM' in cols:
            df['F_SYSTEM'] = df['FUNCTIONAL_CLASS'].map(lambda x: get_f_system(x))
            print('Added State Functional Class column')

        # If RouteQualifier column is missing, adds and populates RouteQualifier pulled from RouteID
        qualifier_dict = {'00':1,'01':2, '02':1, '03':5, '04':1, '05':1, '06':1, '07':1, '08':3, '09':3, '10':3, '11':3, '12':3, '13':9, '14':4, '15':6, '16':10, '17':10, '18':10, '19':10, '20':1, '21':10, '22':10, '23':10, '24':7, '25':10, '26':10, '27':10, '28':10, '51':10, '99':10}
        if not 'RouteQualifier' in cols:
            df['RouteQualifier'] = df['RouteID'].str[9:11]
            df['RouteQualifier'] = df['RouteQualifier'].map(lambda x: qualifier_dict[x])

        # Creates Dir through lanes from existing events
        if not 'Dir_Through_Lanes' in cols:
            df['Dir_Through_Lanes'] = ''
            df['Dir_Through_Lanes'].loc[((df['97_ORIG_SURVEY_DIRECTION'].notna()) & (df['97_ORIG_SURVEY_DIRECTION'] == '0'))] = df['PEAK_LANES']
            df['Dir_Through_Lanes'].loc[((df['97_ORIG_SURVEY_DIRECTION'].notna()) & (df['97_ORIG_SURVEY_DIRECTION'].isin(['1','A'])))] = df['COUNTER_PEAK_LANES']

        return df

class Cross_Validation():
    def __init__(self, df):
        self.df = load_defaults(df)

    def inventory_spatial_join(self):
        df = self.df
        # Creates a column for each rule, outputs a False for each row that doesn't pass the rule for a column
        spatial_join_checks = {
            'sji01': ((df['F_SYSTEM'].notna()) & (df['RouteID'].isin(arnold_rids))),
            'sji02': ((df['FACILITY_TYPE'].notna()) & (df['RouteID'].isin(arnold_rids))),
            'sji03': ((df['OWNERSHIP'].notna()) & (df['RouteID'].isin(arnold_rids))),
            'sji04': ((df['URBAN_ID'].notna()) & (df['RouteID'].isin(arnold_rids))),
            'sji05': (((df['FACILITY_TYPE'].notna()) & (df['F_SYSTEM'].notna())) | df['FACILITY_TYPE'].isna()),
            'sji06': (((df['F_SYSTEM'].notna()) & (df['FACILITY_TYPE'].notna())) | df['F_SYSTEM'].isna()),
            'sji07': (((df['F_SYSTEM'] == 1) & (df['FACILITY_TYPE'].isin([1,2])) & (df['RouteNumber'].notna())) | (df['F_SYSTEM'] != 1)),
            'sji08': (((df['F_SYSTEM'] == 1) & (df['NHS'] == 1)) | (df['F_SYSTEM'] != 1)),
            'sji09': (((df['RouteNumber'].notna()) & (df['RouteSigning'].notna())) | (df['RouteNumber'].isna())),
            'sji10': (((df['RouteNumber'].notna()) & (df['RouteQualifier'].notna())) | (df['RouteNumber'].isna())),
            'sji11': (((df['F_SYSTEM'] == 1) & (df['STRAHNET'] == 1)) | (df['F_SYSTEM'] != 1)),
            'sji12': (((df['STRAHNET'].isin([1,2])) & (df['NHS'] == 1)) | (~df['STRAHNET'].isin([1,2]))),
            'sji13': (((df['F_SYSTEM'] == 1) & (df['NN'] == 1)) | (df['F_SYSTEM'] != 1))
        }

        tmp = df.copy()
        for k,v in spatial_join_checks.items():
            tmp[k] = v
        tmp2 = pd.DataFrame()
        for rule in spatial_join_checks.keys():
            tmp2 = tmp2.append(tmp[tmp[rule] == False])
        return tmp2.drop_duplicates()


    def traffic_spatial_join(self):
        df = self.df
        spatial_join_checks = {
            'sjt01': ((df['AADT_SINGLE_UNIT'].isna()) | (df['AADT_SINGLE_UNIT'] > (0.4 * df['AADT']))),
            'sjt02': ((df['AADT_SINGLE_UNIT'].isna()) | (((df['AADT'] > 500) & (df['AADT_SINGLE_UNIT'] > 0)) | (df['AADT'] <= 500))),
            'sjt03': ((df['AADT_SINGLE_UNIT'].isna()) | ((df['AADT_SINGLE_UNIT'] + df['AADT_COMBINATION']) < (0.8 * df['AADT']))),
            'sjt04': ((df['AADT_SINGLE_UNIT'].isna()) | (((df['AADT_SINGLE_UNIT'] * 0.01) < (df['AADT'] * (df['PCT_DH_SINGLE_UNIT'] * .01))) & ((df['AADT'] * (df['PCT_DH_SINGLE_UNIT'] * .01)) < (df['AADT_SINGLE_UNIT'] * 0.5)))),
            'sjt05': ((df['PCT_DH_SINGLE_UNIT'].isna()) | ((df['PCT_DH_SINGLE_UNIT'] > 0) & (df['PCT_DH_SINGLE_UNIT'] < 25))),
            'sjt06': ((df['PCT_DH_SINGLE_UNIT'].isna()) | (((df['AADT_SINGLE_UNIT'] < 50) & (df['PCT_DH_SINGLE_UNIT'] == 0)) | (df['PCT_DH_SINGLE_UNIT'] != 0))),
            'sjt07': ((df['AADT_COMBINATION'].isna()) | (df['AADT_COMBINATION'] < (0.4 * df['AADT'])))
        }

        tmp = df.copy()
        for k,v in spatial_join_checks.items():
            tmp[k] = v
        tmp2 = pd.DataFrame()
        for rule in spatial_join_checks.keys():
            tmp2 = tmp2.append(tmp[tmp[rule] == False])
        return tmp2.drop_duplicates()


input_file = 'lrs_data/lrs_dump_03-01-23.csv'
data = pd.read_csv(input_file)

# data = load_defaults(data)

# # Temporary Arnold dataframe until the real one is added
# arnold = pd.DataFrame()
# arnold['RouteID'] = data['RouteID']

# inventory = inventory_spatial_join(data)
# traffic = traffic_spatial_join(data)

# print(inventory)
# print(traffic)

cross_validation = Cross_Validation(data)
isj_errors = cross_validation.inventory_spatial_join()
tsj_errors = cross_validation.traffic_spatial_join()

print('ISJ Errors', isj_errors)
print('TSJ Errors', tsj_errors)
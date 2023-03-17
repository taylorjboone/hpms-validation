import pandas as pd
import numpy as np
from pandarallel import pandarallel
from datetime import date,timedelta
from dateutil.relativedelta import relativedelta
import random
pandarallel.initialize()
import os
from os import listdir
from os.path import isfile,join



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
facility_list = [1,2,4,5,6]
facility_list2 = [1,2,4]
f_system_list = [1,2,3,4,5,6,7]
urban_id_list = ['06139','15481','21745','36190','40753','59275','67672','93592','94726']

# convert_dict = {
#     'BMP':'BeginPoint',
#     'EMP':'EndPoint',
#     '63_County_Code_Value_Numeric':'COUNTY_ID',
#     '21_AADT_Value_Numeric':'AADT',
#     '28_FUTURE_AADT_Value_Numeric':'FUTURE_AADT',
#     '5_Acces_Control_Value_Numeric':'ACCESS_CONTROL',
#     '20_ALternative_Route_Name_Value_Numeric':'ALT_ROUTE_NAME',
#     '33_At_Grade_Other_Value_Numeric':'AT_GRADE_OTHER',
#     '34_Lane_Width_Value_Numeric':'LANE_WIDTH',
#     '59_Base_Type_Value_Numeric':'BASE_TYPE',
#     '52_Cracking_Percent_Value_Numeric':'CRACKING_PERCENT',
#     '43_Curve_Classification_Value_Numeric':'CURVE_CLASSIFICATION',
#     '66_NN_Value_Numeric':'NN',
#     '3_Facility_Type_Value_Numeric':'FACILITY_TYPE',
#     '51_Faulting_Value_Numeric':'FAULTING',
#     '45_Grade_Classification_Value_Numeric':'GRADE_CLASSIFICATION',
#     '29_HPMS_SAMPLE_NO':'is_sample',
#     '47_IRI_Value_Numeric':'IRI',
#     '36_Median_Width_Value_Numeric':'MEDIAN_WIDTH',
#     '35_Median_Type_Value_Numeric':'MEDIAN_TYPE',
#     '64_NHS_Value_Numeric':'NHS',
#     '31_Number_Signals_Value_Numeric':'NUMBER_SIGNALS',
#     '6_Ownership_Value_Numeric':'OWNERSHIP',
#     '30_Pct_Green_Time_Value_Numeric':'PCT_GREEN_TIME',
#     '46_Pct_Pass_Sight_Value_Numeric':'PCT_PASS_SIGHT',
#     '10_Peak_Lanes_Value_Numeric':'PEAK_LANES',
#     '11_Counter_Peak_Lanes_Value_Numeric':'COUNTER_PEAK_LANES',
#     # '45_PSR':'PSR',
#     '50_Rutting_Value_Numeric':'RUTTING',
#     '37_Shoulder_Type_Value_Numeric':'SHOULDER_TYPE',
#     '39_Left_Shoulder_Width_Value_Numeric':'SHOULDER_WIDTH_L',
#     '38_Right_Shoulder_Width_Value_Numeric':"SHOULDER_WIDTH_R",
#     '29_Signal_Type_Value_Numeric':'SIGNAL_TYPE',
#     '14_SpeedLimit_Value_Numeric':'SPEED_LIMIT',
#     '1_F_System_Value_Numeric':'F_SYSTEM',
#     '32_Stop_Signs_Value_Numeric':'STOP_SIGNS',
#     '49_Surface_Type_Value_Numeric':'SURFACE_TYPE',
#     '44_Terrain_Type_Value_Numeric':'TERRAIN_TYPE',
#     '7_Through_Lanes_Value_Numeric':'THROUGH_LANES',
#     '15_Toll_ID_Value_Numeric':'TOLL_ID',
#     # '76_TOLL_CHARGED':'TOLL_ID',
#     '22_AADT_Single_Unit_Value_Numeric':'AADT_SINGLE_UNIT',
#     '24_AADT_Combination_Value_Numeric':'AADT_COMBINATION',
#     '23_PCT_DH_Single_Unit_Value_Numeric':'PCT_DH_SINGLE_UNIT',
#     '25_Pct_Peak_Combination_Value_Numeric':'PCT_DH_COMBINATION',
#     '26_K_Factor_Value_Numeric':"K_FACTOR",
#     '27_Dir_Factor_Value_Numeric':'DIR_FACTOR',
#     '13_Turn_Lanes_L_Value_Numeric':'TURN_LANES_L',
#     '12_Turn_lanes_R_Value_Numeric':'TURN_LANES_R',
#     '2_Urban_Code_Value_Numeric':'URBAN_ID',
#     '42_Widening_Potential_Value_Numeric':'WIDENING_POTENTIAL',
#     # '85_WIDENING_POTENTIAL':'WIDENING_POTENTIAL',
#     '54_Year_Of_Last_Improvement_Value_Numeric':'YEAR_LAST_OF_IMPROVEMENT',
#     '65_STRAHNET_Type_Value_Numeric':'STRAHNET',
#     '55_Year_of_Last_Construction_Value_Numeric':"YEAR_LAST_OF_CONSTRUCTION",
#     '71_Travel_Time_Code_Value_Numeric':"TRAVEL_TIME_CODE",
#     '57_Thickness_Rigid_Value_Numeric':"THICKNESS_RIGID",
#     '58_Thickness_Flexible_Value_Numeric':"THICKNESS_FLEXIBLE",
#     '68_Maintenance_Operations_Value_Numeric':"MAINTENANCE_OPERATIONS"
# }


# convert_dict = {
#    'Route_ID':'RouteID',
#    'Begin_Point':'BMP',
#    'End_Point':'EMP',
# }


class full_spatial_join_class():

    def __init__(self,df):
        self.df = df
        # print(self.df)       

    # def dummy_data(self):
    #     self.df['RouteID']=self.df['RouteID'].astype(str)
    #     self.df['sup_code'] = self.df['RouteID'].str.slice(9,11)
    #     self.df['BeginDate'] = date.fromisoformat('2022-12-31')
    #     self.df['sign_system'] = self.df['RouteID'].str.slice(2,3)
    #     self.df['section_length'] = self.df['EMP'] - self.df['BMP']
    #     self.df['ValueDate'] = date.fromisoformat('2022-10-31')
    #     self.df['ValueText'] ='A'
    #     self.df['TRAVEL_TIME_CODE'] = 'asdfasdfasf'
    #     self.df['YEAR_LAST_CONSTRUCTION'] = date.fromisoformat('2010-12-31') 
    #     self.df['THICKNESS_RIGID'] = np.random.randint(1, 20, self.df.shape[0])
    #     self.df['THICKNESS_FLEXIBLE'] = np.random.randint(1, 20, self.df.shape[0])
    #     self.df['MAINTENANCE_OPERATIONS'] = np.random.randint(1, 12, self.df.shape[0])
    
    # def create_data_files(self,mypath):
    #     df = pd.DataFrame(columns=['Year_Record','State_Code','RouteID','BMP','EMP','Data_Item','Section_Length','Value_Numeric','Value_Text','Value_Date','Comments'])
    #     df.to_csv('C:\\PythonTest\\Voltron\\district_chrystal_report_website\\hpms-validation\\base_file.csv')
    #     onlyfiles = [os.path.join(mypath,f) for f in listdir(mypath) if isfile(join(mypath, f))]
    #     for split_file in onlyfiles[-6:]:
    #         print(split_file)
    #         df = pd.read_csv(split_file,sep='|')
    #         print(len(df.axes[1]))
    #         prefix = split_file.replace("C:\PythonTest\Voltron\district_chrystal_report_website\hpms-validation\hpms_data_items\data_items\DataItem","").split(".")[0]
    #         print(prefix)
    #         prefix2 = prefix.replace("C:\PythonTest\Voltron\district_chrystal_report_website\hpms-validation\hpms_data_items\data_items","")
    #         # print(prefix2)
    #         command = f'lrsops split -b C:\\PythonTest\\Voltron\\district_chrystal_report_website\\hpms-validation\\base_file.csv -s {split_file} -c "Data_Item,Value_Numeric" --prefix "{prefix2}" -o C:\\PythonTest\\Voltron\\district_chrystal_report_website\\hpms-validation\\base_file.csv' 
    #         print(command)            
    #         os.system(command)
        
    def check_rule_sjf43(self):
        sum_curve = self.df.loc[( self.df['CURVE_CLASSIFICATION'].notna()),'section_length'].sum()
        sum_sample = self.df.loc[ (((self.df['is_sample'].notna())&( self.df['F_SYSTEM'].isin([1,2,3]) )) | ( (self.df['F_SYSTEM']==4) & (self.df['URBAN_ID']==99999) ) ),'section_length'].sum()
        if sum_curve != sum_sample:
            print('Sums are not equal, please review')
            return False
        else:
            print('Sums are equal')
            return True
        
    def check_rule_sjf47(self):
        sum_grade = self.df.loc[( self.df['GRADE_CLASSIFICATION'].notna()),'section_length'].sum()
        sum_sample = self.df.loc[ (((self.df['is_sample'].notna())&( self.df['F_SYSTEM'].isin([1,2,3]) )) | ( (self.df['F_SYSTEM']==4) & (self.df['URBAN_ID']==99999) ) ),'section_length'].sum()
        if sum_grade != sum_sample:
            print('Sums are not equal, please review')
            return False
        else:
            print('Sums are equal')
            return True
        
    # def produce_dir_through_lanes(self, x):
    #     boolval = (len(x['RouteID']) == 13) and (x.section_length > 0) and (int(x.sup_code) < 24) and \
    #     (x['70_SURFACE_TYPE'] !=1) and  not (x['97_ORIG_SURVEY_DIRECTION']==np.isnan) and \
    #         (x.sign_system=='1')
    #     if boolval and (x['97_ORIG_SURVEY_DIRECTION'] == '0'):
    #         x['Dir_Through_Lanes']=x['43_PEAK_LANES']
    #     elif boolval and (x['97_ORIG_SURVEY_DIRECTION'] in ['1','A']):
    #         x['Dir_Through_Lanes']=x['43_COUNTER_PEAK_LANES']
    #     else:
    #         # print('NAN')
    #         pass
    #     return x
    
    def load_defaults(self):
        print(self.df.head(60))
        cols = self.df.columns.tolist()


        # # If RouteNumber column is missing, adds and populates RouteNumber pulled from RouteID
        # if not 'RouteNumber' in cols:
        #     df['RouteNumber'] = df['RouteID'].str[3:7]
        #     df['RouteNumber'] = df['RouteNumber'].map(lambda x: x.lstrip('0'))
        #     print('Added RouteNumber column')

        # # If Sign System column is missing, adds and populates sign system pulled from RouteID
        # if not 'RouteSigning' in cols:
        #     df['RouteSigning'] = df['RouteID'].str[2]
        #     print('Added RouteSigning column')

        # if not '65_STATE_FUNCTIONAL_CLASS' in cols:
        #     df['65_STATE_FUNCTIONAL_CLASS'] = df['35_NAT_FUNCTIONAL_CLASS'].map(f_system_dict)
        #     print('Added State Functional Class column')

        # if not 'Dir_Through_lanes' in cols:
        #     df = df.apply(self.produce_dir_through_lanes,axis=1)
        #     # print(df['Dir_Through_Lanes'])

        return self.df 
    
    def full_spatial_join(self):
        df = self.df
        # df = self.load_defaults()
        # df = df.rename(columns=convert_dict)
        error_dict = {
            'sjf01':((df['F_SYSTEM'].notna()) & (df['FACILITY_TYPE'].isin(facility_list))),
            'sjf02':(((df['URBAN_ID'].notna()) & (df['FACILITY_TYPE'].isin(facility_list2)) & \
            (df['F_SYSTEM'].isin(f_system_list))) | \
            ((df['FACILITY_TYPE'] == 6) & \
            (df['DIR_THROUGH_LANES'] > 0) & \
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
            'sjf43': self.check_rule_sjf43(),
            'sjf44':((df['is_sample'].notna()) & (df['URBAN_ID']==99999) & (df['F_SYSTEM'].isin([1,2,3,4,5]))),
            'sjf45':(   ( df['GRADE_CLASSIFICATION'].notna() ) &   ( df['is_sample'].notna() ) & ((df['F_SYSTEM'].isin([1,2,3,4]) )  | (df['F_SYSTEM']==4) ) & (df['URBAN_ID'] == 99999) & (df['SURFACE_TYPE'] > 1)) ,#Double check, similiar to previous rules
            'sjf46':(   ( df['GRADE_CLASSIFICATION'].notna() ) &   ( df['is_sample'].notna() ) & ((df['F_SYSTEM'].isin([1,2,3,4]) )  | (df['F_SYSTEM']==4) ) & (df['URBAN_ID'] == 99999) & (df['SURFACE_TYPE'] > 1)) ,
            'sjf47': self.check_rule_sjf47(),
            'sjf48':( ( df['is_sample'].notna() ) & (df['URBAN_ID']==99999) & (df['THROUGH_LANES']==2) & (df['MEDIAN_TYPE'].isin([1,2])) & (df['SURFACE_TYPE'] > 1)),
            'sjf49':(df['IRI'].isna()) | ((df['IRI'].notna()) & (df['SURFACE_TYPE']>1)&((df['FACILITY_TYPE'].isin([1,2]))&((df['PSR']!='A')&((df['F_SYSTEM'].isin([1,2,3])) | (df['NHS'].notna()))|((df['is_sample'].notna())&(df['F_SYSTEM']==4)&(df['URBAN_ID']==99999)))|(df['DIR_THROUGH_LANES']>0))),
            'sjf50':((df['PSR'].notna())&(df['IRI'].isna())&(df['FACILITY_TYPE'].isin([1,2]))&(df['SURFACE_TYPE'])),
            'sjf51':(df['SURFACE_TYPE'].isna())|((df['SURFACE_TYPE'].notna()) & (df['FACILITY_TYPE'].isin([1,2])) & ((df['F_SYSTEM']==1) | (df['NHS'].notna()) | (df['is_sample'].notna())) | ((df['DIR_THROUGH_LANES'] > 0) & ((df['IRI'].notna())|(df['PSR'].notna())))),
            'sjf52':(df['RUTTING'].isna())|((df['SURFACE_TYPE'].isin([2,6,7,8]))&((df['FACILITY_TYPE'].isin([1,2]))&((df['F_SYSTEM']==1)|(df['NHS'].notna())|(df['is_sample'].notna()))|(df['DIR_THROUGH_LANES']>0)&((df['IRI'].notna())|(df['PSR'].notna())))),
            'sjf53':(df['FAULTING'].isna())|((df['SURFACE_TYPE'].isin([3,4,9,10]))&((df['FACILITY_TYPE'].isin([1,2]))&((df['F_SYSTEM']==1)|(df['NHS'].notna())|(df['is_sample'].notna()))|(df['DIR_THROUGH_LANES']>0)&((df['IRI'].notna())|(df['PSR'].notna())))),
            'sjf54':(df['CRACKING_PERCENT'].isna())|((df['SURFACE_TYPE'].isin([2,3,4,5,6,7,8,9]))&((df['FACILITY_TYPE'].isin([1,2]))&((df['F_SYSTEM']==1)|(df['NHS'].notna())|(df['is_sample'].notna()))|(df['DIR_THROUGH_LANES']>0)&((df['IRI'].notna())|(df['PSR'].notna())))),
            'sjf55':(((df['SURFACE_TYPE'].isin([2,3,4,5,6,7,8,9,10])) & (df['is_sample'].notna()) )| ((df['YEAR_LAST_CONSTRUCTION'].notna()) & (df['YEAR_LAST_CONSTRUCTION'] < (df['BeginDate']-timedelta(days = 7305)) ) ) ),
            'sjf56':((df['SURFACE_TYPE'].isin([2,3,4,5,6,7,8,9,10])) & (df['is_sample'].notna())),
            'sjf57':((df['is_sample'].notna()) & (df['YEAR_LAST_IMPROVEMENT'].notna())),
            'sjf58':((df['SURFACE_TYPE'].isin([3,4,5,6,7,8,9,10])) & (df['is_sample'].notna())),
            'sjf59':((df['SURFACE_TYPE'].isin([2,6,7,8])) & (df['is_sample'].notna())),
            'sjf60':((df['is_sample'].notna()) & (df['SURFACE_TYPE'] > 1)),
            'sjf61':(((df['BASE_TYPE'].notna()) & (df['BASE_TYPE'].astype('Int64') > 1)) & (df['SURFACE_TYPE'] > 1) & (df['is_sample'].notna())),
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
            'sjf88':(df['FAULTING'] <= 1),
            'sjf89': ((~df['SURFACE_TYPE'].isin([2,6,7,8])) | (((df['LANE_WIDTH'] == 10) & (df['CRACKING_PERCENT'] < 65)) | ((df['LANE_WIDTH'] == 11) & (df['CRACKING_PERCENT'] < 59)) | ((df['LANE_WIDTH'] == 12) & (df['CRACKING_PERCENT'] < 54)))),  
            'sjf90':((df['SURFACE_TYPE'].isin([3,4,5,9,10])) & (df['CRACKING_PERCENT']<75)),
            'sjf91':(df['ValueDate']<=df['BeginDate']),
            'sjf92':((df['THICKNESS_RIGID'].isna()) & (df['SURFACE_TYPE'].isin([2,6]) ) ),
            'sjf93':((df['THICKNESS_FLEXIBLE'].isna()) & (df['SURFACE_TYPE'].isin([3,4,5,9,10]) ) ),
            'sjf94':((df['THICKNESS_FLEXIBLE'].notna()) & (df['SURFACE_TYPE'].isin([7,8]) ) ),
            'sjf95':((df['THICKNESS_RIGID'].notna()) & (df['SURFACE_TYPE'].isin([7,8]) ) ),
            'sjf96':(df['DIR_THROUGH_LANES']<=df['THROUGH_LANES']),
            'sjf97':(df['TRAVEL_TIME_CODE'].notna() & df['NHS'].notna()),
            'sjf98':( (df['MAINTENANCE_OPERATIONS']) != (df['OWNERSHIP']) ),
            'sjf99':(True),#TODO Figure out how to write this, seems to need to remember older results
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

        
# full_spatial = full_spatial_join_class('lrs_data/lrs_dump_12-05-22.csv')
# full_spatial.full_spatial_join()
# print(meh.create_data_files)
import pandas as pd
import numpy as np
# from pandarallel import pandarallel
from datetime import date,timedelta
from dateutil.relativedelta import relativedelta
import random
import numpy as np
from os import listdir
from os.path import isfile, join 
from domain_check_class_draft import DomainCheck
from pm2_validations import pm2_spatial_join
from full_spatial_join_class import full_spatial_join_class
from cross_validation import Cross_Validation
import os
import datetime


mypath = "hpms_data_items/data_items/"
class Validations():
    def __init__(self):
        self.convert_dict = {
            'Year_Record':'BeginDate',
            'BMP':'BeginPoint',
            'EMP':'EndPoint',
            'Value_Date':'ValueDate',
            'ValueNumeric':'Value_Numeric',
            '63_County_CodeValue_Numeric':'COUNTY_ID',
            '21_AADTValue_Numeric':'AADT',
            '28_FUTURE_AADTValue_Numeric':'FUTURE_AADT',
            '28_Future_AADTValue_Numeric':'FUTURE_AADT',
            '5_Access_ControlValue_Numeric':'ACCESS_CONTROL',
            '20_Alternative_Route_NameValue_Numeric':'ALT_ROUTE_NAME',
            '33_At_Grade_OtherValue_Numeric':'AT_GRADE_OTHER',
            '34_Lane_WidthValue_Numeric':'LANE_WIDTH',
            '59_Base_TypeValue_Numeric':'BASE_TYPE',
            '52_Cracking_PercentValue_Numeric':'CRACKING_PERCENT',
            '52_Cracking_Percent_non_interstate_NHSValue_Numeric':'CRACKING_PERCENT',
            '43_Curve_ClassificationValue_Numeric':'CURVE_CLASSIFICATION',
            '66_NNValue_Numeric':'NN',
            '3_Facility_TypeValue_Numeric':'FACILITY_TYPE',
            '51_Faulting_non_interstate_NHSValue_Numeric':'FAULTING',
            '45_Grade_ClassifcationValue_Numeric':'GRADE_CLASSIFICATION',
            '29_HPMS_SAMPLE_NO':'is_sample',
            '47_IRI_non_interstate_NHSValue_Numeric':'IRI',
            '47_IRIValue_Numeric':'IRI',
            '36_Median_WidthValue_Numeric':'MEDIAN_WIDTH',
            '35_Median_TypeValue_Numeric':'MEDIAN_TYPE',
            '64_NHSValue_Numeric':'NHS',
            '31_Number_SignalsValue_Numeric':'NUMBER_SIGNALS',
            '6_OwnershipValue_Numeric':'OWNERSHIP',
            '30_Pct_Green_TimeValue_Numeric':'PCT_GREEN_TIME',
            '46_Pct_Pass_SightValue_Numeric':'PCT_PASS_SIGHT',
            '10_Peak_LanesValue_Numeric':'PEAK_LANES',
            '11_Counter_Peak_LanesValue_Numeric':'COUNTER_PEAK_LANES',
            # '45_PSR':'PSR',
            '45_PSRValue_Numeric':'PSR',
            '50_Rutting_non_interstate_NHSValue_Numeric':'RUTTING',
            '37_Shoulder_TypeValue_Numeric':'SHOULDER_TYPE',
            '39_Left_Shoulder_WidthValue_Numeric':'SHOULDER_WIDTH_L',
            '38_Right_Shoulder_WidthValue_Numeric':"SHOULDER_WIDTH_R",
            '29_Signal_TypeValue_Numeric':'SIGNAL_TYPE',
            '14_SpeedLimitValue_Numeric':'SPEED_LIMIT',
            '1_F_SystemValue_Numeric':'F_SYSTEM',
            '32_Stop_SignsValue_Numeric':'STOP_SIGNS',
            '49_Surface_TypeValue_Numeric':'SURFACE_TYPE',
            '44_Terrain_TypeValue_Numeric':'TERRAIN_TYPE',
            '7_Through_LanesValue_Numeric':'THROUGH_LANES',
            '15_Toll_IDValue_Numeric':'TOLL_ID',
            # '76_TOLL_CHARGED':'TOLL_ID',
            '22_AADT_Single_UnitValue_Numeric':'AADT_SINGLE_UNIT',
            '24_AADT_CombinationValue_Numeric':'AADT_COMBINATION',
            '23_PCT_DH_Single_UnitValue_Numeric':'PCT_DH_SINGLE_UNIT',
            '23_Pct_Peak_SingleValue_Numeric':'PCT_DH_SINGLE_UNIT',
            '25_Pct_Peak_CombinationValue_Numeric':'PCT_DH_COMBINATION',
            '25_PCT_DH_CombinationValue_Numeric':'PCT_DH_COMBINATION',
            '26_K_FactorValue_Numeric':"K_FACTOR",
            '27_Dir_FactorValue_Numeric':'DIR_FACTOR',
            '13_Turn_Lanes_LValue_Numeric':'TURN_LANES_L',
            '12_Turn_lanes_RValue_Numeric':'TURN_LANES_R',
            '2_Urban_CodeValue_Numeric':'URBAN_ID',
            '42_Widening_PotentialValue_Numeric':'WIDENING_POTENTIAL',
            # '85_WIDENING_POTENTIAL':'WIDENING_POTENTIAL',
            '54_Year_Of_Last_ImprovementValue_Numeric':'YEAR_LAST_IMPROVEMENT',
            '54_Year_of_Last_ImprovementValue_Numeric':'YEAR_LAST_IMPROVEMENT',
            '65_STRAHNET_TypeValue_Numeric':'STRAHNET',
            '55_Year_of_Last_ConstructionValue_Numeric':"YEAR_LAST_CONSTRUCTION",
            '71_Travel_Time_CodeValue_Numeric':"TRAVEL_TIME_CODE",
            '57_Thickness_RigidValue_Numeric':"THICKNESS_RIGID",
            '58_Thickness_FlexibleValue_Numeric':"THICKNESS_FLEXIBLE",
            '68_Maintenance_OperationsValue_Numeric':"MAINTENANCE_OPERATIONS",
            '70_Dir_Through_LanesValue_Numeric':"DIR_THROUGH_LANES",
            '99_samples_2021Value_Numeric':'is_sample',
            '40_Peak_ParkingValue_Numeric':'PEAK_PARKING',
            '18_Route_SigningValue_Numeric':'ROUTE_SIGNING',
            '19_Route_QualifierValue_Numeric':'ROUTE_QUALIFIER',
            '17_RouteNumberValue_Numeric':'ROUTE_NUMBER',
            '60_Base_ThicknessValue_Numeric':'BASE_THICKNESS',
            '56_Last_Overlay_ThicknessValue_Numeric':'LAST_OVERLAY_THICKNESS',
        }
        pavement_prefix_list = [
            '50_Rutting_non_interstate_NHS',
            '51_Faulting_non_interstate_NHS',
            '52_Cracking_Percent_non_interstate_NHS',
            '54_Year_of_Last_Improvement',
            '55_Year_of_Last_Construction',
            '56_Last_Overlay_Thickness',
            '57_Thickness_Rigid',
            '58_Thickness_Flexible',
            '59_Base_Type','59_Base_Type'
       ]
        
        df = pd.DataFrame(columns=['BeginDate','State_Code','RouteID','BMP','EMP','Data_Item','section_length','Value_Numeric','Value_Text','Value_Date','Comments'])
        print('Created base file dataframe',df.columns)
        df.to_csv('base_file.csv')
        onlyfiles = [os.path.join(mypath,f) for f in listdir(mypath) if isfile(join(mypath, f))]
        for split_file in onlyfiles:
            # print(split_file)
            # # split_file.replace('-','_')
            # base=pd.read_csv(split_file)
            # print('Reading in CSV of split_file',base.columns)
            # base.rename(columns={'ValueNumeric':'Value_Numeric'},inplace=True)
            # print('Renamed split_file with dict',base.columns)
            # base.to_csv(split_file,sep='|',index=False)
            tmp = pd.read_csv(split_file, sep='|')
            tmp.rename(columns={'ValueNumeric': 'Value_Numeric','ValueText':'Value_Text','ValueDate':'Value_Date'}, inplace=True)
            tmp.to_csv(split_file, sep='|', index=False)

            # print(tmp[['Value_Date','Value_Numeric','Value_Text']])
            prefix = split_file.replace("hpms_data_items/data_items/DataItem","").split(".")[0]
            # print('First prefix',prefix)
            prefix2 = prefix.replace("hpms_data_items/data_items","")
            # print('Second prefix',prefix2)
            # print("Split file before being filtered",split_file)
            print(split_file)
            if prefix2 in pavement_prefix_list or True:
                # print('Second Prefix',prefix2)
                command = f'lrsops split -b base_file.csv -s {split_file} -c "Value_Numeric,Value_Date" --prefix "{prefix2}" -o base_file.csv'
                print('pavement here',command)
            else:
                command = f'lrsops split -b base_file.csv -s {split_file} -c "Value_Numeric" --prefix "{prefix2}" -o base_file.csv'
 
            # print(command)            
            os.system(command)
        self.df = pd.read_csv('base_file.csv')
        # df = df.rename(columns=self.convert_dict)
        # self.df = self.df.rename(columns=self.correct_format)
        self.df['section_length'] = self.df['EMP'] - self.df['BMP']
        self.df['Year_Record'] = datetime.datetime(2022,1,1)
        print('hey, where is sectionLength',self.df.columns)
        self.df = self.df.rename(columns=self.convert_dict)
        self.df['YEAR_LAST_CONSTRUCTION'] = pd.to_datetime(self.df['YEAR_LAST_CONSTRUCTION'])
        print('SectionLength is where?',self.df.columns)
    
    # def split_combine(self):
        # df = pd.DataFrame(columns=['BeginDate','State_Code','RouteID','BMP','EMP','Data_Item','section_length','Value_Numeric','Value_Text','Value_Date','Comments'])
        # print('Created base file dataframe',df.columns)
        # df.to_csv('base_file.csv')
        # onlyfiles = [os.path.join(mypath,f) for f in listdir(mypath) if isfile(join(mypath, f))]
        # for split_file in onlyfiles:
        #     # print(split_file)
        #     # # split_file.replace('-','_')
        #     # base=pd.read_csv(split_file)
        #     # print('Reading in CSV of split_file',base.columns)
        #     # base.rename(columns={'ValueNumeric':'Value_Numeric'},inplace=True)
        #     # print('Renamed split_file with dict',base.columns)
        #     # base.to_csv(split_file,sep='|',index=False)
        #     tmp = pd.read_csv(split_file, sep='|')
        #     tmp.rename(columns={'ValueNumeric': 'Value_Numeric'}, inplace=True)
        #     tmp.to_csv(split_file, sep='|', index=False)
        #     prefix = split_file.replace("hpms_data_items/data_items/DataItem","").split(".")[0]
        #     # print(prefix)
        #     prefix2 = prefix.replace("hpms_data_items/data_items","")
        #     # print(prefix2)
        #     command = f'lrsops split -b base_file.csv -s {split_file} -c "Value_Numeric" --prefix "{prefix2}" -o base_file.csv' 
        #     # print(command)            
        #     os.system(command)
        # self.df = pd.read_csv('base_file.csv')
        # # df = df.rename(columns=self.convert_dict)
        # # self.df = self.df.rename(columns=self.correct_format)
        # self.df['section_length'] = self.df['EMP'] - self.df['BMP']
        # self.df['Year_Record'] = datetime.datetime(2022,1,1)
        # print('hey, where is sectionLength',self.df.columns)
        # return self.df
            
        
    # def read_combined_file(self):
    #     self.df = pd.read_csv('base_file.csv')
    #     self.df = self.df.rename(columns=self.convert_dict)
    #     print('SectionLength is where?',self.df.columns)


    
    def check_full_spatial(self):
        df = self.df
        spat = full_spatial_join_class(df)
        spat.full_spatial_join()

    def domain_check(self):
        df = self.df
        dom = DomainCheck(df)
        dom.check_all()

    def cross_check(self):
        df = self.df
        cross = Cross_Validation(df)
        cross.inventory_spatial_join()
        cross.traffic_spatial_join()

        

val = Validations()
print(val.check_full_spatial())
  

import pandas as pd
import numpy as np
from pandarallel import pandarallel
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


mypath = "hpms_data_items/data_items/"
class Validations():
    def __init__(self):
        self.convert_dict = {
    'BMP':'BeginPoint',
    'EMP':'EndPoint',
    '63_County_Code_Value_Numeric':'COUNTY_ID',
    '21_AADT_Value_Numeric':'AADT',
    '28_FUTURE_AADT_Value_Numeric':'FUTURE_AADT',
    '5_Acces_Control_Value_Numeric':'ACCESS_CONTROL',
    '20_ALternative_Route_Name_Value_Numeric':'ALT_ROUTE_NAME',
    '33_At_Grade_Other_Value_Numeric':'AT_GRADE_OTHER',
    '34_Lane_Width_Value_Numeric':'LANE_WIDTH',
    '59_Base_Type_Value_Numeric':'BASE_TYPE',
    '52_Cracking_Percent_Value_Numeric':'CRACKING_PERCENT',
    '43_Curve_Classification_Value_Numeric':'CURVE_CLASSIFICATION',
    '66_NN_Value_Numeric':'NN',
    '3_Facility_Type_Value_Numeric':'FACILITY_TYPE',
    '51_Faulting_Value_Numeric':'FAULTING',
    '45_Grade_Classification_Value_Numeric':'GRADE_CLASSIFICATION',
    '29_HPMS_SAMPLE_NO':'is_sample',
    '47_IRI_Value_Numeric':'IRI',
    '36_Median_Width_Value_Numeric':'MEDIAN_WIDTH',
    '35_Median_Type_Value_Numeric':'MEDIAN_TYPE',
    '64_NHS_Value_Numeric':'NHS',
    '31_Number_Signals_Value_Numeric':'NUMBER_SIGNALS',
    '6_Ownership_Value_Numeric':'OWNERSHIP',
    '30_Pct_Green_Time_Value_Numeric':'PCT_GREEN_TIME',
    '46_Pct_Pass_Sight_Value_Numeric':'PCT_PASS_SIGHT',
    '10_Peak_Lanes_Value_Numeric':'PEAK_LANES',
    '11_Counter_Peak_Lanes_Value_Numeric':'COUNTER_PEAK_LANES',
    # '45_PSR':'PSR',
    '50_Rutting_Value_Numeric':'RUTTING',
    '37_Shoulder_Type_Value_Numeric':'SHOULDER_TYPE',
    '39_Left_Shoulder_Width_Value_Numeric':'SHOULDER_WIDTH_L',
    '38_Right_Shoulder_Width_Value_Numeric':"SHOULDER_WIDTH_R",
    '29_Signal_Type_Value_Numeric':'SIGNAL_TYPE',
    '14_SpeedLimit_Value_Numeric':'SPEED_LIMIT',
    '1_F_System_Value_Numeric':'F_SYSTEM',
    '32_Stop_Signs_Value_Numeric':'STOP_SIGNS',
    '49_Surface_Type_Value_Numeric':'SURFACE_TYPE',
    '44_Terrain_Type_Value_Numeric':'TERRAIN_TYPE',
    '7_Through_Lanes_Value_Numeric':'THROUGH_LANES',
    '15_Toll_ID_Value_Numeric':'TOLL_ID',
    # '76_TOLL_CHARGED':'TOLL_ID',
    '22_AADT_Single_Unit_Value_Numeric':'AADT_SINGLE_UNIT',
    '24_AADT_Combination_Value_Numeric':'AADT_COMBINATION',
    '23_PCT_DH_Single_Unit_Value_Numeric':'PCT_DH_SINGLE_UNIT',
    '25_Pct_Peak_Combination_Value_Numeric':'PCT_DH_COMBINATION',
    '26_K_Factor_Value_Numeric':"K_FACTOR",
    '27_Dir_Factor_Value_Numeric':'DIR_FACTOR',
    '13_Turn_Lanes_L_Value_Numeric':'TURN_LANES_L',
    '12_Turn_lanes_R_Value_Numeric':'TURN_LANES_R',
    '2_Urban_Code_Value_Numeric':'URBAN_ID',
    '42_Widening_Potential_Value_Numeric':'WIDENING_POTENTIAL',
    # '85_WIDENING_POTENTIAL':'WIDENING_POTENTIAL',
    '54_Year_Of_Last_Improvement_Value_Numeric':'YEAR_LAST_OF_IMPROVEMENT',
    '65_STRAHNET_Type_Value_Numeric':'STRAHNET',
    '55_Year_of_Last_Construction_Value_Numeric':"YEAR_LAST_OF_CONSTRUCTION",
    '71_Travel_Time_Code_Value_Numeric':"TRAVEL_TIME_CODE",
    '57_Thickness_Rigid_Value_Numeric':"THICKNESS_RIGID",
    '58_Thickness_Flexible_Value_Numeric':"THICKNESS_FLEXIBLE",
    '68_Maintenance_Operations_Value_Numeric':"MAINTENANCE_OPERATIONS",
    '70_Dir_Through_Lanes':"DIR_THROUGH_LANES",
}
        
    def split_combine(self,mypath):
        df = pd.DataFrame(columns=['Year_Record','State_Code','RouteID','BMP','EMP','Data_Item','Section_Length','Value_Numeric','Value_Text','Value_Date','Comments'])
        df.to_csv('base_file.csv')
        onlyfiles = [os.path.join(mypath,f) for f in listdir(mypath) if isfile(join(mypath, f))]
        for split_file in onlyfiles:
            # print(split_file)
            # df = pd.read_csv(split_file,sep='|')
            # print(len(df.axes[1]))
            prefix = split_file.replace("hpms_data_items/data_items/DataItem","").split(".")[0]
            # print(prefix)
            prefix2 = prefix.replace("hpms_data_items/data_items","")
            # print(prefix2)
            command = f'lrsops split -b base_file.csv -s {split_file} -c "Data_Item,Value_Numeric" --prefix "{prefix2}" -o base_file.csv' 
            # print(command)            
            os.system(command)
        return 'base_file.csv'         
        
    def read_combined_file(self):
        self.df = pd.read_csv(self.split_combine)
        self.df = self.df.rename(columns=self.convert_dict)
        return self.df
    
    def check_full_spatial(self):
        print('**************',self.read_combined_file)
        full_spatial_join_class(self.read_combined_file)

    def domain_check(self):
        DomainCheck(self.read_combined_file)

    def cross_check(self):
        Cross_Validation(self.read_combined_file)
        

val = Validations()
print(val.domain_check())
  

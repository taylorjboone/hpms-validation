import pandas as pd
from datetime import datetime,timedelta
from dateutil.relativedelta import relativedelta
import numpy as np

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
URBAN_CODE_list = ['06139','15481','21745','36190','40753','59275','67672','93592','94726']

column_list = []



class full_spatial_functions():
  #if the sjf columsn return false, 
  # it means the data passes that validation, 
  # if the sjf column returns True, the
  # data faios the validation
  # Comments in the code are the actual rule that is being applied in the function  
    
    
    def __init__(self,df):
        self.error_df = pd.DataFrame()
        self.df = df
        

    def check_rule_sjf43(self):
        #sums up the section lengths of samples and the section length of curves in order to execute rule sjf43
        sum_curve = self.df.loc[( self.curve_classification.notna()),'Section_Length'].sum()
        sum_sample = self.df.loc[ (((self.samples.notna())&( self.f_system.isin([1,2,3]) )) | ( (self.f_system==4) & (self.URBAN_CODE==99999) ) ),'Section_Length'].sum()
        if sum_curve != sum_sample:
            return True
        else:
            return False
    
    def check_rule_sjf47(self):
        #sums up the section lengths of samples and the section length of grades in order to execute rule sjf47
        sum_grade = self.df.loc[( self.grade_classification.notna()),'Section_Length'].sum()
        sum_sample = self.df.loc[ (((self.samples.notna())&( self.f_system.isin([1,2,3]) )) | ( (self.f_system==4) & (self.URBAN_CODE==99999) ) ),'Section_Length'].sum()
        if sum_grade != sum_sample:
            print('Sums are not equal, please review')
            return True
        else:
            print('Sums are equal')
            return False
    
    def sjf01(self):
        #F_SYSTEM must exist where FACILITY_TYPE is in (1;2;4;5;6) and Must not be NULL
        self.df['SJF01'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['FACILITY_TYPE'].isin([1,2,4,5,6]) & tempDF['F_SYSTEM'].isna()]
        self.df['SJF01'].iloc[tempDF.index.tolist()] = False

    def sjf02(self):
        #URBAN_CODE must exist and must not be NULL where: 1. FACILITY_TYPE in (1;2;4) AND F_SYSTEM in (1;2;3;4;5) 
        # [OR] 2. FACILITY_TYPE = 6 AND DIR_THROUGH_LANES > 0 and F_SYSTEM = 1 AND (IRI IS NOT NULL OR PSR IS NOT NULL)
        self.df['SJF02'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['FACILITY_TYPE'].isin([1,2,4])]
        tempDF = tempDF[tempDF['F_SYSTEM'].isin([1,2,3,4,5])]
        tempDF = tempDF[tempDF['URBAN_CODE'].isna()]
        self.df['SJF02'].iloc[tempDF.index.tolist()] = False

        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['FACILITY_TYPE']==6]
        tempDF = tempDF[tempDF['DIR_THROUGH_LANES']>0]
        tempDF = tempDF[tempDF['F_SYSTEM']==1]
        # tempDF = tempDF[tempDF['IRI'].notna() | tempDF['PSR'].notna()]
        tempDF = tempDF[tempDF['IRI'].notna()]
        tempDF = tempDF[tempDF['URBAN_CODE'].isna()]
        
        self.df['SJF02'].iloc[tempDF.index.tolist()] = False

    def sjf03(self):
        #FACILITY_TYPE must exist where F_SYSTEM in (1;2;3;4;5;6;7)  and must not be NULL
        self.df['SJF03'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['F_SYSTEM'].isin(range(1,8)) & tempDF['FACILITY_TYPE'].isna()]
        self.df['SJF03'].iloc[tempDF.index.tolist()] = False

    def sjf04(self):
        #No validation 
        self.df['SJF04'] = True

    def sjf05(self):
        #ACCESS_CONTROL must exist where (F_SYSTEM in (1;2;3) or Sample or NHS) AND FACILITY_TYPE IN (1;2) and must not be NULL
        self.df['SJF05'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['F_SYSTEM'].isin([1,2,3]) | tempDF['sample_Value_Numeric'].notna() | tempDF['NHS'].notna()]
        tempDF = tempDF[tempDF['FACILITY_TYPE'].isin([1,2])]
        tempDF = tempDF[tempDF['ACCESS_CONTROL'].isna()]
        self.df['SJF05'].iloc[tempDF.index.tolist()] = False


    def sjf06(self):
        #OWNERSHIP must exist where (F_SYSTEM in (1;2;3;4;5;6;7) and FACILITY_TYPE (1;2;5;6) and must not be NULL
        self.df['SJF06'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['F_SYSTEM'].isin(range(1,8))]
        tempDF = tempDF[tempDF['FACILITY_TYPE'].isin([1,2,5,6])]
        tempDF = tempDF[tempDF['OWNERSHIP'].isna()]
        self.df['SJF06'].iloc[tempDF.index.tolist()] = False

    def sjf07(self):
        #THROUGH_LANES must exist where FACILITY_TYPE in (1;2;4) 
        # AND (F_SYSTEM in (1;2;3;4;5) or (F_SYSTEM = 6 and URBAN_CODE <99999) or NHS ValueNumeric <> NULL) and must not be NULL
        #Q or (R AND S) or T === (Q or R or T) AND (Q or S or T)
        self.df['SJF07'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['FACILITY_TYPE'].isin([1,2,4])]
        tempDF = tempDF[tempDF['F_SYSTEM'].isin(range(1,6)) | (tempDF['F_SYSTEM'] == 6) | tempDF['NHS'].notna()]
        tempDF = tempDF[tempDF['F_SYSTEM'].isin(range(1,6)) | (tempDF['URBAN_CODE'] < 99999) | tempDF['NHS'].notna()]
        tempDF = tempDF[tempDF['THROUGH_LANES'].isna()]
        self.df['SJF07'].iloc[tempDF.index.tolist()] = False

    def sjf08(self):
        #MANAGED_LANES_TYPE must exist where MANAGED_LANES is not Null
        self.df['SJF08'] = True
        # tempDF = self.df.copy()
        # tempDF[tempDF['MANAGED_LANES'].notna() & tempDF['MANAGED_LANES_TYPE'].isna()]
        # self.df['SJF08'].iloc[tempDF.index.tolist()] = False

    def sjf09(self):
        #MANAGED_LANES must exist where MANAGED_LANES_TYPE is not Null
        self.df['SJF09'] = True
        # tempDF = self.df.copy()
        # tempDF[tempDF['MANAGED_LANES_TYPE'].notna() & tempDF['MANAGED_LANES'].isna()]
        # self.df['SJF09'].iloc[tempDF.index.tolist()] = False

    def sjf10(self):
        #PEAK_LANES must exist on Samples
        self.df['SJF10'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['sample_Value_Numeric'].notna() & tempDF['PEAK_LANES'].isna()]
        self.df['SJF10'].iloc[tempDF.index.tolist()] = False    

    def sjf11(self):
        #COUNTER_PEAK_LANES must exist on Samples where FACILITY_TYPE = 2 AND (URBAN_CODE < 99999 OR THROUGH_LANES >=4)
        self.df['SJF11'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['sample_Value_Numeric'].notna()]
        tempDF = tempDF[tempDF['FACILITY_TYPE']==2]
        tempDF = tempDF[(tempDF['URBAN_CODE']<99999) | (tempDF['THROUGH_LANES'] >= 4)]
        tempDF = tempDF[tempDF['COUNTER_PEAK_LANES'].isna()]
        self.df['SJF11'].iloc[tempDF.index.tolist()] = False

    def sjf12(self):
        #TURN_LANES_R must exist on Samples where URBAN_CODE  < 99999 and ACCESS_CONTROL >1
        self.df['SJF12'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['sample_Value_Numeric'].notna()]
        tempDF = tempDF[(tempDF['URBAN_CODE'] < 99999) & (tempDF['ACCESS_CONTROL'] > 1)]
        tempDF = tempDF[tempDF['TURN_LANES_R'].isna()]
        self.df['SJF12'].iloc[tempDF.index.tolist()] = False

    def sjf13(self):
        #TURN_LANES_L must exist on Samples where URBAN_CODE  < 99999 and ACCESS_CONTROL >1
        self.df['SJF13'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['sample_Value_Numeric'].notna()]
        tempDF = tempDF[(tempDF['URBAN_CODE'] < 99999) & (tempDF['ACCESS_CONTROL'] > 1)]
        tempDF = tempDF[tempDF['TURN_LANES_L'].isna()]
        self.df['SJF13'].iloc[tempDF.index.tolist()] = False

    def sjf14(self):
        #SPEED_LIMIT must exist on Samples and  the NHS
        self.df['SJF14'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['sample_Value_Numeric'].notna() | tempDF['NHS'].notna()]
        tempDF = tempDF[tempDF['SPEED_LIMIT'].isna()]
        self.df['SJF14'].iloc[tempDF.index.tolist()] = False

    def sjf15(self):
        #No validation 
        self.df['SJF15'] = True

    def sjf16(self):
        #ROUTE_NUMBER ValueNumeric Must Exist where (F_SYSTEM in (1;2;3;4) or NHS ValueNumeric <> NULL ) and FACILITY_TYPE (1;2) and ROUTE_SIGNING in (2;3;4;5;6;7;8;9)  
        # OR F_SYSTEM=1 AND FACILITY_TYPE=6 AND DIR_THROUGH_LANES > 0 AND (IRI IS NOT NULL OR PSR IS NOT NULL)
        self.df['SJF16'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['F_SYSTEM'].isin([1,2,3,4]) | tempDF['NHS'].notna()]
        tempDF = tempDF[tempDF['FACILITY_TYPE'].isin([1,2])]
        tempDF = tempDF[tempDF['ROUTE_SIGNING'].isin(range(2,10))]
        tempDF = tempDF[tempDF['ROUTE_NUMBER'].isna()]
        self.df['SJF16'].iloc[tempDF.index.tolist()] = False

        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['F_SYSTEM'] == 1 ]
        tempDF = tempDF[tempDF['FACILITY_TYPE'] == 6]
        tempDF = tempDF[tempDF['DIR_THROUGH_LANES'] > 0]
        # tempDF = tempDF[tempDF['IRI'].notna() | tempDF['PSR'].notna()]
        tempDF = tempDF[tempDF['IRI'].notna()]
        tempDF = tempDF[tempDF['ROUTE_NUMBER'].isna()]
        self.df['SJF16'].iloc[tempDF.index.tolist()] = False

    def sjf17(self):
        #ROUTE_SIGNING must exist where (F_SYSTEM in (1;2;3;4) or NHS) and FACILITY_TYPE (1;2)
        self.df['SJF17'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['F_SYSTEM'].isin([1,2,3,4]) | tempDF['NHS'].notna()]
        tempDF = tempDF[tempDF['FACILITY_TYPE'].isin([1,2])]
        tempDF = tempDF[tempDF['ROUTE_SIGNING'].isna()]
        self.df['SJF17'].iloc[tempDF.index.tolist()] = False   

    def sjf18(self):
        #ROUTE_QUALIFIER must exist where (F_SYSTEM in (1;2;3;4) or NHS) and FACILITY_TYPE (1;2)
        self.df['SJF18'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['F_SYSTEM'].isin([1,2,3,4]) | tempDF['NHS'].notna()]
        tempDF = tempDF[tempDF['FACILITY_TYPE'].isin([1,2])]
        tempDF = tempDF[tempDF['ROUTE_QUALIFIER'].isna()]
        self.df['SJF18'].iloc[tempDF.index.tolist()] = False   

    def sjf19(self):
        #ROUTE_NAME must exist where (F_SYSTEM in (1;2;3;4) or NHS) and FACILITY_TYPE (1;2)
        self.df['SJF19'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['F_SYSTEM'].isin([1,2,3,4]) | tempDF['NHS'].notna()]
        tempDF = tempDF[tempDF['FACILITY_TYPE'].isin([1,2])]
        tempDF = tempDF[tempDF['ALTERNATIVE_ROUTE_NAME'].isna()]
        self.df['SJF19'].iloc[tempDF.index.tolist()] = False

    def sjf20(self):    
        #AADT must exist WHERE: (FACILITY_TYPE in (1;2;4) AND (F_SYSTEM in (1;2;3;4;5)) OR (F_SYSTEM = 6 and URBAN_CODE  <99999) OR NHS ValueNumeric <> NULL 
        self.df['SJF20'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['FACILITY_TYPE'].isin([1,2,4])]
        tempDF = tempDF[tempDF['F_SYSTEM'].isin(range(1,6))]
        tempDF = tempDF[tempDF['AADT'].isna()]
        self.df['SJF20'].iloc[tempDF.index.tolist()] = False

        #(Q and R) OR S === (Q or S) AND (R or S)
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['F_SYSTEM']==6 | tempDF['NHS'].notna()]
        tempDF = tempDF[(tempDF['URBAN_CODE'] < 99999) | tempDF['NHS'].notna()]
        tempDF = tempDF[tempDF['AADT'].isna()]
        self.df['SJF20'].iloc[tempDF.index.tolist()] = False

    def sjf21(self):
        #AADT_SINGLE_UNIT must exist WHERE ((F_SYSTEM in (1) or NHS ValueNumeric <> NULL) and FACILITY_TYPE (1;2)) and on Samples
        self.df['SJF21'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['sample_Value_Numeric'].notna()]
        tempDF = tempDF[tempDF['AADT_SINGLE_UNIT'].isna()]
        self.df['SJF21'].iloc[tempDF.index.tolist()] = False
        
        tempDF = self.df.copy() 
        tempDF = tempDF[(tempDF['F_SYSTEM']==1) | tempDF['NHS'].notna()]
        tempDF = tempDF[tempDF['FACILITY_TYPE'].isin([1,2])]
        tempDF = tempDF[tempDF['AADT_SINGLE_UNIT'].isna()]
        self.df['SJF21'].iloc[tempDF.index.tolist()] = False

    def sjf22(self):
        #PCT_DH_SINGLE_UNIT must exist on Samples
        self.df['SJF22'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['sample_Value_Numeric'].notna() & tempDF['PCT_DH_SINGLE_UNIT'].isna()]
        self.df['SJF22'].iloc[tempDF.index.tolist()] = False

    def sjf23(self):
        #AADT_COMBINATION must exist WHERE ((F_SYSTEM in (1) or NHS ValueNumeric <> NULL) and FACILITY_TYPE (1;2)) and on Samples
        self.df['SJF23'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['sample_Value_Numeric'].notna()]
        tempDF = tempDF[tempDF['AADT_COMBINATION'].isna()]
        self.df['SJF23'].iloc[tempDF.index.tolist()] = False
        
        tempDF = self.df.copy() 
        tempDF = tempDF[(tempDF['F_SYSTEM']==1) | tempDF['NHS'].notna()]
        tempDF = tempDF[tempDF['FACILITY_TYPE'].isin([1,2])]
        tempDF = tempDF[tempDF['AADT_COMBINATION'].isna()]
        self.df['SJF23'].iloc[tempDF.index.tolist()] = False

    def sjf24(self):
        #PCT_DH_COMBINATION must exist on Samples
        self.df['SJF24'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['sample_Value_Numeric'].notna() & tempDF['PCT_DH_COMBINATION'].isna()]
        self.df['SJF24'].iloc[tempDF.index.tolist()] = False

    def sjf25(self):
        #K_FACTOR must exist on Samples
        self.df['SJF25'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['sample_Value_Numeric'].notna() & tempDF['K_FACTOR'].isna()]
        self.df['SJF25'].iloc[tempDF.index.tolist()] = False

    def sjf26(self):
        #DIR_FACTOR must exist on Samples
        self.df['SJF26'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['sample_Value_Numeric'].notna() & tempDF['DIR_FACTOR'].isna()]
        self.df['SJF26'].iloc[tempDF.index.tolist()] = False

    def sjf27(self):
        #FUTURE_AADT must exist on Samples
        self.df['SJF27'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['sample_Value_Numeric'].notna() & tempDF['FUTURE_AADT'].isna()]
        self.df['SJF27'].iloc[tempDF.index.tolist()] = False

    def sjf28(self):
        #SIGNAL_TYPE must exist on Samples WHERE (URBAN_CODE <> 99999 AND NUMBER_SIGNALS >=1)
        self.df['SJF28'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['URBAN_CODE'] != 99999]
        tempDF = tempDF[tempDF['NUMBER_SIGNALS'] >= 1]
        tempDF = tempDF[tempDF['sample_Value_Numeric'].notna()]
        tempDF = tempDF[tempDF['SIGNAL_TYPE'].isna()]
        self.df['SJF28'].iloc[tempDF.index.tolist()] = False 

    def sjf29(self):
        #PCT_GREEN_TIME must exist on Samples WHERE (NUMBER_SIGNALS >=1 AND URBAN_CODE <99999)
        self.df['SJF29'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['URBAN_CODE'] < 99999]
        tempDF = tempDF[tempDF['NUMBER_SIGNALS'] >= 1]
        tempDF = tempDF[tempDF['sample_Value_Numeric'].notna()]
        tempDF = tempDF[tempDF['PCT_GREEN_TIME'].isna()]
        self.df['SJF29'].iloc[tempDF.index.tolist()] = False

    def sjf30(self):
        #NUMBER_SIGNALS must exist on Samples WHERE SIGNAL_TYPE IN (1;2;3;4)
        self.df['SJF30'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['SIGNAL_TYPE'].isin([1,2,3,4])]
        tempDF = tempDF[tempDF['sample_Value_Numeric'].notna()]
        tempDF = tempDF[tempDF['NUMBER_SIGNALS'].isna()]
        self.df['SJF30'].iloc[tempDF.index.tolist()] = False  

    def sjf31(self):
        #STOP_SIGNS (the number of stop sign controlled intersections) must exist on Samples
        self.df['SJF31'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['sample_Value_Numeric'].notna()]
        tempDF = tempDF[tempDF['STOP_SIGNS'].isna()]
        self.df['SJF31'].iloc[tempDF.index.tolist()] = False

    def sjf32(self):
        #AT_GRADE_OTHER (the number of intersections; type 'other') must exist on Samples
        self.df['SJF32'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['sample_Value_Numeric'].notna()]
        tempDF = tempDF[tempDF['AT_GRADE_OTHER'].isna()]
        self.df['SJF32'].iloc[tempDF.index.tolist()] = False 

    def sjf33(self):
        #LANE_WIDTH must exist on Samples
        self.df['SJF33'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['sample_Value_Numeric'].notna()]
        tempDF = tempDF[tempDF['LANE_WIDTH'].isna()]
        self.df['SJF33'].iloc[tempDF.index.tolist()] = False

    def sjf34(self):
        #MEDIAN_TYPE must exist on Samples
        self.df['SJF34'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['sample_Value_Numeric'].notna()]
        tempDF = tempDF[tempDF['MEDIAN_TYPE'].isna()]
        self.df['SJF34'].iloc[tempDF.index.tolist()] = False

    def sjf35(self):
        #MEDIAN_WIDTH must exist on Samples where MEDIAN_TYPE in (2;3;4;5;6;7) 
        self.df['SJF35'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['sample_Value_Numeric'].notna()]
        tempDF = tempDF[tempDF['MEDIAN_TYPE'].isin(range(2,8))]
        tempDF = tempDF[tempDF['MEDIAN_WIDTH'].isna()]
        self.df['SJF35'].iloc[tempDF.index.tolist()] = False

    def sjf36(self):
        #SHOULDER_TYPE must exist on Samples
        self.df['SJF36'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['sample_Value_Numeric'].notna()]
        tempDF = tempDF[tempDF['SHOULDER_TYPE'].isna()]
        self.df['SJF36'].iloc[tempDF.index.tolist()] = False

    def sjf37(self):
        #SHOULDER_WIDTH_R must exist on Samples where SHOULDER_TYPE in (2;3;4;5;6)
        self.df['SJF37'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['sample_Value_Numeric'].notna()]
        tempDF = tempDF[tempDF['SHOULDER_TYPE'].isin(range(2,7))]
        tempDF = tempDF[tempDF['SHOULDER_WIDTH_R'].isna()]
        self.df['SJF37'].iloc[tempDF.index.tolist()] = False

    def sjf38(self):
        #SHOULDER_WIDTH_L must exist on Samples where (SHOULDER_TYPE in (2;3;4;5;6) and MEDIAN_TYPE in (2;3;4;5;6;7))
        self.df['SJF38'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['sample_Value_Numeric'].notna()]
        tempDF = tempDF[tempDF['SHOULDER_TYPE'].isin(range(2,7))]
        tempDF = tempDF[tempDF['MEDIAN_TYPE'].isin(range(2,8))]
        tempDF = tempDF[tempDF['SHOULDER_WIDTH_L'].isna()]
        self.df['SJF38'].iloc[tempDF.index.tolist()] = False

    def sjf39(self):
        #PEAK_PARKING must exist on Samples where URBAN_CODE < 99999
        self.df['SJF39'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['sample_Value_Numeric'].notna()]
        tempDF = tempDF[tempDF['URBAN_CODE'] < 99999]
        tempDF = tempDF[tempDF['PEAK_PARKING'].isna()]
        self.df['SJF39'].iloc[tempDF.index.tolist()] = False

    def sjf40(self):
        #WIDENING_POTENTIAL must exist on Samples
        self.df['SJF40'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['sample_Value_Numeric'].notna()]
        tempDF = tempDF[tempDF['WIDENING_POTENTIAL'].isna()]
        self.df['SJF40'].iloc[tempDF.index.tolist()] = False

    def sjf41(self):
        #CURVES BP/EP on F_SYSTEM in (1;2;3) or F_SYSTEM = 4 and URBAN_CODE = 99999 and SURFACE_TYPE > 1 Must Align with Sample BP/EP
        self.df['SJF41'] = True

    def sjf42(self):
        #At least one CURVES_A-F must be coded for each Sample WHERE (F_SYSTEM in (1;2;3) or F_SYSTEM = 4 and URBAN_CODE = 99999) and SURFACE_TYPE > 1.
        self.df['SJF42'] = True

    def sjf43(self):
        #Sum Length (CURVES_A + CURVES_B + CURVES_C + CURVES_D + CURVES_E + CURVES_E) Must Equal to the Sample Length on 
        # (Sample and (F_SYSTEM (1;2;3) or (F_SYSTEM = 4 and URBAN_CODE = 99999)))
        self.df['SJF43'] = True

    def sjf44(self):
        #TERRAIN_TYPE must exist on Samples WHERE (URBAN_CODE = 99999 AND F_SYSTEM in (1;2;3;4;5))
        self.df['SJF44'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['sample_Value_Numeric'].notna()]
        tempDF = tempDF[tempDF['URBAN_CODE']==99999]
        tempDF = tempDF[tempDF['F_SYSTEM'].isin([1,2,3,4,5])]
        tempDF = tempDF[tempDF['TERRAIN_TYPE'].isna()]
        self.df['SJF44'].iloc[tempDF.index.tolist()] = False

    def sjf45(self):
        #GRADES BP/EP on F_SYSTEM in (1;2;3) or F_SYSTEM = 4 and URBAN_CODE = 99999 and SURFACE_TYPE > 1 Must Align with Sample BP/EP
        self.df['SJF45'] = True

    def sjf46(self):
        #At least one GRADES_A-F must be coded for each Sample WHERE F_SYSTEM in (1;2;3) or F_SYSTEM = 4 and URBAN_CODE = 99999 and SURFACE_TYPE > 1.
        self.df['SJF46'] = True

    def sjf47(self):
        #Sum Length (GRADES_A + GRADES_B + GRADES_C + GRADES_D + GRADES_E + GRADES_E) Must Equal to the Sample Length on (Sample and (F_SYSTEM (1;2;3) or (F_SYSTEM = 4 and URBAN_CODE = 99999)))
        self.df['SJF47'] = True

    def sjf48(self):
        #PCT_PASS_SIGHT must exist on Samples WHERE: (URBAN_CODE = 99999 and THROUGH_LANES =2 and MEDIAN_TYPE in (1;2) and SURFACE_TYPE > 1)
        self.df['SJF48'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['sample_Value_Numeric'].notna()]
        tempDF = tempDF[tempDF['URBAN_CODE']==99999]
        tempDF = tempDF[tempDF['THROUGH_LANES'] == 2]
        tempDF = tempDF[tempDF['MEDIAN_TYPE'].isin([1,2])]
        tempDF = tempDF[tempDF['SURFACE_TYPE'] > 1]
        tempDF = tempDF[tempDF['PCT_PASS_SIGHT'].isna()]
        self.df['SJF48'].iloc[tempDF.index.tolist()] = False

    def sjf49(self):
        #IRI ValueNumeric Must Exist Where SURFACE_TYPE >1 AND (FACILITY_TYPE IN (1;2) AND (PSR ValueText <> 'A' AND (F_SYSTEM in (1;2;3) OR NHS ValueNumeric <>1) 
        # OR Sample sections WHERE (F_SYSTEM = 4 and URBAN_CODE = 99999)OR DIR_THROUGH_LANES >0
        self.df['SJF49'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['F_SYSTEM'].isin([1,2,3]) | tempDF['NHS'] != 1]
        # tempDF = tempDF[tempDF['PSR_VALUE_TEXT'] != 'A']
        tempDF = tempDF[tempDF['FACILITY_TYPE'].isin([1,2])]
        tempDF = tempDF[tempDF['SURFACE_TYPE'] > 1]
        tempDF = tempDF[tempDF['IRI'].isna()]
        self.df['SJF49'].iloc[tempDF.index.tolist()] = False  

        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['sample_Value_Numeric'].notna()]
        tempDF = tempDF[(tempDF['F_SYSTEM'] == 4) | (tempDF['DIR_THROUGH_LANES'] > 0)]
        tempDF = tempDF[(tempDF['URBAN_CODE'] == 99999) | (tempDF['DIR_THROUGH_LANES'] > 0)]
        tempDF = tempDF[tempDF['IRI'].isna()]
        self.df['SJF49'].iloc[tempDF.index.tolist()] = False  

    def sjf50(self):
        #PSR ValueNumeric Must Exist Where IRI ValueNumeric IS NULL AND FACILITY_TYPE IN (1;2) AND SURFACE_TYPE >1 
        # AND(Sample exists AND (F_SYSTEM in (4;6) AND URBAN_CODE <99999 OR F_SYSTEM = 5) 
        # OR (F_SYSTEM = 1 or NHS ValueNumeric <>NULL) AND PSR ValueText = ‘A’)
        self.df['SJF50'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['IRI'].isna()]
        tempDF = tempDF[tempDF['FACILITY_TYPE'].isin([1,2])]
        tempDF = tempDF[tempDF['SURFACE_TYPE'] > 1]
        tempDF = tempDF[tempDF['sample_Value_Numeric'].notna()]
        tempDF = tempDF[tempDF['F_SYSTEM'].isin([4,6]) | (tempDF['F_SYSTEM']==5)]
        tempDF = tempDF[(tempDF['URBAN_CODE'] < 99999) | (tempDF['F_SYSTEM'] == 5)]
        # tempDF = tempDF[tempDF['PSR_VALUE_TEXT'].isna()]
        self.df['SJF50'].iloc[tempDF.index.tolist()] = False   

        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['IRI'].isna()]
        tempDF = tempDF[tempDF['FACILITY_TYPE'].isin([1,2])]
        tempDF = tempDF[tempDF['SURFACE_TYPE'] > 1] 
        tempDF = tempDF[tempDF['F_SYSTEM'] == 1 | tempDF['NHS'].notna()]
        # tempDF = tempDF[tempDF['PSR_VALUE_TEXT'] == 'A']
        # tempDF = tempDF[tempDF['PSR'].isna()]
        self.df['SJF50'].iloc[tempDF.index.tolist()] = False   

    def sjf51(self):
        #SURACE_TYPE|"SURFACE_TYPE ValueNumeric Must Exist Where FACILITY_TYPE in (1;2) AND (F_SYSTEM = 1 OR NHS ValueNumeric <> NULL OR Sample exists) OR DIR_THROUGH_LANES >0 AND (IRI IS NOT NULL OR PSR IS NOT NULL) "
        self.df['SJF51'] = True
        tmp_df = self.df.copy(deep = True)
        tmp_df = tmp_df[tmp_df['FACILITY_TYPE'].isin([1,2])]
        tmp_df = tmp_df[(tmp_df['F_SYSTEM']==1) | (tmp_df['NHS']!=np.nan) | (tmp_df['sample_Value_Numeric']!=np.nan)]
        tmp_df = tmp_df[tmp_df['SURFACE_TYPE'].isna()]
        self.df['SJF51'].iloc[tmp_df.index.tolist()] = False


    
    def sjf52(self):
        #RUTTING|"RUTTING ValueNumeric Must Exist Where SURFACE_TYPE in (2;6;7;8) AND (FACILITY_TYPE in (1;2) AND (F_SYSTEM = 1 OR NHS OR Sample) OR DIR_THROUGH_LANES >0 AND (IRI IS NOT NULL OR PSR IS NOT NULL))
        self.df['SJF52'] = True
        tmp_df = self.df.copy()
        tmp_df = tmp_df[tmp_df['SURFACE_TYPE'].isin([2,6,7,8])]
        tmp_df = tmp_df[tmp_df['FACILITY_TYPE'].isin([1,2])]
        tmp_df = tmp_df[(tmp_df['F_SYSTEM']==1) | (tmp_df['NHS'].notna()) | (tmp_df['sample_Value_Numeric'].notna()) |(tmp_df['DIR_THROUGH_LANES']>0)]
        tmp_df = tmp_df[tmp_df['RUTTING'].isna()]
        self.df['SJF52'].iloc[tmp_df.index.tolist()] = False
    
    def sjf53(self):
        # Faulting ValueNumeric Must Exist Where SURFACE_TYPE in (3;4;9;10) AND (FACILITY_TYPE in (1;2)  AND  (F_SYSTEM = 1 OR NHS OR Sample) OR  DIR_THROUGH_LANES >0 AND (IRI IS NOT NULL OR PSR IS NOT NULL))
        self.df['SJF53'] = True
        tmp_df = self.df.copy()
        tmp_df = tmp_df[tmp_df['SURFACE_TYPE'].isin([3,4,9,10])]
        tmp_df = tmp_df[tmp_df['FACILITY_TYPE'].isin([1,2])]
        tmp_df = tmp_df[(tmp_df['F_SYSTEM']==1) | (tmp_df['NHS'].notna()) | (tmp_df['sample_Value_Numeric'].notna()) |(tmp_df['DIR_THROUGH_LANES']>0)]
        tmp_df = tmp_df[tmp_df['FAULTING'].isna()]
        self.df['SJF53'].iloc[tmp_df.index.tolist()] = False
    
    def sjf54(self):
        # SURFACE_TYPE in (2;3;4;5;6;7;8;9;10) AND (FACILITY_TYPE in (1;2) AND (F_SYSTEM = 1 OR  NHS  OR Sample) OR (DIR_THROUGH_LANES >0 AND (IRI IS NOT NULL OR PSR IS NOT NULL)))
        self.df['SJF54'] = True
        tmp_df = self.df.copy()
        tmp_df = tmp_df[tmp_df['SURFACE_TYPE'].isin([2,3,4,5,6,7,8,9,10])]
        tmp_df = tmp_df[tmp_df['FACILITY_TYPE'].isin([1,2])]
        tmp_df = tmp_df[(tmp_df['F_SYSTEM']==1) | (tmp_df['NHS'].notna()) | (tmp_df['sample_Value_Numeric'].notna()) |(tmp_df['DIR_THROUGH_LANES']>0)]
        tmp_df = tmp_df[tmp_df['CRACKING_PERCENT'].isna()]
        self.df['SJF54'].iloc[tmp_df.index.tolist()] = False

    def sjf55(self):
        # YEAR_LAST_IMPROVEMENT must exist on Samples where SURFACE_TYPE is in the range (2;3;4;5;6;7;8;9;10) OR where  (YEAR_LAST_CONSTRUCTION < BeginDate Year - 20)
        self.df['SJF55'] = True
        tmp_df = self.df.copy()
        tmp_df['BEGIN_DATE'] = pd.to_datetime(tmp_df['BEGIN_DATE'], '%m/%d/%Y')
        tmp_df['YEAR_LAST_CONSTRUCTION'] = pd.to_datetime(tmp_df['YEAR_LAST_CONSTRUCTION'], '%Y')
        tmp_df = tmp_df[(tmp_df['SURFACE_TYPE'].isin([2,3,4,5,6,7,8,9,10]))|(tmp_df['YEAR_LAST_CONSTRUCTION']< (tmp_df['BEGIN_DATE'] - relativedelta(years=20))) ]
        tmp_df = tmp_df[tmp_df['YEAR_LAST_IMPROVEMENT'].isna()]
        self.df['SJF55'].iloc[tmp_df.index.tolist()] = False

    def sjf56(self):
        # YEAR_LAST_CONSTRUCTION	YEAR_LAST_CONSTRUCTION must exist on Samples where SURFACE_TYPE is in the range (2;3;4;5;6;7;8;9;10)
        self.df['SJF56'] = True
        tmp_df = self.df.copy()
        tmp_df = tmp_df[tmp_df['SURFACE_TYPE'].isin([2,3,4,5,6,7,8,9,10])]
        tmp_df = tmp_df[tmp_df['sample_Value_Numeric'].notna()]
        tmp_df = tmp_df[tmp_df['YEAR_LAST_CONSTRUCTION'].isna()]
        self.df['SJF56'].iloc[tmp_df.index.tolist()] = False

    def sjf57(self):
        # LAST_OVERLAY_THICKNESS	Sample and YEAR_LAST_IMPROVEMENT exists	
        self.df['SJF57'] = True
        tmp_df = self.df.copy()
        tmp_df = tmp_df[tmp_df['sample_Value_Numeric'].notna()]
        tmp_df = tmp_df[tmp_df['YEAR_LAST_IMPROVEMENT'].notna()]
        tmp_df = tmp_df[tmp_df['LAST_OVERLAY_THICKNESS'].isna()]
        self.df['SJF57'].iloc[tmp_df.index.tolist()] = False

    def sjf58(self):
        # THICKNESS_RIGID	SURFACE_TYPE (3;4;5;7;8;9;10) and Sample
        self.df['SJF58'] = True
        tmp_df = self.df.copy()
        tmp_df = tmp_df[tmp_df['SURFACE_TYPE'].isin([3,4,5,7,8,9,10])]
        tmp_df = tmp_df[tmp_df['sample_Value_Numeric'].notna()]
        tmp_df = tmp_df[tmp_df['THICKNESS_RIGID'].isna()]
        self.df['SJF58'].iloc[tmp_df.index.tolist()] = False



    
    
    
    
    def run(self):
        #when it returns True, it means the data has no errors itself
        # self.sjf01()
        # self.sjf02()
        # self.sjf03()
        # self.sjf04()
        # self.sjf05()
        # self.sjf06()
        # self.sjf07()
        # self.sjf08()
        # self.sjf09()
        # self.sjf10()
        # self.sjf11()
        # self.sjf12()
        # self.sjf13()
        self.sjf14()
        self.sjf15()
        self.sjf16()
        self.sjf17()
        self.sjf18()
        self.sjf19()
        self.sjf20()
        self.sjf21()
        self.sjf22()
        self.sjf23()
        self.sjf24()
        self.sjf25()
        self.sjf26()
        # self.sjf27()
        # self.sjf28()
        # self.sjf29()
        # self.sjf30()
        # self.sjf31()
        # self.sjf32()
        # self.sjf33()
        # self.sjf34()
        # self.sjf35()
        # self.sjf36()
        # self.sjf37()
        # self.sjf38()
        # self.sjf39()
        # self.sjf40()
        # self.sjf41()
        # self.sjf42()
        # self.sjf43()
        # self.sjf44()
        # self.sjf45()
        # self.sjf46()
        # self.sjf47()
        # self.sjf48()
        # self.sjf49()
        # self.sjf50()
        # self.sjf51()
        # self.sjf52()
        # self.sjf53()
        # self.sjf54()
        # self.sjf55()
        # self.sjf56()
        # self.sjf57()
        # self.sjf58()
        # self.sjf59()
        # self.sjf60()
        # self.sjf61()
        # self.sjf62()
        # self.sjf63()
        # self.sjf64()
        # self.sjf65()
        # self.sjf66()
        # self.sjf67()
        # self.sjf68()
        # self.sjf69()
        # self.sjf70()
        # self.sjf71()
        # self.sjf72()
        # self.sjf73()
        # self.sjf74()
        # self.sjf75()
        # self.sjf76()
        # self.sjf77()
        # self.sjf78()
        # self.sjf79()
        # self.sjf80()
        # self.sjf81()
        # self.sjf82()
        # self.sjf82b()
        # self.sjf82c()
        # self.sjf82d()
        # self.sjf83()
        # self.sjf83b()
        # self.sjf83c()
        # self.sjf83d()
        # self.sjf84()
        # self.sjf85()
        # self.sjf86()
        # self.sjf87()
        # self.sjf88()
        # self.sjf89()
        # self.sjf90()
        # self.sjf91()
        # self.sjf92()
        # self.sjf93()
        # self.sjf94()
        # self.sjf96()
        # self.sjf96()
        # self.sjf97()
        # self.sjf98()
        # self.sjf99()
        # self.sjf100()





df = pd.read_excel('test_data_sjf_newest.xlsx')

c = full_spatial_functions(df)  
# c.run()
c.run()
print(c.df)

c.df.to_excel('test_functions_matt_sucks.xlsx', index=False)

import pandas as pd
from datetime import datetime,timedelta
from dateutil.relativedelta import relativedelta
import numpy as np
import json
import warnings
import copy
import string
import time
import os
from combine_items import combine_errors
s = time.time()
# import combine_items
warnings.filterwarnings("ignore")
debug = int('0' if os.getenv("DEBUG") is None else os.getenv("DEBUG"))==1

def combine_df(df,columns=[]):
    df.to_csv('tmp.csv',index=False)
    colstr = ','.join(columns)
    cmd = f'lrsops combine -f tmp.csv -u "{colstr}" -o tmp2.csv'
    os.system(cmd)
    df = pd.read_csv('tmp2.csv',dtype={'RouteID':str,'URBAN_CODE':str,'HPMS_SAMPLE_NO':str})
    if 'HPMS_SAMPLE_NO' in df.columns:
        df['HPMS_SAMPLE_NO'] = df['HPMS_SAMPLE_NO'].map(lambda x: str(int(float(x))) if '+' in str(x) else x)
    return df 

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

# reading in rule information
rules = pd.read_excel('new_rules_used.xlsx')
rules['UsedColumns'] = rules.UsedColumns.map(lambda x: json.loads(x.replace("'", '"').replace("URBAN_ID",'URBAN_CODE')))
rules = rules[(rules.FileName == 'Spatial Join Full')&(rules['Data Item Name'] !='STRUCTURE_TYPE')]
rules_col_used = rules.set_index('Rule')['UsedColumns'].to_dict()
rules_description = rules.set_index('Rule')['Message'].to_dict()
rules_name = rules.set_index('Rule')['Data Item Name'].to_dict()


class FullSpatial():
    def __init__(self,df):
        self.error_df = pd.DataFrame()
        self.df = df
        self.rules = {}
        self.rules_list = []
        self.get_rulenames()

    def get_rulenames(self):
        method_list = [func for func in dir(self) if callable(getattr(self, func))]
        method_list = [i for i in method_list if not '__' in i and 'sjf' in i]
        for i in method_list[:]:
                rule_name = i.upper()
                if rule_name[-1].isalpha():
                    rule_name = rule_name[:-1] + rule_name[-1].lower()

                myfunc = getattr(self,i)
                col_used = rules_col_used.get(rule_name)
                self.rules[rule_name] = [myfunc,col_used]
                self.rules_list.append(rule_name)
    
    def check_col_can_run(self,col_used):
        if col_used is None:
            return True,''
        for col in col_used:
            if not col in self.df.columns:
                return False,col
        return True,''

    def run_rules(self):
        for k,[myfunc,col_used] in self.rules.items():
            bv,failed_col = self.check_col_can_run(col_used)
            if not bv:
                print(f'Could not run {k} missing the column: {failed_col}')
            else:
                myfunc()

    def consolidate_errors_rules(self):
        '''
        Logically for every error that has an a,b,c,d 
        Adds one column representing that with ors  
        '''
        real_dict = {}
        for k in self.rules.keys():
            if k[-1].isalpha(): 
                real_rule = k[:-1]
                col_list = real_dict.get(real_rule,[])
                col_list.append(k)
                real_dict[real_rule] = col_list
            else: 
                pass 
        
        for k,v in real_dict.items():
            self.df[k] = self.df[v].all(axis=1)


    def conflate_fhwa(self):
        df = self.df.copy()
        fhwa_cols = [i for i in df.columns if 'SJF' in i and '_FHWA' in i]
        newlist = []
        for col in fhwa_cols:
            nor_col = col.replace('_FHWA','')
            if nor_col in df.columns:
                newlist.append([nor_col,len(df[~df[col]]),len(df[~df[nor_col]])])
                # print(nor_col,len(df[~df[col]]),len(df[~df[nor_col]]))
        summ = pd.DataFrame(newlist,columns=['Rule','FHWA Num Rows','Our Num Rows'])
        return summ 


    def sjf01(self):
        #F_SYSTEM must exist where FACILITY_TYPE is in (1;2;4;5;6) and Must not be NULL
        print("Running rule SJF01...")
        self.df['SJF01'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['FACILITY_TYPE'].isin([1,2,4,5,6]) & tempDF['F_SYSTEM'].isna()]
        self.df['SJF01'].iloc[tempDF.index.tolist()] = False

    def sjf02(self):
        #URBAN_CODE must exist and must not be NULL where: 1. FACILITY_TYPE in (1;2;4) AND F_SYSTEM in (1;2;3;4;5) 
        # [OR] 2. FACILITY_TYPE = 6 AND DIR_THROUGH_LANES > 0 and F_SYSTEM = 1 AND (IRI IS NOT NULL OR PSR IS NOT NULL)
        print("Running rule SJF02...")
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
        print("Running rule SJF03...")
        self.df['SJF03'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['F_SYSTEM'].isin(range(1,8)) & tempDF['FACILITY_TYPE'].isna()]
        self.df['SJF03'].iloc[tempDF.index.tolist()] = False

    def sjf04(self):
        #No validation
        print("Running rule SJF04...")
        # self.df['SJF04'] = True

    def sjf05(self):
        #ACCESS_CONTROL must exist where (F_SYSTEM in (1;2;3) or Sample or NHS) AND FACILITY_TYPE IN (1;2) and must not be NULL
        print("Running rule SJF05...")
        self.df['SJF05'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['F_SYSTEM'].isin([1,2,3]) | tempDF['HPMS_SAMPLE_NO'].notna() | tempDF['NHS'].notna()]
        tempDF = tempDF[tempDF['FACILITY_TYPE'].isin([1,2])]
        tempDF = tempDF[tempDF['ACCESS_CONTROL'].isna()]
        self.df['SJF05'].iloc[tempDF.index.tolist()] = False


    def sjf06(self):
        #OWNERSHIP must exist where (F_SYSTEM in (1;2;3;4;5;6;7) and FACILITY_TYPE (1;2;5;6) and must not be NULL
        print("Running rule SJF06...")
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
        print("Running rule SJF07...")
        self.df['SJF07'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['FACILITY_TYPE'].isin([1,2,4])]
        tempDF = tempDF[tempDF['F_SYSTEM'].isin(range(1,6)) | (tempDF['F_SYSTEM'] == 6) | tempDF['NHS'].notna()]
        tempDF = tempDF[tempDF['F_SYSTEM'].isin(range(1,6)) | (tempDF['URBAN_CODE'].astype(float) < 99999) | tempDF['NHS'].notna()]
        tempDF = tempDF[tempDF['THROUGH_LANES'].isna()]
        self.df['SJF07'].iloc[tempDF.index.tolist()] = False

    def sjf08(self):
        #MANAGED_LANES_TYPE must exist where MANAGED_LANES is not Null
        print("  rule SJF08...")
        # self.df['SJF08'] = True
        # tempDF = self.df.copy()
        # tempDF[tempDF['MANAGED_LANES'].notna() & tempDF['MANAGED_LANES_TYPE'].isna()]
        # self.df['SJF08'].iloc[tempDF.index.tolist()] = False

    def sjf09(self):
        #MANAGED_LANES must exist where MANAGED_LANES_TYPE is not Null
        print("Running rule SJF09...")
        # self.df['SJF09'] = True
        # tempDF = self.df.copy()
        # tempDF[tempDF['MANAGED_LANES_TYPE'].notna() & tempDF['MANAGED_LANES'].isna()]
        # self.df['SJF09'].iloc[tempDF.index.tolist()] = False

    def sjf10(self):
        #PEAK_LANES must exist on Samples
        print("Running rule SJF10...")
        self.df['SJF10'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['HPMS_SAMPLE_NO'].notna() & tempDF['PEAK_LANES'].isna()]
        self.df['SJF10'].iloc[tempDF.index.tolist()] = False    

    def sjf11(self):
        #COUNTER_PEAK_LANES must exist on Samples where FACILITY_TYPE = 2 AND (URBAN_CODE < 99999 OR THROUGH_LANES >=4)
        print("Running rule SJF11...")
        self.df['SJF11'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['HPMS_SAMPLE_NO'].notna()]
        tempDF = tempDF[tempDF['FACILITY_TYPE']==2]
        tempDF = tempDF[(tempDF['URBAN_CODE'].astype(float)<99999) | (tempDF['THROUGH_LANES'] >= 4)]
        tempDF = tempDF[tempDF['COUNTER_PEAK_LANES'].isna()]
        self.df['SJF11'].iloc[tempDF.index.tolist()] = False

    def sjf12(self):
        #TURN_LANES_R must exist on Samples where URBAN_CODE  < 99999 and ACCESS_CONTROL >1
        print("Running rule SJF12...")
        self.df['SJF12'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['HPMS_SAMPLE_NO'].notna()]
        tempDF = tempDF[(tempDF['URBAN_CODE'].astype(float) < 99999) & (tempDF['ACCESS_CONTROL'] > 1)]
        tempDF = tempDF[tempDF['TURN_LANES_R'].isna()]
        self.df['SJF12'].iloc[tempDF.index.tolist()] = False

    def sjf13(self):
        #TURN_LANES_L must exist on Samples where URBAN_CODE  < 99999 and ACCESS_CONTROL >1
        print("Running rule SJF13...")
        self.df['SJF13'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['HPMS_SAMPLE_NO'].notna()]
        tempDF = tempDF[(tempDF['URBAN_CODE'].astype(float) < 99999) & (tempDF['ACCESS_CONTROL'] > 1)]
        tempDF = tempDF[tempDF['TURN_LANES_L'].isna()]
        self.df['SJF13'].iloc[tempDF.index.tolist()] = False

    def sjf14(self):
        #SPEED_LIMIT must exist on Samples and  the NHS
        print("Running rule SJF14...")
        self.df['SJF14'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['HPMS_SAMPLE_NO'].notna() | tempDF['NHS'].notna()]
        tempDF = tempDF[tempDF['SPEED_LIMIT'].isna()]
        self.df['SJF14'].iloc[tempDF.index.tolist()] = False

    def sjf15(self):
        #No validation
        print("Running rule SJF15...")
        # self.df['SJF15'] = True

    def sjf16(self):
        #ROUTE_NUMBER ValueNumeric Must Exist where (F_SYSTEM in (1;2;3;4) or NHS ValueNumeric <> NULL ) and FACILITY_TYPE (1;2) and ROUTE_SIGNING in (2;3;4;5;6;7;8;9)  
        # OR F_SYSTEM=1 AND FACILITY_TYPE=6 AND DIR_THROUGH_LANES > 0 AND (IRI IS NOT NULL OR PSR IS NOT NULL)
        print("Running rule SJF16...")
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
        print("Running rule SJF17...")
        self.df['SJF17'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['F_SYSTEM'].isin([1,2,3,4]) | tempDF['NHS'].notna()]
        tempDF = tempDF[tempDF['FACILITY_TYPE'].isin([1,2])]
        tempDF = tempDF[tempDF['ROUTE_SIGNING'].isna()]
        self.df['SJF17'].iloc[tempDF.index.tolist()] = False   

    def sjf18(self):
        #ROUTE_QUALIFIER must exist where (F_SYSTEM in (1;2;3;4) or NHS) and FACILITY_TYPE (1;2)
        print("Running rule SJF18...")
        self.df['SJF18'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['F_SYSTEM'].isin([1,2,3,4]) | tempDF['NHS'].notna()]
        tempDF = tempDF[tempDF['FACILITY_TYPE'].isin([1,2])]
        tempDF = tempDF[tempDF['ROUTE_QUALIFIER'].isna()]
        self.df['SJF18'].iloc[tempDF.index.tolist()] = False   

    def sjf19(self):
        #ROUTE_NAME must exist where (F_SYSTEM in (1;2;3;4) or NHS) and FACILITY_TYPE (1;2)
        print("Running rule SJF19...")
        self.df['SJF19'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['F_SYSTEM'].isin([1,2,3,4]) | tempDF['NHS'].notna()]
        tempDF = tempDF[tempDF['FACILITY_TYPE'].isin([1,2])]
        tempDF = tempDF[tempDF['ROUTE_NAME_VALUE_TEXT'].isna()]
        self.df['SJF19'].iloc[tempDF.index.tolist()] = False

    def sjf20(self):    
        #AADT must exist WHERE: (FACILITY_TYPE in (1;2;4) AND (F_SYSTEM in (1;2;3;4;5)) OR (F_SYSTEM = 6 and URBAN_CODE  <99999) OR NHS ValueNumeric <> NULL
        print("Running rule SJF20...")
        self.df['SJF20'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['FACILITY_TYPE'].isin([1,2,4])]
        tempDF = tempDF[tempDF['F_SYSTEM'].isin(range(1,6))]
        tempDF = tempDF[tempDF['AADT'].isna()]
        self.df['SJF20'].iloc[tempDF.index.tolist()] = False

        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['FACILITY_TYPE'].isin([1,2,4])]
        tempDF = tempDF[(tempDF['F_SYSTEM']==6)]
        tempDF = tempDF[(tempDF['URBAN_CODE'].astype(float) < 99999)]
        tempDF = tempDF[tempDF['AADT'].isna()]
        self.df['SJF20'].iloc[tempDF.index.tolist()] = False

        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['FACILITY_TYPE'].isin([1,2,4])]
        tempDF = tempDF[tempDF['NHS'].notna()]
        tempDF = tempDF[tempDF['AADT'].isna()]
        self.df['SJF20'].iloc[tempDF.index.tolist()] = False



    def sjf21(self):
        #AADT_SINGLE_UNIT must exist WHERE ((F_SYSTEM in (1) or NHS ValueNumeric <> NULL) and FACILITY_TYPE (1;2)) and on Samples
        print("Running rule SJF21...")
        self.df['SJF21'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['HPMS_SAMPLE_NO'].notna()]
        tempDF = tempDF[tempDF['AADT_SINGLE_UNIT'].isna()]
        self.df['SJF21'].iloc[tempDF.index.tolist()] = False
        
        tempDF = self.df.copy() 
        tempDF = tempDF[(tempDF['F_SYSTEM']==1) | tempDF['NHS'].notna()]
        tempDF = tempDF[tempDF['FACILITY_TYPE'].isin([1,2])]
        tempDF = tempDF[tempDF['AADT_SINGLE_UNIT'].isna()]
        self.df['SJF21'].iloc[tempDF.index.tolist()] = False

    def sjf22(self):
        #PCT_DH_SINGLE_UNIT must exist on Samples
        print("Running rule SJF22...")
        self.df['SJF22'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['HPMS_SAMPLE_NO'].notna()]
        tempDF = tempDF[tempDF['PCT_DH_SINGLE_UNIT'].isna()]
        self.df['SJF22'].iloc[tempDF.index.tolist()] = False

    def sjf23(self):
        #AADT_COMBINATION must exist WHERE ((F_SYSTEM in (1) or NHS ValueNumeric <> NULL) and FACILITY_TYPE (1;2)) and on Samples
        print("Running rule SJF23...")
        self.df['SJF23'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['HPMS_SAMPLE_NO'].notna()]
        tempDF = tempDF[tempDF['AADT_COMBINATION'].isna()]
        self.df['SJF23'].iloc[tempDF.index.tolist()] = False
        
        tempDF = self.df.copy() 
        tempDF = tempDF[(tempDF['F_SYSTEM']==1) | tempDF['NHS'].notna()]
        tempDF = tempDF[tempDF['FACILITY_TYPE'].isin([1,2])]
        tempDF = tempDF[tempDF['AADT_COMBINATION'].isna()]
        self.df['SJF23'].iloc[tempDF.index.tolist()] = False

    def sjf24(self):
        #PCT_DH_COMBINATION must exist on Samples
        print("Running rule SJF24...")
        self.df['SJF24'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['HPMS_SAMPLE_NO'].notna() & tempDF['PCT_DH_COMBINATION'].isna()]
        self.df['SJF24'].iloc[tempDF.index.tolist()] = False

    def sjf25(self):
        #K_FACTOR must exist on Samples
        print("Running rule SJF25...")
        self.df['SJF25'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['HPMS_SAMPLE_NO'].notna() & tempDF['K_FACTOR'].isna()]
        self.df['SJF25'].iloc[tempDF.index.tolist()] = False

    def sjf26(self):
        #DIR_FACTOR must exist on Samples
        print("Running rule SJF26...")
        self.df['SJF26'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['HPMS_SAMPLE_NO'].notna() & tempDF['DIR_FACTOR'].isna()]
        self.df['SJF26'].iloc[tempDF.index.tolist()] = False

    def sjf27(self):
        #FUTURE_AADT must exist on Samples
        print("Running rule SJF27...")
        self.df['SJF27'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['HPMS_SAMPLE_NO'].notna() & tempDF['FUTURE_AADT'].isna()]
        self.df['SJF27'].iloc[tempDF.index.tolist()] = False

    def sjf28(self):
        #SIGNAL_TYPE must exist on Samples WHERE (URBAN_CODE <> 99999 AND NUMBER_SIGNALS >=1)
        print("Running rule SJF28...")
        self.df['SJF28'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['URBAN_CODE'].astype(float) != 99999]
        tempDF = tempDF[tempDF['NUMBER_SIGNALS'] >= 1]
        tempDF = tempDF[tempDF['HPMS_SAMPLE_NO'].notna()]
        tempDF = tempDF[tempDF['SIGNAL_TYPE'].isna()]
        self.df['SJF28'].iloc[tempDF.index.tolist()] = False 

    def sjf29(self):
        #PCT_GREEN_TIME must exist on Samples WHERE (NUMBER_SIGNALS >=1 AND URBAN_CODE <99999)
        print("Running rule SJF29...")
        self.df['SJF29'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['URBAN_CODE'].astype(float) < 99999]
        tempDF = tempDF[tempDF['NUMBER_SIGNALS'] >= 1]
        tempDF = tempDF[tempDF['HPMS_SAMPLE_NO'].notna()]
        tempDF = tempDF[tempDF['PCT_GREEN_TIME'].isna()]
        self.df['SJF29'].iloc[tempDF.index.tolist()] = False

    def sjf30(self):
        #NUMBER_SIGNALS must exist on Samples WHERE SIGNAL_TYPE IN (1;2;3;4)
        print("Running rule SJF30...")
        self.df['SJF30'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['SIGNAL_TYPE'].isin([1,2,3,4])]
        tempDF = tempDF[tempDF['HPMS_SAMPLE_NO'].notna()]
        tempDF = tempDF[tempDF['NUMBER_SIGNALS'].isna()]
        self.df['SJF30'].iloc[tempDF.index.tolist()] = False  

    def sjf31(self):
        #STOP_SIGNS (the number of stop sign controlled intersections) must exist on Samples
        print("Running rule SJF31...")
        self.df['SJF31'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['HPMS_SAMPLE_NO'].notna()]
        tempDF = tempDF[tempDF['STOP_SIGNS'].isna()]
        self.df['SJF31'].iloc[tempDF.index.tolist()] = False

    def sjf32(self):
        #AT_GRADE_OTHER (the number of intersections; type 'other') must exist on Samples
        print("Running rule SJF32...")
        self.df['SJF32'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['HPMS_SAMPLE_NO'].notna()]
        tempDF = tempDF[tempDF['AT_GRADE_OTHER'].isna()]
        self.df['SJF32'].iloc[tempDF.index.tolist()] = False 

    def sjf33(self):
        #LANE_WIDTH must exist on Samples
        print("Running rule SJF33...")
        self.df['SJF33'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['HPMS_SAMPLE_NO'].notna()]
        tempDF = tempDF[tempDF['LANE_WIDTH'].isna()]
        self.df['SJF33'].iloc[tempDF.index.tolist()] = False

    def sjf34(self):
        #MEDIAN_TYPE must exist on Samples
        print("Running rule SJF34...")
        self.df['SJF34'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['HPMS_SAMPLE_NO'].notna()]
        tempDF = tempDF[tempDF['MEDIAN_TYPE'].isna()]
        self.df['SJF34'].iloc[tempDF.index.tolist()] = False

    def sjf35(self):
        #MEDIAN_WIDTH must exist on Samples where MEDIAN_TYPE in (2;3;4;5;6;7)
        print("Running rule SJF35...")
        self.df['SJF35'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['HPMS_SAMPLE_NO'].notna()]
        tempDF = tempDF[tempDF['MEDIAN_TYPE'].isin(range(2,8))]
        tempDF = tempDF[tempDF['MEDIAN_WIDTH'].isna()]
        self.df['SJF35'].iloc[tempDF.index.tolist()] = False

    def sjf36(self):
        #SHOULDER_TYPE must exist on Samples
        print("Running rule SJF36...")
        self.df['SJF36'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['HPMS_SAMPLE_NO'].notna()]
        tempDF = tempDF[tempDF['SHOULDER_TYPE'].isna()]
        self.df['SJF36'].iloc[tempDF.index.tolist()] = False

    def sjf37(self):
        #SHOULDER_WIDTH_R must exist on Samples where SHOULDER_TYPE in (2;3;4;5;6)
        print("Running rule SJF37...")
        self.df['SJF37'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['HPMS_SAMPLE_NO'].notna()]
        tempDF = tempDF[tempDF['SHOULDER_TYPE'].isin(range(2,7))]
        tempDF = tempDF[tempDF['SHOULDER_WIDTH_R'].isna()]
        self.df['SJF37'].iloc[tempDF.index.tolist()] = False

    def sjf38(self):
        #SHOULDER_WIDTH_L must exist on Samples where (SHOULDER_TYPE in (2;3;4;5;6) and MEDIAN_TYPE in (2;3;4;5;6;7))
        print("Running rule SJF38...")
        self.df['SJF38'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['HPMS_SAMPLE_NO'].notna()]
        tempDF = tempDF[tempDF['SHOULDER_TYPE'].isin(range(2,7))]
        tempDF = tempDF[tempDF['MEDIAN_TYPE'].isin(range(2,8))]
        tempDF = tempDF[tempDF['SHOULDER_WIDTH_L'].isna()]
        self.df['SJF38'].iloc[tempDF.index.tolist()] = False

    def sjf39(self):
        #PEAK_PARKING must exist on Samples where URBAN_CODE < 99999
        print("Running rule SJF39...")
        self.df['SJF39'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['HPMS_SAMPLE_NO'].notna()]
        tempDF = tempDF[tempDF['URBAN_CODE'].astype(float) < 99999]
        tempDF = tempDF[tempDF['PEAK_PARKING'].isna()]
        self.df['SJF39'].iloc[tempDF.index.tolist()] = False

    def sjf40(self):
        #WIDENING_POTENTIAL must exist on Samples
        print("Running rule SJF40...")
        self.df['SJF40'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['HPMS_SAMPLE_NO'].notna()]
        tempDF = tempDF[tempDF['WIDENING_POTENTIAL'].isna()]
        self.df['SJF40'].iloc[tempDF.index.tolist()] = False

    def sjf41(self):
        #CURVES BP/EP on F_SYSTEM in (1;2;3) or F_SYSTEM = 4 and URBAN_CODE = 99999 and SURFACE_TYPE > 1 Must Align with Sample BP/EP
        #REVIEW LATER
        print("Running rule SJF41...")
        # self.df['SJF41'] = True



    def sjf42(self):
        #At least one CURVES_A-F must be coded for each Sample WHERE (F_SYSTEM in (1;2;3) or F_SYSTEM = 4 and URBAN_CODE = 99999) and SURFACE_TYPE > 1.
        print("Running rule SJF42...")
        self.df['SJF42'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['HPMS_SAMPLE_NO'].notna()]
        tempDF = tempDF[tempDF['F_SYSTEM'].isin([1,2,3]) | (tempDF['F_SYSTEM'] == 4)]
        tempDF = tempDF[tempDF['F_SYSTEM'].isin([1,2,3]) | (tempDF['URBAN_CODE'].astype(float) == 99999)]
        tempDF = tempDF[tempDF['SURFACE_TYPE'] > 1]
        tempDF = tempDF[tempDF['CURVES_F'].isna()]
        self.df['SJF42'].iloc[tempDF.index.tolist()] = False

    # def sjf43(self):
    #     #Sum Length (CURVES_A + CURVES_B + CURVES_C + CURVES_D + CURVES_E + CURVES_E) Must Equal to the Sample Length on 
    #     # (Sample and (F_SYSTEM (1;2;3) or (F_SYSTEM = 4 and URBAN_CODE = 99999)))

    #     #ENTIRE column ('SJF43') should either be True or False as this rule does not check individual rows
    #     print("Running rule SJF43...")
    #     tempDF = self.df.copy()
    #     # tempDF = tempDF[tempDF['CURVES_F'].notna()]
    #     tempDF['CURVE_LEN'] = round(tempDF['EMP'] - tempDF['BMP'], 3)
    #     curve_len_sum = tempDF['CURVE_LEN'].sum()

    #     tempDF = tempDF[tempDF['HPMS_SAMPLE_NO'].notna()]
    #     tempDF = tempDF[tempDF['F_SYSTEM'].isin([1,2,3]) | ((tempDF['F_SYSTEM'] == 4)&(tempDF['URBAN_CODE'].astype(float) == 99999))]

        
    #     sample_len = tempDF.groupby("HPMS_SAMPLE_NO")['CURVE_LEN'].sum().to_dict()
    #     tempDF['sample_length'] = tempDF.HPMS_SAMPLE_NO.map(lambda x:sample_len.get(x,''))
    #     # print(tempDF[['sample_length','HPMS_SAMPLE_NO']])
    #     curves = ['CURVES_A','CURVES_B','CURVES_C','CURVES_D','CURVES_E','RouteID','BMP','EMP']
    #     # print(tempDF.groupby('HPMS_SAMPLE_NO')[curves].first())
    #     eh = tempDF.groupby('HPMS_SAMPLE_NO').agg({'BMP': ['min'], 'EMP': ['max']})
    #     eh.columns = ['bmp','emp']
    #     tempDF['curve_sum'] = tempDF[curves].sum(axis=1)
    #     tempDF['SAMPLE_BEG'] = tempDF['HPMS_SAMPLE_NO'].map(lambda x:eh.loc[x].bmp) 
    #     tempDF['SAMPLE_END'] = tempDF['HPMS_SAMPLE_NO'].map(lambda x:eh.loc[x].emp) 
    #     print((tempDF['SAMPLE_END']-tempDF['SAMPLE_BEG']),tempDF[curves].sum(axis=1))
    #     print(tempDF[['sample_length','curve_sum']])

    #     # print(tempDF[curves].sum(axis=1),tempDF['sample_length'],'here')


    #     # for name,tmpdf in tempDF.groupby("HPMS_SAMPLE_NO"):
    #     #     print(tmpdf[['CURVES_A','CURVES_B','CURVES_C','CURVES_D','CURVES_E','RouteID','BMP','EMP']])

    #     # tempDF = tempDF[tempDF['F_SYSTEM'].isin([1,2,3]) | (tempDF['URBAN_CODE'].astype(float) == 99999)]
    #     tempDF['SAMPLE_LEN'] = round(tempDF['EMP'] - tempDF['BMP'], 3)
    #     sample_len_sum = tempDF['SAMPLE_LEN'].sum()

    #     if curve_len_sum == sample_len_sum:
    #         self.df['SJF43'] = True
    #     else:
    #         self.df['SJF43'] = False 

    def sjf44(self):
        #TERRAIN_TYPE must exist on Samples WHERE (URBAN_CODE = 99999 AND F_SYSTEM in (1;2;3;4;5))
        print("Running rule SJF44...")
        self.df['SJF44'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['HPMS_SAMPLE_NO'].notna()]
        tempDF = tempDF[tempDF['URBAN_CODE'].astype(float)==99999]
        tempDF = tempDF[tempDF['F_SYSTEM'].isin([1,2,3,4,5])]
        tempDF = tempDF[tempDF['TERRAIN_TYPE'].isna()]
        self.df['SJF44'].iloc[tempDF.index.tolist()] = False

    def sjf45(self):
        #GRADES BP/EP on F_SYSTEM in (1;2;3) or F_SYSTEM = 4 and URBAN_CODE = 99999 and SURFACE_TYPE > 1 Must Align with Sample BP/EP
        #REVIEW LATER
        print("Running rule SJF45...")
        self.df['SJF45'] = True

    def sjf46(self):
        #At least one GRADES_A-F must be coded for each Sample WHERE F_SYSTEM in (1;2;3) or F_SYSTEM = 4 and URBAN_CODE = 99999 and SURFACE_TYPE > 1.
        print("Running rule SJF46...")
        self.df['SJF46'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['HPMS_SAMPLE_NO'].notna()]
        tempDF = tempDF[tempDF['F_SYSTEM'].isin([1,2,3]) | (tempDF['F_SYSTEM'] == 4)]
        tempDF = tempDF[tempDF['F_SYSTEM'].isin([1,2,3]) | (tempDF['URBAN_CODE'].astype(float) == 99999)]
        tempDF = tempDF[tempDF['SURFACE_TYPE'] > 1]
        tempDF = tempDF[tempDF['GRADES_F'].isna()]
        self.df['SJF46'].iloc[tempDF.index.tolist()] = False

    def sjf47(self):
        #Sum Length (GRADES_A + GRADES_B + GRADES_C + GRADES_D + GRADES_E + GRADES_E) Must Equal to the Sample Length on (Sample and (F_SYSTEM (1;2;3) or (F_SYSTEM = 4 and URBAN_CODE = 99999)))
        #ENTIRE column (SJF47) should be either TRUE or False as this rule does not check individual rows
        print("Running rule SJF47...")
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['GRADES_F'].notna()]
        tempDF['GRADE_LEN'] = round(tempDF['EMP'] - tempDF['BMP'], 3)
        grade_len_sum = tempDF['GRADE_LEN'].sum()

        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['HPMS_SAMPLE_NO'].notna()]
        tempDF = tempDF[tempDF['F_SYSTEM'].isin([1,2,3]) | (tempDF['F_SYSTEM'] == 4)]
        tempDF = tempDF[tempDF['F_SYSTEM'].isin([1,2,3]) | (tempDF['URBAN_CODE'].astype(float) == 99999)]
        tempDF['SAMPLE_LEN'] = round(tempDF['EMP'] - tempDF['BMP'], 3)
        sample_len_sum = tempDF['SAMPLE_LEN'].sum()

        if sample_len_sum == grade_len_sum:
            self.df['SJF47'] = True
        else:
            self.df['SJF47'] = False


    def sjf48(self):
        #PCT_PASS_SIGHT must exist on Samples WHERE: (URBAN_CODE = 99999 and THROUGH_LANES =2 and MEDIAN_TYPE in (1;2) and SURFACE_TYPE > 1)
        print("Running rule SJF48...")
        self.df['SJF48'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['HPMS_SAMPLE_NO'].notna()]
        tempDF = tempDF[tempDF['URBAN_CODE'].astype(float)==99999]
        tempDF = tempDF[tempDF['THROUGH_LANES'] == 2]
        tempDF = tempDF[tempDF['MEDIAN_TYPE'].isin([1,2])]
        tempDF = tempDF[tempDF['SURFACE_TYPE'] > 1]
        tempDF = tempDF[tempDF['PCT_PASS_SIGHT'].isna()]
        self.df['SJF48'].iloc[tempDF.index.tolist()] = False

    def sjf49(self):
        #IRI ValueNumeric Must Exist Where SURFACE_TYPE >1 AND (FACILITY_TYPE IN (1;2) AND (PSR ValueText <> 'A' AND (F_SYSTEM in (1;2;3) OR NHS ValueNumeric <>1) 
        # OR Sample sections WHERE (F_SYSTEM = 4 and URBAN_CODE = 99999)OR DIR_THROUGH_LANES >0
        print("Running rule SJF49...")
        self.df['SJF49'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['SURFACE_TYPE'] > 1]
        tempDF = tempDF[tempDF['FACILITY_TYPE'].isin([1,2])]
        tempDF = tempDF[tempDF['F_SYSTEM'].isin([1,2,3]) | (tempDF['NHS'] != 1)]
        # tempDF = tempDF[tempDF['PSR_VALUE_TEXT'] != 'A']
        tempDF = tempDF[tempDF['IRI'].isna()]
        self.df['SJF49'].iloc[tempDF.index.tolist()] = False  

        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['HPMS_SAMPLE_NO'].notna()]
        tempDF = tempDF[(tempDF['F_SYSTEM'] == 4) | (tempDF['DIR_THROUGH_LANES'] > 0)]
        tempDF = tempDF[(tempDF['URBAN_CODE'].astype(float) == 99999) | (tempDF['DIR_THROUGH_LANES'] > 0)]
        tempDF = tempDF[tempDF['IRI'].isna()]
        self.df['SJF49'].iloc[tempDF.index.tolist()] = False  

    def sjf50(self):
        #PSR ValueNumeric Must Exist Where IRI ValueNumeric IS NULL AND FACILITY_TYPE IN (1;2) AND SURFACE_TYPE >1 
        # AND(Sample exists AND (F_SYSTEM in (4;6) AND URBAN_CODE <99999 OR F_SYSTEM = 5) 
        # OR (F_SYSTEM = 1 or NHS ValueNumeric <>NULL) AND PSR ValueText = ‘A’)
        print("Running rule SJF50...")

        #PSR NOT CHECKED
        # self.df['SJF50'] = True
        # tempDF = self.df.copy()
        # tempDF = tempDF[tempDF['IRI'].isna()]
        # tempDF = tempDF[tempDF['FACILITY_TYPE'].isin([1,2])]
        # tempDF = tempDF[tempDF['SURFACE_TYPE'] > 1]
        # tempDF = tempDF[tempDF['HPMS_SAMPLE_NO'].notna()]
        # tempDF = tempDF[tempDF['F_SYSTEM'].isin([4,6]) | (tempDF['F_SYSTEM']==5)]
        # tempDF = tempDF[(tempDF['URBAN_CODE'] < 99999) | (tempDF['F_SYSTEM'] == 5)]
        # # tempDF = tempDF[tempDF['PSR_VALUE_TEXT'].isna()]
        # self.df['SJF50'].iloc[tempDF.index.tolist()] = False   

        # tempDF = self.df.copy()
        # tempDF = tempDF[tempDF['IRI'].isna()]
        # tempDF = tempDF[tempDF['FACILITY_TYPE'].isin([1,2])]
        # tempDF = tempDF[tempDF['SURFACE_TYPE'] > 1] 
        # tempDF = tempDF[tempDF['F_SYSTEM'] == 1 | tempDF['NHS'].notna()]
        # # tempDF = tempDF[tempDF['PSR_VALUE_TEXT'] == 'A']
        # # tempDF = tempDF[tempDF['PSR'].isna()]
        # self.df['SJF50'].iloc[tempDF.index.tolist()] = False 

    def sjf51(self):
        #SURACE_TYPE|"SURFACE_TYPE ValueNumeric Must Exist Where FACILITY_TYPE in (1;2) AND (F_SYSTEM = 1 OR NHS ValueNumeric <> NULL OR Sample exists) OR DIR_THROUGH_LANES >0 AND (IRI IS NOT NULL OR PSR IS NOT NULL) "
        print("Running rule SJF51...")
        self.df['SJF51'] = True
        tmp_df = self.df.copy(deep = True)
        tmp_df = tmp_df[tmp_df['FACILITY_TYPE'].isin([1,2])]
        tmp_df = tmp_df[(tmp_df['F_SYSTEM']==1) | (tmp_df['NHS']!=np.nan) | (tmp_df['HPMS_SAMPLE_NO'].notna())|(tmp_df['DIR_THROUGH_LANES']>0)]
        tmp_df = tmp_df[tmp_df['SURFACE_TYPE'].isna()]
        self.df['SJF51'].iloc[tmp_df.index.tolist()] = False


    
    def sjf52(self):
        #RUTTING|"RUTTING ValueNumeric Must Exist Where SURFACE_TYPE in (2;6;7;8) AND (FACILITY_TYPE in (1;2) AND (F_SYSTEM = 1 OR NHS OR Sample) OR DIR_THROUGH_LANES >0 AND (IRI IS NOT NULL OR PSR IS NOT NULL))
        print("Running rule SJF52...")
        self.df['SJF52'] = True
        tmp_df = self.df.copy()
        tmp_df = tmp_df[tmp_df['SURFACE_TYPE'].isin([2,6,7,8])]
        tmp_df = tmp_df[tmp_df['FACILITY_TYPE'].isin([1,2])]
        tmp_df = tmp_df[(tmp_df['F_SYSTEM']==1) | (tmp_df['NHS'].notna()) | (tmp_df['HPMS_SAMPLE_NO'].notna()) |(tmp_df['DIR_THROUGH_LANES']>0)]
        tmp_df = tmp_df[tmp_df['RUTTING'].isna()]
        self.df['SJF52'].iloc[tmp_df.index.tolist()] = False
    
    def sjf53(self):
        # Faulting ValueNumeric Must Exist Where SURFACE_TYPE in (3;4;9;10) AND (FACILITY_TYPE in (1;2)  AND  (F_SYSTEM = 1 OR NHS OR Sample) OR  DIR_THROUGH_LANES >0 AND (IRI IS NOT NULL OR PSR IS NOT NULL))
        print("Running rule SJF53...")
        self.df['SJF53'] = True
        tmp_df = self.df.copy()
        tmp_df = tmp_df[tmp_df['SURFACE_TYPE'].isin([3,4,9,10])]
        tmp_df = tmp_df[tmp_df['FACILITY_TYPE'].isin([1,2])]
        tmp_df = tmp_df[(tmp_df['F_SYSTEM']==1) | (tmp_df['NHS'].notna()) | (tmp_df['HPMS_SAMPLE_NO'].notna()) |(tmp_df['DIR_THROUGH_LANES']>0)]
        tmp_df = tmp_df[tmp_df['FAULTING'].isna()]
        self.df['SJF53'].iloc[tmp_df.index.tolist()] = False
    
    def sjf54(self):
        # SURFACE_TYPE in (2;3;4;5;6;7;8;9;10) AND (FACILITY_TYPE in (1;2) AND (F_SYSTEM = 1 OR  NHS  OR Sample) OR (DIR_THROUGH_LANES >0 AND (IRI IS NOT NULL OR PSR IS NOT NULL)))
        print("Running rule SJF54...")
        self.df['SJF54'] = True
        tmp_df = self.df.copy()
        tmp_df = tmp_df[tmp_df['SURFACE_TYPE'].isin([2,3,4,5,6,7,8,9,10])]
        tmp_df = tmp_df[tmp_df['FACILITY_TYPE'].isin([1,2])]
        tmp_df = tmp_df[(tmp_df['F_SYSTEM']==1) | (tmp_df['NHS'].notna()) | (tmp_df['HPMS_SAMPLE_NO'].notna()) |(tmp_df['DIR_THROUGH_LANES']>0)]
        tmp_df = tmp_df[tmp_df['CRACKING_PERCENT'].isna()]
        self.df['SJF54'].iloc[tmp_df.index.tolist()] = False

    def sjf55(self):
        # YEAR_LAST_IMPROVEMENT must exist on Samples where SURFACE_TYPE is in the range (2;3;4;5;6;7;8;9;10) OR where  (YEAR_LAST_CONSTRUCTION < BeginDate Year - 20)
        print("Running rule SJF55...")
        self.df['SJF55'] = True
        tmp_df = self.df.copy()
        tmp_df = tmp_df[tmp_df['HPMS_SAMPLE_NO'].notna()]
        # tmp_df['BEGIN_DATE'] = pd.to_datetime(tmp_df['BEGIN_DATE'], '%m/%d/%Y')
        # beginDate = datetime.now() - relativedelta(years=21)

        tmp_df['Begin_Date'] = pd.to_datetime(tmp_df.Begin_Date)
        beginDate = tmp_df.Begin_Date.min()

        beginDate_less_20 = datetime.strptime(str(beginDate.year), '%Y')
        tmp_df['YEAR_LAST_CONSTRUCTION_VALUE_DATE'] = pd.to_datetime(tmp_df['YEAR_LAST_CONSTRUCTION_VALUE_DATE'], format='%Y',errors='ignore')
        # tmp_df['BEGIN_20_LESS'] = tmp_df['BEGIN_DATE'].apply(lambda x: x-relativedelta(years=20))
        tmp_df = tmp_df[tmp_df['SURFACE_TYPE'].isin([2,3,4,5,6,7,8,9,10]) | (tmp_df['YEAR_LAST_CONSTRUCTION_VALUE_DATE'] < (tmp_df['Begin_Date']-pd.Timedelta(weeks=52*20)))]
        tmp_df = tmp_df[tmp_df['YEAR_LAST_IMPROVEMENT_VALUE_DATE'].isna()]
        self.df['SJF55'].iloc[tmp_df.index.tolist()] = False

    def sjf56(self):
        # YEAR_LAST_CONSTRUCTION	YEAR_LAST_CONSTRUCTION must exist on Samples where SURFACE_TYPE is in the range (2;3;4;5;6;7;8;9;10)
        print("Running rule SJF56...")
        self.df['SJF56'] = True
        tmp_df = self.df.copy()
        tmp_df = tmp_df[tmp_df['SURFACE_TYPE'].isin([2,3,4,5,6,7,8,9,10])]
        tmp_df = tmp_df[tmp_df['HPMS_SAMPLE_NO'].notna()]
        tmp_df = tmp_df[tmp_df['YEAR_LAST_CONSTRUCTION_VALUE_DATE'].isna()]
        self.df['SJF56'].iloc[tmp_df.index.tolist()] = False

    def sjf57(self):
        # LAST_OVERLAY_THICKNESS	Sample and YEAR_LAST_IMPROVEMENT exists	
        print("Running rule SJF57...")
        self.df['SJF57'] = True
        tmp_df = self.df.copy()
        tmp_df = tmp_df[tmp_df['HPMS_SAMPLE_NO'].notna()]
        tmp_df = tmp_df[tmp_df['YEAR_LAST_IMPROVEMENT_VALUE_DATE'].notna()]
        tmp_df = tmp_df[tmp_df['LAST_OVERLAY_THICKNESS'].isna()]
        self.df['SJF57'].iloc[tmp_df.index.tolist()] = False

    def sjf58(self):
        # THICKNESS_RIGID	SURFACE_TYPE (3;4;5;7;8;9;10) and Sample
        print("Running rule SJF58...")
        self.df['SJF58'] = True
        tmp_df = self.df.copy()
        tmp_df = tmp_df[tmp_df['SURFACE_TYPE'].isin([3,4,5,7,8,9,10])]
        tmp_df = tmp_df[tmp_df['HPMS_SAMPLE_NO'].notna()]
        tmp_df = tmp_df[tmp_df['THICKNESS_RIGID'].isna()]
        self.df['SJF58'].iloc[tmp_df.index.tolist()] = False
        
    def sjf59(self):
        # THICKNESS_FLEXIBLE SURFACE_TYPE (2;6;7;8) and Sample
        print("Running rule SJF59...")
        self.df['SJF59'] = True
        tmp_df = self.df.copy()
        tmp_df = tmp_df[tmp_df['SURFACE_TYPE'].isin([2,6,7,8])]
        tmp_df = tmp_df[tmp_df['HPMS_SAMPLE_NO'].notna()]
        tmp_df = tmp_df[tmp_df['THICKNESS_FLEXIBLE'].isna()]
        self.df['SJF59'].iloc[tmp_df.index.tolist()] = False

    def sjf60(self):
        # BASE_TYPE	Sample and SURFACE_TYPE >1
        print("Running rule SJF60...")
        self.df['SJF60'] = True
        tmp_df = self.df.copy()
        tmp_df = tmp_df[tmp_df['SURFACE_TYPE'] > 1]
        tmp_df = tmp_df[tmp_df['HPMS_SAMPLE_NO'].notna()]
        tmp_df = tmp_df[tmp_df['BASE_TYPE'].isna()]
        self.df['SJF60'].iloc[tmp_df.index.tolist()] = False

    def sjf61(self):
        # BASE_THICKNESS	Where BASE_TYPE >1; SURFACE_TYPE >1  and Sample
        print("Running rule SJF61...")
        self.df['SJF61'] = True
        tmp_df = self.df.copy()
        tmp_df = tmp_df[tmp_df['HPMS_SAMPLE_NO'].notna()]
        tmp_df = tmp_df[tmp_df['BASE_TYPE'].astype(float)>1]
        tmp_df = tmp_df[tmp_df['SURFACE_TYPE']>1]
        tmp_df = tmp_df[tmp_df['BASE_THICKNESS'].isna()]
        self.df['SJF61'].iloc[tmp_df.index.tolist()] = False

    def sjf62(self):
        # SOIL TYPE DO NOT REPORT
        # self.df['SJF62'] = True
        pass
    
    def sjf63(self):
        # COUNTY_ID	FACILITY_TYPE in (1;2) AND (F_SYSTEM in (1;2;3;4;5) or (F_SYSTEM = 6 and URBAN_ID <99999) or NHS
        print("Running rule SJF63...")
        self.df['SJF63'] = True
        tmp_df = self.df.copy()
        # tmp_df = tmp_df[tmp_df['FACILITY_TYPE'].isin([1,2])]
        tmp_df = tmp_df[(tmp_df['FACILITY_TYPE'].isin([1,2])) | \
            (tmp_df['F_SYSTEM'].isin([1,2,3,4,5]))|((tmp_df['F_SYSTEM']==6)&(tmp_df['URBAN_CODE'].astype(float)<99999))|(tmp_df['NHS'].notna())]
        print(len(tmp_df))
        tmp_df = tmp_df[tmp_df['COUNTY_ID'].isna()]

        self.df['SJF63'].iloc[tmp_df.index.tolist()] = False

    def sjf64(self):
        # NHS	(F_SYSTEM = 1 AND (FACILITY_TYPE  in 1;2;6))
        print("Running rule SJF64...")
        self.df['SJF64'] = True
        tmp_df = self.df.copy()
        tmp_df = tmp_df[tmp_df['F_SYSTEM']==1]
        tmp_df = tmp_df[tmp_df['FACILITY_TYPE'].isin([1,2,6])]
        tmp_df = tmp_df[tmp_df['NHS'].isna()]
        self.df['SJF64'].iloc[tmp_df.index.tolist()] = False

    def sjf65(self):
        # STRAHNET DO NOT VALIDATE
        print("Running rule SJF65...")
        # self.df['SJF65'] = True

    def sjf66(self):
        # NN DO NOT VALIDATE
        print("Running rule SJF66...")
        # self.df['SJF66'] = True

    def sjf67(self):
        # MAINTENANCE_OPERATIONS DO NOT VALIDATE
        print("Running rule SJF67...")
        # self.df['SJF67'] = True

    def sjf68(self):
        # DIR_THROUGH_LANES	F_SYSTEM =1 AND (FACILITY_TYPE = 6) AND (IRI OR PSR >0)
        print("Running rule SJF68...")
        self.df['SJF68'] = True
        tmp_df = self.df.copy()
        tmp_df = tmp_df[tmp_df['F_SYSTEM']==1]
        tmp_df = tmp_df[tmp_df['FACILITY_TYPE']==6]
        tmp_df = tmp_df[tmp_df['IRI']>0]
        tmp_df = tmp_df[tmp_df['DIR_THROUGH_LANES'].isna()]
        self.df['SJF68'].iloc[tmp_df.index.tolist()] = False

    def sjf69(self):
        # THROUGH_LANES	THROUGH_LANES must be >1 WHERE FACILITY_TYPE = 2
        print("Running rule SJF69...")
        self.df['SJF69'] = True
        tmp_df = self.df.copy()
        tmp_df = tmp_df[tmp_df['FACILITY_TYPE']==2]
        tmp_df = tmp_df[(tmp_df['THROUGH_LANES']<=1)|(tmp_df['THROUGH_LANES'].isna())]
        # tmp_df = tmp_df[tmp_df['THROUGH_LANES'].isna()]
        self.df['SJF69'].iloc[tmp_df.index.tolist()] = False

    def sjf70(self):
        # THROUGH_LANES	The sum of COUNTER_PEAK_LANES + PEAK_LANES must be >= THROUGH_LANES
        print("Running rule SJF70...")
        self.df['SJF70'] = True
        tmp_df = self.df.copy()
        tmp_df = tmp_df[tmp_df[['COUNTER_PEAK_LANES','PEAK_LANES','THROUGH_LANES']].notna().apply(lambda x:x.all(),axis=1)]
        sum = tmp_df['COUNTER_PEAK_LANES'] + tmp_df['PEAK_LANES']
        tmp_df = tmp_df[sum < tmp_df['THROUGH_LANES']]
        # tmp_df = tmp_df[tmp_df['THROUGH_LANES'].notna()]
        self.df['SJF70'].iloc[tmp_df.index.tolist()] = False

    def sjf71(self):
        # COUNTER_PEAK_LANES	COUNTER_PEAK_LANES must be NULL if FACILITY_TYPE is 1
        print("Running rule SJF71...")
        self.df['SJF71'] = True
        tmp_df = self.df.copy()
        tmp_df = tmp_df[tmp_df['FACILITY_TYPE']==1]
        tmp_df = tmp_df[tmp_df['COUNTER_PEAK_LANES'].notna()]
        self.df['SJF71'].iloc[tmp_df.index.tolist()]=False

    def sjf72(self):
        #SPEED_LIMIT ValueNumeric must be divisible by 5; and < 90 OR = 999
        print("Running rule SJF72...")
        self.df['SJF72'] = True
        tmp_df = self.df.copy()
        
        tmp_df = tmp_df[tmp_df['SPEED_LIMIT'].notna()]
        # tmp_df = tmp_df[(tmp_df['SPEED_LIMIT'].astype(int) % 5) ==0]
        # tmp_df = tmp_df[(tmp_df['SPEED_LIMIT'] < 90) | (tmp_df['SPEED_LIMIT'] == 999)]
        tmp_df = tmp_df[((tmp_df['SPEED_LIMIT'].astype(int)%5) != 0) | ((tmp_df['SPEED_LIMIT'] >= 90) & (tmp_df['SPEED_LIMIT'] != 999))]

        self.df['SJF72'].iloc[tmp_df.index.tolist()] = False

    def sjf73(self):
        # SIGNAL_TYPE	Where F_SYSTEM = 1 and URBAN_ID <> 99999; SIGNAL_TYPE must = 5
        print('Running rule sjf73...')
        self.df['SJF73'] = True
        tmp_df = self.df.copy()
        tmp_df = tmp_df[tmp_df['F_SYSTEM']==1]
        tmp_df = tmp_df[tmp_df['URBAN_CODE'].astype(float)!=99999]
        tmp_df = tmp_df[tmp_df['SIGNAL_TYPE']!=5]
        self.df['SJF73'].iloc[tmp_df.index.tolist()] = False

    def sjf74(self):
        # LANE_WIDTH	LANE_WIDTH ValueNumeric should be > 5 and <19
        print('Running rule sjf74...')
        self.df['SJF74'] = True
        tmp_df = self.df.copy()
        tmp_df = tmp_df[(tmp_df['LANE_WIDTH']<=5)|(tmp_df['LANE_WIDTH']>=19)]
        tmp_df = tmp_df[tmp_df['LANE_WIDTH'].notna()]
        self.df['SJF74'].iloc[tmp_df.index.tolist()] = False

    def sjf75(self):
        # MEDIAN_TYPE	Where MEDIAN_TYPE is in the range (2;3;4;5;6) THEN MEDIAN_WIDTH MUST BE > 0
        print('Running rule sjf75...')
        self.df['SJF75'] = True
        tmp_df = self.df.copy()
        tmp_df = tmp_df[tmp_df['MEDIAN_TYPE'].isin([2,3,4,5,6])]
        tmp_df = tmp_df[tmp_df['MEDIAN_WIDTH']<=0]
        self.df['SJF75'].iloc[tmp_df.index.tolist()] = False

    def sjf76(self):
        # MEDIAN_WIDTH	MEDIAN_WIDTH should be NULL if (FACILITY_TYPE ValueNumeric is = 1 or=  4; OR WHERE MEDIAN_TYPE ValueNumeric = 1
        print('Running rule sjf76...')
        self.df['SJF76'] = True
        tmp_df = self.df.copy()
        tmp_df = tmp_df[(tmp_df['FACILITY_TYPE'].isin([1,4]))| (tmp_df['MEDIAN_TYPE']==1)]
        tmp_df = tmp_df[tmp_df['MEDIAN_WIDTH'].notna()]
        self.df['SJF76'].iloc[tmp_df.index.tolist()]= False

    def sjf77(self):
        # SHOULDER_WIDTH_L	SHOULDER_WIDTH_L should be < Median_Width
        print('Running rule sjf77...')
        self.df['SJF77'] = True
        tmp_df = self.df.copy()
        tmp_df = tmp_df[tmp_df['SHOULDER_WIDTH_L']>=tmp_df['MEDIAN_WIDTH']]
        self.df['SJF77'].iloc[tmp_df.index.tolist()] = False
        
    def sjf78(self):
        #IRI
        #ValueDate Must Must = BeginDate Where Sample OR ValueText is Null AND F_SYSTEM >1 and NHS in (1;2;3;4;5;6;7;8;9)
        #Ignoring logic after OR as we don't have ValueText information
        #Assuming ValueDate must be >= BeginDate otherwise all items would fail
        print("Running rule SJF78...")
        self.df['SJF78'] = True
        tempDF = self.df.copy() 

        tempDF['Begin_Date'] = pd.to_datetime(tempDF.Begin_Date)
        beginDate = tempDF.Begin_Date.min()
        beginDate = datetime.strptime(str(beginDate.year), '%Y')

        tempDF = tempDF[tempDF['HPMS_SAMPLE_NO'].notna()]
        tempDF = tempDF[pd.to_datetime(tempDF['IRI_VALUE_DATE']) < beginDate]
        self.df['SJF78'].iloc[tempDF.index.tolist()] = False  

    def sjf79(self):
        #RUTTING
        #ValueDate Must Must = BeginDate Where Sample OR ValueText is Null AND F_SYSTEM >1 and NHS in (1;2;3;4;5;6;7;8;9)
        #Ignoring logic after OR as we don't have ValueText information
        #Assuming ValueDate must be >= BeginDate otherwise all items would fail
        print("Running rule SJF79...")
        self.df['SJF79'] = True
        tempDF = self.df.copy() 

        tempDF['Begin_Date'] = pd.to_datetime(tempDF.Begin_Date)
        beginDate = tempDF.Begin_Date.min()
        beginDate = datetime.strptime(str(beginDate.year), '%Y')

        tempDF = tempDF[tempDF['HPMS_SAMPLE_NO'].notna()]
        tempDF = tempDF[pd.to_datetime(tempDF['RUTTING_VALUE_DATE']) < beginDate]
        self.df['SJF79'].iloc[tempDF.index.tolist()] = False  

    def sjf80(self):
        #FAULTING
        #ValueDate Must Must = BeginDate Where Sample OR ValueText is Null AND F_SYSTEM >1 and NHS in (1;2;3;4;5;6;7;8;9)
        #Ignoring logic after OR as we don't have ValueText information
        #Assuming ValueDate must be >= BeginDate otherwise all items would fail
        print("Running rule SJF80...")
        self.df['SJF80'] = True
        tempDF = self.df.copy() 

        tempDF['Begin_Date'] = pd.to_datetime(tempDF.Begin_Date)
        beginDate = tempDF.Begin_Date.min()
        beginDate = datetime.strptime(str(beginDate.year), '%Y')

        tempDF = tempDF[tempDF['HPMS_SAMPLE_NO'].notna()]
        tempDF = tempDF[tempDF['FAULTING_VALUE_DATE'].notna()]

        tempDF = tempDF[pd.to_datetime(df['FAULTING_VALUE_DATE'],errors='coerce') < beginDate]
        self.df['SJF80'].iloc[tempDF.index.tolist()] = False  

    def sjf81(self):
        #CRACKING_PERCENT
        #ValueDate Must Must >= (BeginDate – 1) Where Sample OR ValueText is Null AND F_SYSTEM >1 and NHS in (1;2;3;4;5;6;7;8;9)
        #Ignoring logic after OR since we don't have ValueText information
        print("Running rule SJF81...")
        self.df['SJF81'] = True
        tempDF = self.df.copy()

        beginDate_less_one = datetime.now() - relativedelta(years=2)
        beginDate_less_one = datetime.strptime(str(beginDate_less_one.year), '%Y')

        tempDF = tempDF[tempDF['HPMS_SAMPLE_NO'].notna()]
        tempDF = tempDF[pd.to_datetime(tempDF['CRACKING_PERCENT_VALUE_DATE']) < beginDate_less_one]
        self.df['SJF81'].iloc[tempDF.index.tolist()] = False

    def sjf82a(self):
        #Cracking Percent
        #ValueDate Must = BeginDate  Where ValueText is Null AND F_SYSTEM =1
        #Rule not created as we don't have ValueText information
        print("Running rule SJF82a...")
        self.df['SJF82a'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[(tempDF['CRACKING_PERCENT_VALUE_TEXT'].isna())&(tempDF['F_SYSTEM']==1)]
        tempDF = tempDF[~(tempDF['CRACKING_PERCENT_VALUE_DATE']==tempDF['Begin_Date'])]
        tempDF = tempDF[~(tempDF.CRACKING_PERCENT_VALUE_DATE.isna()&tempDF.Begin_Date.isna())]
        self.df['SJF82a'].iloc[tempDF.index.tolist()] = False
        # print(self.df[~self.df['SJF82a']][rules_col_used.get('SJF82a')+['RouteID','BMP','EMP']])


    def sjf82b(self):
        #Faulting
        #ValueDate Must = BeginDate  Where ValueText is Null AND F_SYSTEM =1
        #Rule not created as we don't have ValueText information
        print("Running rule SJF82b...")
        self.df['SJF82b'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[(tempDF['FAULTING_VALUE_TEXT'].isna())&(tempDF['F_SYSTEM']==1)]
        tempDF = tempDF[~(tempDF['FAULTING_VALUE_DATE']==tempDF['Begin_Date'])]
        tempDF = tempDF[~(tempDF.FAULTING_VALUE_DATE.isna()&tempDF.Begin_Date.isna())]
        self.df['SJF82b'].iloc[tempDF.index.tolist()] = False
        # print(self.df[~self.df['SJF82a']][rules_col_used.get('SJF82a')+['RouteID','BMP','EMP']])

    def sjf82c(self):
        #IRI
        #ValueDate Must = BeginDate  Where ValueText is Null AND F_SYSTEM =1
        #Rule not created as we don't have ValueText information
        print("Running rule SJF82c...")
        self.df['SJF82c'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[(tempDF['IRI_VALUE_TEXT'].isna())&(tempDF['F_SYSTEM']==1)]
        tempDF = tempDF[~(tempDF['IRI_VALUE_DATE']==tempDF['Begin_Date'])]
        tempDF = tempDF[~(tempDF.IRI_VALUE_DATE.isna()&tempDF.Begin_Date.isna())]
        self.df['SJF82c'].iloc[tempDF.index.tolist()] = False
        # print(self.df[~self.df['SJF82a']][rules_col_used.get(self.df.columns[-1],[])+['RouteID','BMP','EMP']])

    def sjf82d(self):
        #Rutting
        #ValueDate Must = BeginDate  Where ValueText is Null AND F_SYSTEM =1
        #Rule not created as we don't have ValueText information
        print("Running rule SJF82d...")
        self.df['SJF82d'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[(tempDF['RUTTING_VALUE_TEXT'].isna())&(tempDF['F_SYSTEM']==1)]
        tempDF = tempDF[~(tempDF['RUTTING_VALUE_DATE']==tempDF['Begin_Date'])]
        tempDF = tempDF[~(tempDF.RUTTING_VALUE_DATE.isna()&tempDF.Begin_Date.isna())]
        self.df['SJF82d'].iloc[tempDF.index.tolist()] = False
        # print(self.df[~self.df['SJF82a']][rules_col_used.get(self.df.columns[-1],[])+['RouteID','BMP','EMP']])


    def sjf83a(self):
        #CRACKING_PERCENT
        #ValueText Must Be In (A;B;C;D;E) Where ValueDate <> BeginDate and F_SYSTEM = 1 OR if ValueDate < BeginDate -1 on NHS
        #Rule not created as we don't have the ValueText information
        print("Running rule SJF83a...")
        self.df['SJF83a'] = True
        tempDF = self.df.copy()

        bv = pd.to_datetime(tempDF.CRACKING_PERCENT_VALUE_DATE,errors='coerce')<(pd.to_datetime(tempDF.Begin_Date,errors='coerce')-pd.Timedelta(weeks=52))
        tempDF = tempDF[((tempDF.CRACKING_PERCENT_VALUE_DATE!=tempDF.Begin_Date)&(tempDF['F_SYSTEM']==1&tempDF.NHS.notna()))|bv]

        # print(tempDF.CRACKING_PERCENT_VALUE_TEXT.unique())
        tempDF = tempDF[~tempDF.CRACKING_PERCENT_VALUE_TEXT.isin(['A','B','C','D','E'])]
        self.df['SJF83a'].iloc[tempDF.index.tolist()] = False
        # print(self.df[~self.df['SJF83a']][rules_col_used.get(self.df.columns[-1],[])+['RouteID','BMP','EMP']])


    def sjf83b(self):
        #Faulting
        #ValueText Must Be In (A;B;C;D;E) Where ValueDate <> BeginDate and F_SYSTEM = 1 OR if ValueDate < BeginDate -1 on NHS
        #Rule not created as we don't have the ValueText information
        print("Running rule SJF83b...")
        self.df['SJF83b'] = True
        tempDF = self.df.copy()
        bv = pd.to_datetime(tempDF.FAULTING_VALUE_DATE,errors='coerce')<(pd.to_datetime(tempDF.Begin_Date,errors='coerce')-pd.Timedelta(days=1))
        tempDF = tempDF[((tempDF.FAULTING_VALUE_DATE!=tempDF.Begin_Date)&(tempDF['F_SYSTEM']==1&tempDF.NHS.notna()))|bv]

        # print(tempDF.CRACKING_PERCENT_VALUE_TEXT.unique())
        tempDF = tempDF[~tempDF.FAULTING_VALUE_TEXT.isin(['A','B','C','D'])]
        self.df['SJF83b'].iloc[tempDF.index.tolist()] = False
        # print(self.df[~self.df['SJF83a']][rules_col_used.get(self.df.columns[-1],[])+['RouteID','BMP','EMP']])


    def sjf83c(self):
        #IRI
        #ValueText Must Be In (A;B;C;D;E) Where ValueDate <> BeginDate and F_SYSTEM = 1 OR if ValueDate < BeginDate -1 on NHS
        #Rule not created as we don't have the ValueText information
        print("Running rule SJF83c...")
        self.df['SJF83c'] = True

        tempDF = self.df.copy()
        bv = pd.to_datetime(tempDF.IRI_VALUE_DATE,errors='coerce')<(pd.to_datetime(tempDF.Begin_Date,errors='coerce')-pd.Timedelta(days=1))
        tempDF = tempDF[((tempDF.IRI_VALUE_DATE!=tempDF.Begin_Date)&(tempDF['F_SYSTEM']==1&tempDF.NHS.notna()))|bv]

        # print(tempDF.CRACKING_PERCENT_VALUE_TEXT.unique())
        tempDF = tempDF[~tempDF.IRI_VALUE_TEXT.isin(['A','B','C','D'])]
        self.df['SJF83c'].iloc[tempDF.index.tolist()] = False
        # print(self.df[~self.df['SJF83c']][rules_col_used.get(self.df.columns[-1],[])+['RouteID','BMP','EMP']])


    def sjf83d(self):
        #Rutting
        #ValueText Must Be In (A;B;C;D;E) Where ValueDate <> BeginDate and F_SYSTEM = 1 OR if ValueDate < BeginDate -1 on NHS
        #Rule not created as we don't have the ValueText information
        print("Running rule SJF83d...")
        self.df['SJF83d'] = True
        tempDF = self.df.copy()
        bv = pd.to_datetime(tempDF.RUTTING_VALUE_DATE,errors='coerce')<(pd.to_datetime(tempDF.Begin_Date,errors='coerce')-pd.Timedelta(days=1))
        tempDF = tempDF[((tempDF.RUTTING_VALUE_DATE!=tempDF.Begin_Date)&(tempDF['F_SYSTEM']==1&tempDF.NHS.notna()))|bv]

        # print(tempDF.CRACKING_PERCENT_VALUE_TEXT.unique())
        tempDF = tempDF[~tempDF.RUTTING_VALUE_TEXT.isin(['A','B','C','D'])]
        self.df['SJF83d'].iloc[tempDF.index.tolist()] = False
        # print(self.df[~self.df['SJF83d']][rules_col_used.get(self.df.columns[-1],[])+['RouteID','BMP','EMP']])


    def sjf84(self):
        #PSR ValueDate Must Must >= BeginDate – 1 Where Sample OR F_SYSTEM >1 and NHS in (1;2;3;4;5;6;7;8;9)
        #Rule not implemented as we don't report PSR
        print("Running rule SJF84...")
        self.df['SJF84'] = True
        tempDF = self.df.copy()
        bv = pd.to_datetime(tempDF.PRS_VALUE_DATE,errors='coerce')>(pd.to_datetime(tempDF.Begin_Date,errors='coerce')-pd.Timedelta(days=1))
        tempDF = tempDF[tempDF.HPMS_SAMPLE_NO.notna()|((tempDF.F_SYSTEM==1)&(tempDF.NHS.isin([1,2,3,4,5,6,7,8,9])))]
        tempDF = tempDF[~bv]
        self.df['SJF84'].iloc[tempDF.index.tolist()] = False
        # print(self.df[~self.df['SJF84']][rules_col_used.get(self.df.columns[-1],[])+['RouteID','BMP','EMP']])


    def sjf85(self):
        #ValueDate Must = BeginDate Where PSR ValueText is "A" AND F_SYSTEM =1
        #Rule not implemented as we don't report PSR
        print("Running rule SJF85...")
        # self.df['SJF85'] = True

    def sjf86(self):
        #Where F_SYSTEM =1; and IRI is Null; PSR ValueNumeric Must be >0 and PSR ValueText must = A
        #Rule not implemented as we don't report PSR
        print("Running rule SJF86...")
        # self.df['SJF86'] = True

    def sjf87(self):
        #RUTTING < 1
        print("Running rule SJF87...")
        self.df['SJF87'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['RUTTING'] >= 1]

        self.df['SJF87'].iloc[tempDF.index.tolist()] = False

    def sjf88(self):
        #FAULTING<=1
        print("Running rule SJF88...")
        self.df['SJF88'] = True
        tempDF = self.df.copy() 
        tempDF = tempDF[tempDF['FAULTING'] > 1]
        self.df['SJF88'].iloc[tempDF.index.tolist()] = False 


    def sjf89(self):
        #Where SURFACE_TYPE is in (2;6;7;8) CRACKING_PERCENT <= X based on LANE_WIDTH. Where X = Max % AC Cracking on AC Cracking Validation table.
        #Don't have AC Cracking Validation table, skipping rule for now
        print("Running rule SJF89...")
        # self.df['SJF89'] = True

    def sjf90(self):
        #Where SURFACE_TYPE is in (3;4;5;9;10) CRACKING_PERCENT < .75
        print("Running rule SJF90...")
        self.df['SJF90'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['SURFACE_TYPE'].isin([3,4,5,9,10])]
        tempDF = tempDF[tempDF['CRACKING_PERCENT'] >= .75]
        self.df['SJF90'].iloc[tempDF.index.tolist()] = False

    def sjf91(self):
        #ValueDate <= BeginDate
        #Begin date is assumed to be last year (from whenever the program is run)
        print("Running rule SJF91...")
        self.df['SJF91'] = True

        # this is fucked
        # lastYear = datetime.now() - relativedelta(years=1) 
        # lastYear = datetime.strptime(str(lastYear.year), '%Y')
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['YEAR_LAST_CONSTRUCTION_VALUE_DATE'].notna()]
        tempDF['YEAR_LAST_CONSTRUCTION_VALUE_DATE'] = pd.to_datetime(tempDF['YEAR_LAST_CONSTRUCTION_VALUE_DATE'])
        tempDF['Begin_Date'] = pd.to_datetime(tempDF['Begin_Date'])

        tempDF = tempDF[tempDF['YEAR_LAST_CONSTRUCTION_VALUE_DATE'] > tempDF.Begin_Date]
        self.df['SJF91'].iloc[tempDF.index.tolist()] = False

    def sjf92(self):
        #THICKNESS_RIGID must be Null WHERE SURFACE_TYPE in (2;6) 
        print("Running rule SJF92...")
        self.df['SJF92'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['SURFACE_TYPE'].isin([2,6])]
        tempDF = tempDF[tempDF['THICKNESS_RIGID'].notna()]
        self.df['SJF92'].iloc[tempDF.index.tolist()] = False


    def sjf93(self):
        #THICKNESS_FLEXIBLE must be Null WHERE SURFACE_TYPE in (3;4;5;9;10)
        print("Running rule SJF93...")
        self.df['SJF93'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['SURFACE_TYPE'].isin([3,4,5,9,10])]
        tempDF = tempDF[tempDF['THICKNESS_FLEXIBLE'].notna()]
        self.df['SJF93'].iloc[tempDF.index.tolist()] = False

    def sjf94(self):
        #THICKNESS_FLEXIBLE MUST NOT be Null WHERE SURFACE_TYPE IN (7;8)
        print("Running rule SJF94...")
        self.df['SJF94'] = True
        tempDF = self.df.copy() 
        tempDF = tempDF[tempDF['SURFACE_TYPE'].isin([7,8])]
        tempDF = tempDF[tempDF['THICKNESS_FLEXIBLE'].isna()]
        self.df['SJF94'].iloc[tempDF.index.tolist()] = False

    def sjf95(self):
        #THICKNESS_RIGID MUST NOT be Null WHERE SURFACE_TYPE IN (7;8)
        print("Running rule SJF95...")
        self.df['SJF95'] = True
        tempDF = self.df.copy() 
        tempDF = tempDF[tempDF['SURFACE_TYPE'].isin([7,8])]
        tempDF = tempDF[tempDF['THICKNESS_RIGID'].isna()]
        self.df['SJF95'].iloc[tempDF.index.tolist()] = False

    def sjf96(self):
        #DIR_THROUGH_LANES ValueNumeric Must be < OR = ValueNumeric for THROUGH_LANES
        print("Running rule SJF96...")
        self.df['SJF96'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['DIR_THROUGH_LANES'] > tempDF['THROUGH_LANES']]
        self.df['SJF96'].iloc[tempDF.index.tolist()] = False


    def sjf97(self):
        rule_name = self.sjf97.__name__.upper()
        #IF TRAVEL_TIME_CODE is reported; it must cover NHS
        #We don't report TRAVEL_TIME
        print("Running rule SJF97...")
        # self.df[rule_name] = True

    def sjf98(self):
        rule_name = self.sjf98.__name__.upper()

        #REVIEW THIS RULE
        #MAINTENANCE_OPERATIONS ValueNumeric <> OWNERSHIP ValueNumeric
        print("Running rule SJF98...")
        self.df[rule_name] = True
        tempDF = self.df.copy()
        tempDF = tempDF[(tempDF['MAINTENANCE_OPERATIONS']!=tempDF.OWNERSHIP)&(tempDF.OWNERSHIP.notna()|tempDF.MAINTENANCE_OPERATIONS.notna())]
        self.df[rule_name].iloc[tempDF.index.tolist()] = False
        # print(self.df[~self.df[rule_name]][rules_col_used.get(self.df.columns[-1],[])+['RouteID','BMP','EMP']])

        # tempDF = self.df.copy() 
        # self.df['SJF'].iloc[tempDF.index.tolist()] = False

    def sjf99(self):
        #Sample crosses TOPS.  
        # The extent of a given Sample Panel Section extends beyond the extent of the associated TOPS section.  
        # Samples should match the length of TOPS sections or be shorter; but can not be longer.
        print("Running rule SJF99...")
        # self.df['SJF99'] = True
   

    def sjf100(self):
        #Samples may only exist where Facility_Type IN 1;2 and (F_System = 1-5 or F_System = 6 and Urban Code <99999)
        print("Running rule SJF100...")
        self.df['SJF100'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[~tempDF['FACILITY_TYPE'].isin([1,2])]
        tempDF = tempDF[~tempDF['F_SYSTEM'].isin(range(1,6))]
        tempDF = tempDF[~(tempDF['F_SYSTEM'] == 6) | ~(tempDF['URBAN_CODE'].astype(float) < 99999)]
        tempDF = tempDF[tempDF['HPMS_SAMPLE_NO'].notna()]
        self.df['SJF100'].iloc[tempDF.index.tolist()] = False 
    
    def run(self):
        #when it returns True, it means the data has no errors itself
        if debug:
            self.sjf82a()
            self.sjf82b()
            self.sjf82c()
            self.sjf82d()
            self.sjf83a()

            self.sjf83b()
            
            self.sjf83c()
            self.sjf83d()
            self.sjf84()
            self.sjf85()
            self.sjf98()

        else:
            pass
    def _create_link(self,rule):
        return f'=HYPERLINK("#{rule}!A1", "{rule}")'
    

    
    def create_output(self,outfilename='full_spatial_validations_error.xlsx',combine_segs=True,size_limit=1000000,fhwa=False):
        '''
        Creates the output xlsx sheet I couldn't figure out the autofit API that tyler got working one time? 
        Maybe you can only use it via windows but anyway here is a good start for the code to create an output. 
        
        outfilename  - the output file location 
        
        combine_segs - whether or not to combine_like segments for error outputs (we only export 
                       columns used in the specific rule therefore it is more readable to combine 
                       those like or duplicated columns as to not have unneeded splits) 
        
        size_limit   - The max size limit of the rule dataframe that we will still do a combine on. The combine is 
                       a heavy operation in pandas therefore limiting the huge rule violations saves a lot of compute 

        TODO: 
            - Create an xlsx file with all the rule relations inside of it instead of the dictionaries at the top 
            - read that sheet in manipulate it to get the dictionaries needed
            - Add the item name column to the sheet as well 
            - Finish the sheet and put all rules inside of it as well as column relationships 
            - Add description formatting to wordwrap and maybe adjust height of summary row size to accomadate 
            multiple rows of text
            - add a any rule violated table as well 
        '''
        df = self.df.copy()
        cols = [i for i in df.columns if i[:2].upper() in ["SJ", "SF"] and not '_FHWA' in i]
        df['Length'] = df['EMP'] - df['BMP']

        # creating summary
        vals = []
        for i in cols:
            val = df.groupby(i)['Length'].count().to_dict()
            print(i)
            val['Failing Length'] = df[~df[i]].Length.sum()
            val['Rule'] = i if val.get(False,0) == 0 else self._create_link(i)
            val['Description'] = rules_description.get(i,'')
            val['Data Item'] = rules_name.get(i,'')
            vals.append(val)
        
        # making into df and renaming True and False columns
        sumdf = pd.DataFrame(vals).fillna(value=0)
        sumdf.rename(columns={False:'Failed Rows',True:'Passed Rows'},inplace=True)

        # creating writer, selecting used columns and writing out
        writer = pd.ExcelWriter(outfilename,engine='xlsxwriter')
        sumdf = sumdf[['Rule','Data Item','Description','Failed Rows','Passed Rows','Failing Length']]
        sumdf.to_excel(writer,sheet_name='Summary',index=False)

        # getting book and sheet and creating formats
        workbook = writer.book # Access the workbook
        worksheet= writer.sheets['Summary'] # Access the Worksheet
        text_format = workbook.add_format({'text_wrap': True})
        cell_format_hl = workbook.add_format({'font_color': 'blue','font':'Calibri (Body)','underline':'single'})
        
        # iterating through each header and apply relevant formatting
        header_list = sumdf.columns.values.tolist() # Generate list of headers
        for i in range(0, len(header_list)):
            if header_list[i] == 'Rule': # this column gets a hyperlink format
                for ii in range(len(sumdf)):
                    worksheet.write(f'A{ii+2}',sumdf['Rule'].iloc[ii],cell_format_hl)
            elif header_list[i] == 'Description': # this column gets a text wrap
                worksheet.set_column(i, i, 30) # Set column widths based on len(header)
                for ii in range(len(sumdf)):
                    worksheet.write(f'C{ii+2}',sumdf['Description'].iloc[ii],text_format)
            else:
                worksheet.set_column(i, i, max([sumdf[header_list[i]].astype(str).str.len().max(),len(header_list[i])]),None) # Set column widths based on len(header)
        
        # creating filter and filtering based on Failed Rows 
        worksheet.autofilter(0, 0, len(sumdf)-1, len(sumdf.columns)-1)
        worksheet.filter_column('D', 'x > 0')
        
        # hiding rows 
        for row_num in (sumdf.index[(sumdf['Failed Rows'] == 0)].tolist()):
            worksheet.set_row(row_num + 1, options={'hidden': True})
        
        # iterating through each rule 
        pos = 0
        for k,v in rules_col_used.items():
            if k in df.columns:
                v = [i for i in v if i in df.columns] # if used columns dont exist in df.columns filter 

                # where the rule fails select the used columns and the RouteID,BMP,EMP
                tmp_cols = ['RouteID','BMP','EMP']+v 
                tmpdf = df[~df[k]][tmp_cols] 

                # performing this part only if tmpdf is not zero and tmpdf is less than size_limit 
                # if combine_segs is set to False then the df needs have at least 1 row
                # the size_limit filters out rules that are not currently useful due to high volume
                # and reduces the time on the combine_df function
                if len(tmpdf) > 0 and ((len(tmpdf) < size_limit and combine_segs) or not combine_segs):
                    # combining segments if designated
                    if combine_segs: tmpdf = combine_df(tmpdf,columns=v)[tmp_cols]
                    tmpdf.to_excel(writer,sheet_name=k,index=False,startrow=2)
                    workbook = writer.book # Access the workbook
                    worksheet= writer.sheets[k] # Access the Worksheet
                    header_list = tmpdf.columns.values.tolist() # Generate list of headers
                    for i in range(0, len(header_list)):
                        if i < 3 and i > 0: # bmp and emp set to width 5
                            worksheet.set_column(i,i,9)
                        else:
                            worksheet.set_column(i, i, max([tmpdf[header_list[i]].astype(str).str.len().max(),len(header_list[i])])+4) # Set column widths based on len(header)
                    
                    worksheet.write(0,0,'=HYPERLINK("#Summary!A1", "Back to Summary Sheet")',cell_format_hl)
                    # worksheet.write(1,0,'{0}: {1}'.format(k,rules_description.get(k,'')),text_format)
                    worksheet.merge_range(1, 0, 1, len(tmpdf.columns)-1,'{0} ({1}): {2}'.format(k,rules_name.get(k,''),rules_description.get(k,'')), text_format)
                    worksheet.set_row(1, 45) # Set column widths based on len(header)
                    worksheet.freeze_panes(3,0)
                    worksheet.autofilter(2, 0, len(tmpdf)+1, len(tmpdf.columns)-1)

                elif len(tmpdf) > size_limit:
                    print(f'Skipped {k} Item due to large size {len(tmpdf)}')
            pos+=1
            print(f'[{pos}/{len(rules_col_used)}]')
        writer.close()


        # removing tmp.csv and tmp2.csv if they exist
        if os.path.exists('tmp.csv'): os.remove('tmp.csv')
        if os.path.exists('tmp2.csv'): os.remove('tmp2.csv')

# df = pd.read_csv('/Users/charlesbmurphy/Downloads/full_spatial_errors_table.csv')
# df = combine_errors(df,'all_submission_data.csv')

# df = pd.read_csv('all_submission_data.csv',dtype={'URBAN_CODE':str,'HPMS_SAMPLE_NO':str})
# df = pd.read_csv('all_submission_data.csv',dtype={'URBAN_CODE':str,'HPMS_SAMPLE_NO':str})


# errors = pd.read_csv('/Users/charlesbmurphy/Downloads/hpms-validation/7-7-2023_full_spatial_validations.csv')
# df = combine_errors(errors,'/Users/charlesbmurphy/Downloads/hpms-validation/full_join_taylor.csv',dtype={'UrbanIdVn':str,'SampleId':str})
# df.to_csv('full_inventory_taylor_with_errors.csv',index=False)

# remapping columns from FHWA to our schema
df = pd.read_csv('full_inventory_taylor_with_errors.csv',dtype={'UrbanIdVn':str,'SampleId':str})
df['Begin_Date'] = '01/01/2022'
df['PRS_VALUE_DATE'] = pd.NA
rename_dict = {'RouteId': 'RouteID', 'BeginPoint': 'BMP', 'EndPoint': 'EMP','Begin_Date':'Begin_Date', 'RouteNumber': 'ROUTE_NUMBER', 'RouteQualifier': 'ROUTE_QUALIFIER', 'RouteName': 'ROUTE_NAME_VALUE_TEXT', 'RouteSigning': 'ROUTE_SIGNING', 'SampleId': 'HPMS_SAMPLE_NO', 'FsystemVn': 'F_SYSTEM', 'NhsVn': 'NHS', 'StrahnetTypeVn': 'STRAHNET_TYPE', 'NnVn': 'NN', 'NhfnVn': 'NHFN', 'UrbanIdVn': 'URBAN_CODE', 'FacilityTypeVn': 'FACILITY_TYPE', 'StructureTypeVn': 'STRUCTURE_TYPE', 'OwnershipVn': 'OWNERSHIP', 'CountyIdVn': 'COUNTY_ID', 'MaintenanceOperationsVn': 'MAINTENANCE_OPERATIONS', 'IsRestrictedVn': 'IS_RESTRICTED', 'ThroughLanesVn': 'THROUGH_LANES', 'ManagedLanesTypeVn': 'MANAGED_LANES_TYPE', 'ManagedLanesVn': 'MANAGED_LANES', 'PeakLanesVn': 'PEAK_LANES', 'CounterPeakLanesVn': 'COUNTER_PEAK_LANES', 'TollIdVn': 'TOLL_ID', 'LaneWidthVn': 'LANE_WIDTH', 'MedianTypeVn': 'MEDIAN_TYPE', 'MedianWidthVn': 'MEDIAN_WIDTH', 'ShoulderTypeVn': 'SHOULDER_TYPE', 'ShoulderWidthRVn': 'SHOULDER_WIDTH_R', 'ShoulderWidthLVn': 'SHOULDER_WIDTH_L', 'PeakParkingVn': 'PEAK_PARKING', 'DirThroughLanesVn': 'DIR_THROUGH_LANES', 'TurnLanesRVn': 'TURN_LANES_R', 'TurnLanesLVn': 'TURN_LANES_L', 'SignalTypeVn': 'SIGNAL_TYPE', 'PctGreenTimeVn': 'PCT_GREEN_TIME', 'NumberSignalsVn': 'NUMBER_SIGNALS', 'StopSignsVn': 'STOP_SIGNS', 'AtGradeOtherVn': 'AT_GRADE_OTHER', 'AadtVn': 'AADT', 'AadtVt': 'AADT_VALUE_TEXT', 'AadtVd': 'AADT_VALUE_DATE', 'AadtsingleUnitVn': 'AADT_SINGLE_UNIT', 'AadtsingleUnitVt': 'AADT_SINGLE_UNIT_VALUE_TEXT', 'AadtsingleUnitVd': 'AADT_SINGLE_UNIT_VALUE_DATE', 'AadtcombinationVn': 'AADT_COMBINATION', 'AadtcombinationVt': 'AADT_COMBINATION_VALUE_TEXT', 'AadtcombinationVd': 'AADT_COMBINATION_VALUE_DATE', 'PctdhsingleVn': 'PCT_DH_SINGLE_UNIT', 'PctdhcombinationVn': 'PCT_DH_COMBINATION', 'KfactorVn': 'K_FACTOR', 'DirFactorVn': 'DIR_FACTOR', 'FutureAadtVn': 'FUTURE_AADT', 'FutureAadtVd': 'FUTURE_AADT_VALUE_DATE', 'AccessControlVn': 'ACCESS_CONTROL', 'SpeedLimitVn': 'SPEED_LIMIT', 'IriVn': 'IRI', 'IriVt': 'IRI_VALUE_TEXT', 'IriVd': 'IRI_VALUE_DATE', 'PsrVn': 'PSR', 'PsrVt': 'PSR_VALUE_TEXT', 'PsrVd': 'PSR_VALUE_DATE', 'SurfaceTypeVn': 'SURFACE_TYPE', 'RuttingVn': 'RUTTING', 'RuttingVt': 'RUTTING_VALUE_TEXT', 'RuttingVd': 'RUTTING_VALUE_DATE', 'FaultingVn': 'FAULTING', 'FaultingVt': 'FAULTING_VALUE_TEXT', 'FaultingVd': 'FAULTING_VALUE_DATE', 'CrackingPercentVn': 'CRACKING_PERCENT', 'CrackingPercentVt': 'CRACKING_PERCENT_VALUE_TEXT', 'CrackingPercentVd': 'CRACKING_PERCENT_VALUE_DATE', 'YearLastImprovementVd': 'YEAR_LAST_IMPROVEMENT_VALUE_DATE', 'YearLastConstructionVd': 'YEAR_LAST_CONSTRUCTION_VALUE_DATE', 'LastOverlayThicknessVn': 'LAST_OVERLAY_THICKNESS', 'ThicknessRigidVn': 'THICKNESS_RIGID', 'ThicknessFlexibleVn': 'THICKNESS_FLEXIBLE', 'BaseTypeVn': 'BASE_TYPE', 'BaseThicknessVn': 'BASE_THICKNESS', 'SoilTypeVn': 'SOIL_TYPE', 'WideningPotentialVn': 'WIDENING_POTENTIAL', 'WideningPotentialVt': 'WIDENING_POTENTIAL_VALUE_TEXT', 'CurvesAVn': 'CURVES_A', 'CurvesBVn': 'CURVES_B', 'CurvesCVn': 'CURVES_C', 'CurvesDVn': 'CURVES_D', 'CurvesEVn': 'CURVES_E', 'CurvesFVn': 'CURVES_F', 'TerrarinTypeVn': 'TERRARIN_TYPE', 'GradesAVn': 'GRADES_A', 'GradesBVn': 'GRADES_B', 'GradesCVn': 'GRADES_C', 'GradesDVn': 'GRADES_D', 'GradesEVn': 'GRADES_E', 'GradesFVn': 'GRADES_F', 'PctPassSightVn': 'PCT_PASS_SIGHT', 'TravelTimeCodeVt': 'TRAVEL_TIME_CODE_VALUE_TEXT'}
df.rename(columns=rename_dict,inplace=True) 


c = FullSpatial(df) 
c.run_rules()
fhwa = c.conflate_fhwa()
print(fhwa)
# c.consolidate_errors_rules()
# w = c.create_output()

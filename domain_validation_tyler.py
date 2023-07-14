import pandas as pd
import shutil
import os
from openpyxl.styles import Font
from openpyxl.utils import get_column_letter
import warnings
warnings.filterwarnings("ignore")

URBAN_CODE_list = ['06139','15481','21745','36190','40753','59275','67672','93592','94726', '99998', '99999']


class domain_validations():
    
    def __init__(self,df):
        self.df = df
        self.df.rename(columns={'TRUCK':'NN'}, inplace=True)

    def drd01(self):
        #F_SYSTEM ValueNumeric must be a valid integer in the range (1-7)
        print("Running rule DRD01...")
        self.df['DRD01'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['F_SYSTEM'].notna()]
        tempDF = tempDF[~tempDF['F_SYSTEM'].isin(range(1,8))]
        self.df['DRD01'].iloc[tempDF.index.tolist()] = False

    def drd64(self):
        #NHS ValueNumeric must be an integer in the range (1-9)

        print("Running rule DRD64...")
        self.df['DRD64'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['NHS'].notna()]
        tempDF = tempDF[~tempDF['NHS'].isin(range(1,10))]
        self.df['DRD64'].iloc[tempDF.index.tolist()] = False

    def drd65(self):
        #STRAHNET_TYPE ValueNumeric must be an integer in the range (1;2)

        print("Running rule DRD65...")
        self.df['DRD65'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['STRAHNET_TYPE'].notna()]
        tempDF = tempDF[~tempDF['STRAHNET_TYPE'].isin([1,2])]
        self.df['DRD65'].iloc[tempDF.index.tolist()] = False

    def drd66(self):
        #NN ValueNumeric must be an integer in the range (1;2)

        print("Running rule DRD66...")
        self.df['DRD66'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['NN'].notna()]
        tempDF = tempDF[~tempDF['NN'].isin([1,2])]
        self.df['DRD66'].iloc[tempDF.index.tolist()] = False

    def drd72(self):
        #NHFN ValueNumeric must be an integer in the range (1;2;3)
        #NHFN isn't reported, so rule is disabled in the list below
        print("Running rule DRD72...")
        self.df['DRD72'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['NHFN'].notna()]
        tempDF = tempDF[~tempDF['NHFN'].isin([1,2,3])]
        self.df['DRD72'].iloc[tempDF.index.tolist()] = False


    def drd202(self):
        #RouteID field must not be blank and can not contain pipe characters 
        print("Running rule DRD202...")
        self.df['DRD202'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['RouteID'].isna() | tempDF['RouteID'].str.contains("|", regex=False, na=False)]
        self.df['DRD202'].iloc[tempDF.index.tolist()] = False

    def drd203(self):
        #BeginPoint must be in the format Numeric (8;3) AND must be < EndPoint
        print("Running rule DRD203...")
        self.df['DRD203'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['BMP'] > tempDF['EMP']]
        self.df['DRD203'].iloc[tempDF.index.tolist()] = False

        tempDF = self.df.copy()
        tempDF['BMP'] = tempDF['BMP'].astype("string")
        tempDF['BMP'] = tempDF['BMP'].str.split(".")
        tempDF = tempDF[(tempDF['BMP'].str[0].str.len() > 8) | (tempDF['BMP'].str[1].str.len() > 3)]
        self.df['DRD203'].iloc[tempDF.index.tolist()] = False

    def drd204(self):
        #EndPoint must be in the format Numeric (8;3) AND must be > BeginPoint

        print("Running rule DRD204...")
        self.df['DRD204'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['EMP'] < tempDF['BMP']]
        self.df['DRD204'].iloc[tempDF.index.tolist()] = False

        tempDF = self.df.copy()
        tempDF['EMP'] = tempDF['EMP'].astype("string")
        tempDF['EMP'] = tempDF['EMP'].str.split(".")
        tempDF = tempDF[(tempDF['EMP'].str[0].str.len() > 8) | (tempDF['EMP'].str[1].str.len() > 3)]
        self.df['DRD204'].iloc[tempDF.index.tolist()] = False

    def dre02(self):
        #URBAN_ID ValueNumeric must be an integer and a valid US Census Urban Code for the applicable state

        print("Running rule DRE02...")
        self.df['DRE02'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['URBAN_CODE'].notna()]
        tempDF = tempDF[~tempDF['URBAN_CODE'].isin(URBAN_CODE_list) | tempDF['URBAN_CODE'].str.contains('.', regex=False)]
        self.df['DRE02'].iloc[tempDF.index.tolist()] = False  

    def dre03(self):
        #FACILITY_TYPE ValueNumeric must be a valid integer in the range (1;2;4;5;6;7)

        print("Running rule DRE03...")
        self.df['DRE03'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['FACILITY_TYPE'].notna()]
        tempDF = tempDF[~tempDF['FACILITY_TYPE'].isin([1,2,4,5,6,7])]
        self.df['DRE03'].iloc[tempDF.index.tolist()] = False

    def dre04(self):
        #STRUCTURE_TYPE ValueNumeric must be a valid integer within the range (1-3)

        print("Running rule DRE04...")
        self.df['DRE04'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['STRUCTURE_TYPE'].notna()]
        tempDF = tempDF[~tempDF['STRUCTURE_TYPE'].isin([1,2,3])]
        self.df['DRE04'].iloc[tempDF.index.tolist()] = False

    def dre05(self):
        #ACCESS_CONTROL ValueNumeric must be a valid integer within the range (1-3)

        print("Running rule DRE05...")
        self.df['DRE05'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['ACCESS_CONTROL'].notna()]
        tempDF = tempDF[tempDF['ACCESS_CONTROL'].isin([1,2,3])]
        self.df['DRE05'].iloc[tempDF.index.tolist()] = False

    def dre06(self):
        #OWNERSHIP ValueNumeric must be a valid integer within the range (1;2;3;4;11;12;21;25;26;27;31;32;40;50;60;62;63;64;66;67;68;69;70;72;73;74;80)

        print("Running rule DRE06...")
        self.df['DRE06'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['OWNERSHIP'].notna()]
        tempDF = tempDF[~tempDF['OWNERSHIP'].isin([1,2,3,4,11,12,21,25,26,27,31,32,40,50,60,62,63,64,66,67,68,69,70,72,73,74,80])]
        self.df['DRE06'].iloc[tempDF.index.tolist()] = False
                
    def dre07(self):
        #THROUGH_LANES ValueNumeric must be a valid integer > 0 

        print("Running rule DRE07...")
        self.df['DRE07'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['THROUGH_LANES'].notna()]
        tempDF = tempDF[(tempDF['THROUGH_LANES'] <= 1) | tempDF[tempDF['THROUGH_LANES'].str.contains(".", regex=False)]]
        self.df['DRE07'].iloc[tempDF.index.tolist()] = False

    def dre10(self):
        #PEAK_LANES ValueNumeric must be a valid integer > 0

        print("Running rule DRE10...")
        self.df['DRE10'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['PEAK_LANES'].notna()]
        tempDF = tempDF[tempDF['PEAK_LANES'] <= 0]
        self.df['DRE10'].iloc[tempDF.index.tolist()] = False

    def dre11(self):
        #COUNTER_PEAK_LANES ValueNumeric must be a valid integer > 0 

        print("Running rule DRE11...")
        self.df['DRE11'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['COUNTER_PEAK_LANES'].notna()]
        tempDF = tempDF[tempDF['COUNTER_PEAK_LANES'] <= 0]
        self.df['DRE11'].iloc[tempDF.index.tolist()] = False

    def dre12(self):
        #TURN_LANES_R ValueNumeric must be a valid integer within the range (1-6)

        print("Running rule DRE12...")
        self.df['DRE12'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['TURN_LANES_R'].notna()]
        tempDF = tempDF[~tempDF['TURN_LANES_R'].isin(range(1,7))]
        self.df['DRE12'].iloc[tempDF.index.tolist()] = False

    def dre13(self):
        #TURN_LANES_L ValueNumeric must be a valid integer within the range (1-6)

        print("Running rule DRE13...")
        self.df['DRE13'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['TURN_LANES_L'].notna()]
        tempDF = tempDF[~tempDF['TURN_LANES_L'].isin(range(1,7))]
        self.df['DRE13'].iloc[tempDF.index.tolist()] = False

    def dre14(self):
        #SPEED_LIMIT ValueNumeric must be a valid integer in the range (0-99)

        print("Running rule DRE14...")
        self.df['DRE14'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['SPEED_LIMIT'].notna()]
        tempDF = tempDF[~tempDF['SPEED_LIMIT'].isin(range(0,100))]
        self.df['DRE14'].iloc[tempDF.index.tolist()] = False

    def dre15(self):
        #TOLL_ID ValueNumeric must be a valid Toll ID of no more than 15
        #Assuming this rule means no more than 15 characters, as most toll IDs are > 15

        print("Running rule DRE15...")
        self.df['DRE15'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['TOLL_ID'].notna()]
        tempDF = tempDF[tempDF['TOLL_ID'].astype("string").str.len() > 15]
        self.df['DRE15'].iloc[tempDF.index.tolist()] = False

    def dre21(self):
        #AADT ValueNumeric must be positive integer in the range (0-600000); 
        # ValueText must be in (A;B;C;D;E); 
        # and ValueDate must not be null; and must be in the format YYYY
        print("Running rule DRE21...")
        self.df['DRE21'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['AADT'].notna()]
        tempDF = tempDF[~tempDF['AADT'].isin(range(0,600001))]
        self.df['DRE21'].iloc[tempDF.index.tolist()] = False

        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['AADT_VALUE_TEXT'].notna()]
        tempDF = tempDF[~tempDF['AADT_VALUE_TEXT'].str.contains("[ABCDE]{1}", regex=True)]
        self.df['DRE21'].iloc[tempDF.index.tolist()] = False

        tempDF = self.df.copy()
        tempDF = tempDF[~tempDF['AADT_VALUE_DATE'].str.contains("^\d{4}$") | tempDF['AADT_VALUE_DATE'].isna()]
        self.df['DRE21'].iloc[tempDF.index.tolist()] = False

    def dre22(self):
        #AADT_SINGLE_UNIT ValueNumeric must be positive integer in the range (0-500000);
        #  ValueText must be in (A;B;C;D;E)

        print("Running rule DRE22...")
        self.df['DRE22'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['AADT_SINGLE_UNIT'].notna()]
        tempDF = tempDF[~tempDF['AADT_SINGLE_UNIT'].isin(range(0,500001))]
        self.df['DRE22'].iloc[tempDF.index.tolist()] = False

        tempDF = self.df.copy()
        tempDF = tempDF[~tempDF['AADT_SINGLE_UNIT_VALUE_TEXT'].str.contains("[ABCDE]{1}", regex=True)]
        self.df['DRE22'].iloc[tempDF.index.tolist()] = False

    def dre23(self):
        #PCT_DH_SINGLE_UNIT ValueNumeric must be in the range (0-50)

        print("Running rule DRE23...")
        self.df['DRE23'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['PCT_DH_SINGLE_UNIT'].notna()]
        tempDF = tempDF[~tempDF['PCT_DH_SINGLE_UNIT'].isin(range(0,51))]
        self.df['DRE23'].iloc[tempDF.index.tolist()] = False   

    def dre24(self):
        #AADT_COMBINATION ValueNumeric must be positive integer in the range (0-500000); ValueText must be in (A;B;C;D;E)

        print("Running rule DRE24...")
        self.df['DRE24'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['AADT_COMBINATION'].notna()]
        tempDF = tempDF[~tempDF['AADT_COMBINATION'].isin(range(0,500001))]
        self.df['DRE24'].iloc[tempDF.index.tolist()] = False   

        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['AADT_COMBINATION_VALUE_TEXT'].notna()]
        tempDF = tempDF[~tempDF['AADT_COMBINATION_VALUE_TEXT'].str.contains("[ABCDE]{1}", regex=True)]
        self.df['DRE24'].iloc[tempDF.index.tolist()] = False 

    def dre25(self):
        print("Running rule DRE25...")
        self.df['DRE25'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[~tempDF['AADT_COMBINATION'].isin(range(0,51))]
        self.df['DRE25'].iloc[tempDF.index.tolist()] = False   

    def dre26(self):
        print("Running rule DRE26...")
        self.df['DRE26'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['K_FACTOR'] <= 4]
        self.df['DRE26'].iloc[tempDF.index.tolist()] = False

    def dre27(self):
        print("Running rule DRE27...")
        self.df['DRE27'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[~tempDF['DIR_FACTOR'].isin(range(50,101))]
        self.df['DRE27'].iloc[tempDF.index.tolist()] = False

    def dre28(self):
        print("Running rule DRE28...")
        self.df['DRE28'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['FUTURE_AADT'].notna()]
        tempDF = tempDF[(tempDF['FUTURE_AADT'] <= 0) | tempDF['FUTURE_AADT'].astype(str).str.contains('.', regex=False)]
        self.df['DRE28'].iloc[tempDF.index.tolist()] = False

        tempDF = self.df.copy()
        tempDF = tempDF[(tempDF['FUTURE_AADT_VALUE_DATE'] < (tempDF['BEGIN_DATE'].dt.year + 18)) | (tempDF['FUTURE_AADT_VALUE_DATE'] > (tempDF['BEGIN_DATE'] + 25))]
        self.df['DRE28'].iloc[tempDF.index.tolist()] = False

    def dre29(self):
        #SIGNAL_Type ValueNumeric must be an integer in the range (1 -5)

        print('Running rule DRE29...')
        self.df['DRE29'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['SIGNAL_TYPE'].notna()]
        tempDF = tempDF[~tempDF['SIGNAL_TYPE'].isin(range(1,6))]
        self.df['DRE29'].iloc[tempDF.index.tolist()] = False

    def dre30(self):
        #PCT_GREEN_TIME ValueNumeric must be a positive integer > 0 and <=100

        print('Running rule DRE30...')
        self.df['DRE30'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['PCT_GREEN_TIME'].notna()]
        tempDF = tempDF[~tempDF['PCT_GREEN_TIME'].isin(range(1,101))]
        self.df['DRE30'].iloc[tempDF.index.tolist()] = False

    def dre31(self):
        #NUMBER_SIGNALS ValueNumeric must be an integer >=0

        print('Running rule DRE31...')
        self.df['DRE31'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['NUMBER_SIGNALS'].notna()]
        tempDF = tempDF[(tempDF['NUMBER_SIGNALS'] < 0) | tempDF['NUMBER_SIGNALS'].astype(str).str.contains('.', regex=False)]
        self.df['DRE31'].iloc[tempDF.index.tolist()] = False

    def dre32(self):
        #STOP_SIGNS ValueNumeric must be an integer >=0

        print('Running rule DRE32...')
        self.df['DRE32'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['STOP_SIGNS'].notna()]
        tempDF = tempDF[(tempDF['STOP_SIGNS'] < 0) | tempDF[tempDF['STOP_SIGNS'].astype(str).str.contains('.', regex=False)]]
        self.df['DRE32'].iloc[tempDF.index.tolist()] = False

    def dre33(self):
        #AT_GRADE_OTHER ValueNumeric must be an integer >=0

        print('Running rule DRE33...')
        self.df['DRE33'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['AT_GRADE_OTHER'].notna()]
        tempDF = tempDF[(tempDF['AT_GRADE_OTHER'] < 0) | tempDF['AT_GRADE_OTHER'].astype(str).str.contains('.', regex=False)]
        self.df['DRE33'].iloc[tempDF.index.tolist()] = False

    def dre34(self):
        #LANE_WIDTH ValueNumberic must be an integer in the range (6-30)

        print('Running rule DRE34...')
        self.df['DRE34'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['LANE_WIDTH'].notna()]
        tempDF = tempDF[~tempDF['LANE_WIDTH'].isin(range(6,31))]
        self.df['DRE34'].iloc[tempDF.index.tolist()] = False

    def dre35(self):
        #MEDIAN_TYPE ValueNumeric must be an integer in the range (1-7)

        print('Running rule DRE35...')
        self.df['DRE35'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['MEDIAN_TYPE'].notna()]
        tempDF = tempDF[~tempDF['MEDIAN_TYPE'].isin(range(1,8))]
        self.df['DRE35'].iloc[tempDF.index.tolist()] = False

    def dre36(self):
        #MEDIAN_WIDTH ValueNumeric must be an integer in the range (0-99)

        print('Running rule DRE36...')
        self.df['DRE36'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['MEDIAN_WIDTH'].notna()]
        tempDF = tempDF[~tempDF['MEDIAN_WIDTH'].isin(range(0,100))]
        self.df['DRE36'].iloc[tempDF.index.tolist()] = False

    def dre37(self):
        #SHOULDER_TYPE ValueNumeric must be an integer in the range (1-6)

        print('Running rule DRE37...')
        self.df['DRE37'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['SHOULDER_TYPE'].notna()]
        tempDF = tempDF[~tempDF['SHOULDER_TYPE'].isin(range(1,7))]
        self.df['DRE37'].iloc[tempDF.index.tolist()] = False

    def dre38(self):
        #SHOULDER_WIDTH_R ValueNumeric must be an integer >=0

        print('Running rule DRE38...')
        self.df['DRE38'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['SHOULDER_WIDTH_R'].notna()]
        tempDF = tempDF[(tempDF['SHOULDER_WIDTH_R'] < 0) | tempDF['SHOULDER_WIDTH_R'].astype(str).str.contains('.', regex=False)]
        self.df['DRE38'].iloc[tempDF.index.tolist()] = False

    def dre39(self):
        #SHOULDER_WIDTH_L ValueNumeric must be an integer >=0

        print('Running rule DRE39...')
        self.df['DRE39'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['SHOULDER_WIDTH_L'].notna()]
        tempDF = tempDF[(tempDF['SHOULDER_WIDTH_L'] < 0) | tempDF['SHOULDER_WIDTH_L'].astype(str).str.contains('.', regex=False)]
        self.df['DRE39'].iloc[tempDF.index.tolist()] = False

    def dre40(self):
        #PEAK_PARKING ValueNumeric must be an integer in the range (1-3)

        print('Running rule DRE40...')
        self.df['DRE40'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['PEAK_PARKING'].notna()]
        tempDF = tempDF[~tempDF['PEAK_PARKING'].isin(range(1,4))]
        self.df['DRE40'].iloc[tempDF.index.tolist()] = False

    def dre42(self):
        #WIDENING_POTENTIAL ValueNumberic must be an integer in the range (1-4) AND ValueText must be (X) OR (A;B;C;D;E). 
        #Where ValueText = X; ValueNumeric Must be in (1-4). 

        print('Running rule DRE42...')
        self.df['DRE42'] = True
        tempDF = self.df.copy()
        tempDF = tempDF[tempDF['WIDENING_POTENTIAL'].notna()]
        tempDF = tempDF[~tempDF['WIDENING_POTENTIAL'].isin(range(1,5)) | ~tempDF['WIDENING_POTENTIAL_VALUE_TEXT'].str.contains("[XABCDE]{1}")]
        self.df['DRE42'].iloc[tempDF.index.tolist()] = False






    # def dre(self):
    #     print('Running rule DRE...')
    #     self.df['DRE'] = True
    #     tempDF = self.df.copy()
    #     tempDF = tempDF[tempDF[]]
    #     self.df['DRE'].iloc[tempDF.index.tolist()] = False


    def run(self):

        self.drd01()
        self.drd64()
        self.drd65()
        self.drd66()
        # self.drd72() Data item not in all submission data
        self.drd202()
        self.drd203()
        self.drd204()
        self.dre02()
        self.dre03()
        self.dre04()
        self.dre05()
        self.dre06()
        self.dre07()
        self.dre10()
        self.dre11()
        self.dre12()
        self.dre13()
        self.dre14()
        self.dre15()
        self.dre21()
        self.dre22()
        self.dre23()
        self.dre24()





df = pd.read_csv("all_submission_data.csv", dtype={'URBAN_CODE':str, 'AADT_VALUE_DATE':str})
c = domain_validations(df)
c.run()
c.df.to_csv('test_domain_tyler.csv', index=False) 
import pandas as pd
import shutil
import os
from openpyxl.styles import Font
from openpyxl.utils import get_column_letter
import warnings
warnings.filterwarnings("ignore")

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




                

 
    # def drd(self):
    #     print("Running rule DRD...")
    #     self.df['DRD'] = True
    #     tempDF = self.df.copy()

    #     self.df['DRD'].iloc[tempDF.index.tolist()] = False

    def run(self):
        self.drd01()
        self.drd64()
        self.drd65()
        self.drd66()
        # self.drd72()
        self.drd202()
        self.drd203()
        self.drd204()





df = pd.read_csv("all_submission_data.csv")
c = domain_validations(df)
c.run()
c.df.to_csv('test_domain_tyler.csv', index=False) 
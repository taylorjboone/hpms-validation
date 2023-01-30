from distutils.log import error
import os
from matplotlib import scale
import pandas as pd
from wvdot_utils import add_routeid_df, add_geom_validation_df
from os import listdir
from os.path import isfile, join
import numpy as np

class DomainCheck():

    def __init__(self):
        self.urban_code_list = [99999, 99998, 15481, 6139,21745, 36190, 40753, 59275, 67672, 93592, 94726]
        self.facility_type_list = [1, 2, 4, 5, 6, 7]
        self.functional_class_list = [1, 2, 3, 4, 5, 6, 7]
        self.ownership_list = [1, 2, 3, 4, 11, 12, 21, 25, 26, 27, 31,
                  32, 40, 50, 60, 62, 63, 64, 66, 67, 68, 69, 70, 72]
        self.signal_list=[1,2,3,4,5]
        self.median_type_list=[1,2,3,4,5,6,7]
        self.shoulder_type_list=[1,2,3,4,5,6]
        self.peak_parking_list=[1,2,3]
        self.widening_potential_list=[1,2,3,4]
        self.curve_list=['A','B','C','D','E','F']
        self.terrain_types_list=[1,2,3]
        self.grades_list=['A','B','C','D','E','F']
        self.surface_type_list=[1,2,3,4,5,6,7,8,9,10,11]
        self.base_type_list=[1,2,3,4,5,6,7,8]
        self.county_id_list=[1,3,5,7,9,11,13,15,17,19,21,23,25,27,29,31,33,35,37,39,41,43,45,47,49,51,53,55,57,59,61,63,65,67,69,71,73,75,77,79,91,93,95,97,99,101,103,105,107,109,111]
        self.strahnet_type_list=[1,2]
        self.nn_list=[1,2]
        self.main_ops_list=[1,2,3,4,11,12,21,25,26,27]
        self.nhfn_list=[1,2,3]
        self.is_restricted_list=[1]
        self.access_control_list=[1,2,3]
        self.route_qualifier_list=[1,2,3,4,5,6,7,8,9,10]
        self.route_signing_list=[1,2,3,4,5,6,7,8,9,10]
        self.off_list=[]
        self.total_errors = pd.DataFrame()
        self.correct_format={'County_Code':'CountyID','Pct_MC':'PctMotorcycles','Pct_Lgt_Trucks':'PctLightTrucks','Pct_SU_Trucks':'PctSingleUnit','Pct_CU_Trucks':'PctCombination','YEAR_RECORD':'BeginDate','END_POINT':'EndPoint','STATE_CODE':'StateID','ROUTE_ID':'RouteID','VALUE_DATE':'ValueDate','Data_Item':'DataItem','Value_Numeric':'ValueNumeric','Year_Record':'BeginDate','End_Point':'EndPoint','State_Code':'StateID','Value_Date':'ValueDate','Data_Item':'DataItem','Route_ID':'RouteID','Urban_Code':'UrbanID','Value_Text':'ValueText','VALUE_TEXT':'ValueText','BEGIN_POINT':'BeginPoint','Begin_Point':'BeginPoint'}
        self.correct_format_inverse = {}
        self.mypath = 'hpms_data_items'
        for k,v in self.correct_format.items():
            self.correct_format_inverse[v] = k

    def get_indexes(self,x):
        pair_list=[]
        for n in range(len(x)):
            for b in range(len(x)):
                pair = [n,b]
                if pair not in pair_list and n!=b and n<b:
                    pair_list.append(pair)
        print(pair_list)
        return pair_list

    
    def rid_overlap(self,df):
        for rid in df['Route_ID'].unique():
            tmp = df[df['RouteID']==rid]
            print(tmp)
            print(self.overlap_intersect_check(tmp))

    def overlap_intersect_check(self,x):
        v=False
        indexes=self.get_indexes(x)
        for i1,i2 in indexes:
            s = x[['Route_ID','Begin_Point','End_Point']].iloc[i1]
            s2 = x[['Begin_Point','End_Point']].iloc[i2]
            rid,bmp1,emp1 = s['Route_ID'],s['Begin_Point'],s['End_Point']
            bmp2,emp2 = s2['Begin_Point'],s2['End_Point']
            if self._overlap(bmp1,emp1,bmp2,emp2)==True:
                v=True
                self.off_list.append({rid:[[bmp1,emp1],[bmp2,emp2]]})
        return self.off_list,v
    
    def _overlap(self,bmp1,emp1,bmp2,emp2,debug=False):
        bmp_overlap1 = bmp2 < bmp1 and emp2 > bmp1
        emp_overlap1 = bmp2 < emp1 and emp2 > emp1
        bmp_overlap2 = bmp1 < bmp2 and emp1 > bmp2
        emp_overlap2 = bmp1 < emp2 and emp1 > emp2

        if debug: print(bmp_overlap1,emp_overlap1,bmp_overlap2,emp_overlap2,[bmp1,emp1,bmp2,emp2])
        return (bmp_overlap1 or 
        emp_overlap1 or 
        bmp_overlap2 or 
        emp_overlap2)
    
    def add_column_section_length(self,df):
        if 'Section_Length' not in df.columns:
            df['Section_Length']=df['End_Point']-df['Begin_Point']
            return df
        else:
            return df
    
    def add_error_df(self,df,error_message):
        self.total_errors
        if len(df) > 0:
            df['Error'] = error_message
            self.total_errors = pd.concat([self.total_errors,df],ignore_index=True)
            print('Wrote Errors df with message:%s and size:%s'% (error_message,len(df)))
        else:
            print('No errors found for error message | %s'% error_message)
    
    def read_hpms_csv(self,x):
        df = pd.read_csv(self.mypath + '/' + x,sep='|')
        return df.rename(columns=self.correct_format_inverse)
    
    def get_invalid_geom_name(self,x):
        geom_fn=str.split(x,'.')[0]
        return geom_fn + '_geom_check.xlsx'
    
    def output_geom_file(self,geom_check,fn):
        geom_fn = self.get_invalid_geom_name(fn)
        geom_check[geom_check.IsValid==False].to_excel(geom_fn,index=False)
        print('Output file made : ')
    
    def check_fsystem_valid(self,x,check_geom):
        data = self.read_hpms_csv(x)
        data2 = self.add_column_section_length(data)
        if check_geom:
            geom_check =add_geom_validation_df(
                data2,routeid_field='Route_ID',bmp_field='Begin_Point',emp_field='End_Point')
            self.add_error_df(geom_check[geom_check.IsValid ==
                     False], 'fsystem geometry check invalid!')
        tmpdf = data2[~data2['Value_Numeric'].isin(self.functional_class_list)]
        self.add_error_df(tmpdf, "Check value numeric for fsystem valid")
        tmpdf2 = data2[data2['Section_Length'] == 0]
        print(self.rid_overlap(data2))
        self.add_error_df(
            tmpdf2, "Check value numeric for fsystem section length is zero")
    
    def check_urban_code(self,x,check_geom):
        data = self.read_hpms_csv(x)
        data2 = self.add_column_section_length(data)
        if check_geom:
            geom_check = add_geom_validation_df(
                data2,routeid_field='Route_ID',bmp_field='Begin_Point',emp_field='End_Point')
            self.add_error_df(geom_check[geom_check.IsValid ==
                     False], 'Urban Code geometry check invalid!')
        tmpdf = data2[~data2['Value_Numeric'].isin(self.functional_class_list)]
        self.add_error_df(tmpdf, "Check value numeric for Urban Code valid")
        tmpdf2 = data2[data2['Section_Length'] == 0]
        print(self.rid_overlap(data2))
        self.add_error_df(
            tmpdf2, "Check value numeric for fsystem section length is zero")
    
    def check_facility_type(self,x,check_geom):
        data = self.read_hpms_csv(x)
        data2 = self.add_column_section_length(data)
        if check_geom:
            geom_check = add_geom_validation_df(
                data2,routeid_field='Route_ID',bmp_field='Begin_Point',emp_field='End_Point')
            self.add_error_df(geom_check[geom_check.IsValid ==
                     False], 'Facility Type geometry check invalid!')
        tmpdf = data2[~data2['Value_Numeric'].isin(self.functional_class_list)]
        self.add_error_df(tmpdf, "Check value numeric for Facility Type valid")
        tmpdf2 = data2[data2['Section_Length'] == 0]
        print(self.rid_overlap(data2))
        self.add_error_df(
            tmpdf2, "Check value numeric for fsystem section length is zero")
    
    def check_ownership(self,x,check_geom):
        data = self.read_hpms_csv(x)
        data2 = self.add_column_section_length(data)
        if check_geom:
            geom_check = add_geom_validation_df(
                data2,routeid_field='Route_ID',bmp_field='Begin_Point',emp_field='End_Point')
            self.add_error_df(geom_check[geom_check.IsValid ==
                     False], 'Facility Type geometry check invalid!')
        tmpdf = data2[~data2['Value_Numeric'].isin(self.functional_class_list)]
        self.add_error_df(tmpdf, "Check value numeric for Facility Type valid")
        tmpdf2 = data2[data2['Section_Length'] == 0]
        print(self.rid_overlap(data2))
        self.add_error_df(
            tmpdf2, "Check value numeric for fsystem section length is zero")

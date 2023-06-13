from distutils.log import error
import os
import pandas as pd
from wvdot_utils import add_routeid_df, add_geom_validation_df
from os import listdir
from os.path import isfile, join
import numpy as np

class DomainCheck():

    def __init__(self,df):
        self.df = df
        print(self.df)
        self.cols = ['Error','BeginDate',
        'StateID',
        'RouteID',
        'BeginPoint',
        'EndPoint',
        'DataItem',
        'ValueNumeric',
        'ValueText',
        'ValueDate',
        'Comments']
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
        # self.correct_format={'County_Code':'CountyID','Pct_MC':'PctMotorcycles','Pct_Lgt_Trucks':'PctLightTrucks','Pct_SU_Trucks':'PctSingleUnit','Pct_CU_Trucks':'PctCombination','YEAR_RECORD':'BeginDate','END_POINT':'EndPoint','STATE_CODE':'StateID','ROUTE_ID':'RouteID','VALUE_DATE':'ValueDate','Data_Item':'DataItem','Value_Numeric':'ValueNumeric','Year_Record':'BeginDate','End_Point':'EndPoint','State_Code':'StateID','Value_Date':'ValueDate','Data_Item':'DataItem','Route_ID':'RouteID','Urban_Code':'UrbanID','Value_Text':'ValueText','BEGIN_POINT':'BeginPoint','Begin_Point':'BeginPoint'}
        self.correct_format_inverse = {}
        self.mypath = 'hpms_data_items'
        self.onlyfiles = [f for f in listdir(self.mypath) if isfile(join(self.mypath, f))]
        self.validation_dict = {
    'F_SYSTEM': [self.check_fsystem_valid,'F_SYSTEM'],
    'URBAN_ID': [self.check_urban_code,'URBAN_CODE'],
    'FACILITY_TYPE': [self.check_facility_type,'FACILITY_TYPE'],
    'ACCESS_CONTROL' :[self.access_control,'ACCESS_CONTROL'],
    'OWNERSHIP': [self.check_ownership,'OWNERSHIP'],
    'THROUGH_LANES': [self.check_through_lanes,'THROUGH_LANES'],
    'PEAK_LANES' : [self.peak_lanes,'PEAK_LANES'],
    'COUNTER_PEAK_LANES' : [self.counter_peak_lanes,'COUNTER_PEAK_LANES'],
    'TURN_LANES_R' : [self.turn_lanes_r,'TURN_LANES_R'],
    'TURN_LANES_L' : [self.turn_lanes_l,'TURN_LANES_L'],
    'SPEED_LIMIT' : [self.speed_limit,'SPEED_LIMIT'],
    'TOLL_ID' : [self.toll_id,'TOLL_ID'],
    'ROUTE_NUMBER' : [self.route_number,'ROUTE_NUMBER'],
    'ROUTE_SIGNING' : [self.route_signing,'ROUTE_SIGNING'],
    'ROUTE_QUALIFIER' : [self.route_qualifier,'ROUTE_QUALIFIER'],
    'ALTERNATIVE_ROUTE_NAME' : [self.alternative_route_name,'ALTERNATIVE_ROUTE_NAME'],
    'AADT':[self.check_aadt,'AADT'],
    'AADT_SINGLE_UNIT':[self.check_aadt_single_unit,'AADT_SINGLE_UNIT'],
    'PCT_DH_SINGLE_UNIT':[self.check_pct_peak_single,'PCT_PEAK_SINGLE'],
    'AADT_COMBINATION':[self.check_aadt_combination,'AADT_COMBINATION'],
    'PCT_DH_COMBINATION':[self.check_pct_peak_combination,'PCT_PEAK_COMBINATION'],
    'K_FACTOR' : [self.check_k_factor,'K_FACTOR'],
    'DIR_FACTOR' : [self.dir_factor,'DIR_FACTOR'],
    'FUTURE_AADT' : [self.future_aadt,'FUTURE_AADT'],
    'SIGNAL_TYPE' : [self.signal_type,'SIGNAL_TYPE'],
    'PCT_GREEN_TIME' : [self.pct_green_time,'PERCENT_GREEN_TIME'],
    'NUMBER_SIGNALS' : [self.number_signals,'NUMBER_SIGNALS'],
    'STOP_SIGNS' : [self.stop_signs,'STOP_SIGNS'],
    'AT_GRADE_OTHER' : [self.at_grade_others,'AT_GRADE_OTHER'],
    'LANE_WIDTH' : [self.lane_width,'LANE_WIDTH'],
    'MEDIAN_TYPE' : [self.median_type,'MEDIAN_TYPE'],
    'MEDIAN_WIDTH' : [self.median_width,'MEDIAN_WIDTH'],
    'SHOULDER_TYPE' : [self.shoulder_type,'SHOULDER_TYPE'],
    'SHOULDER_WIDTH_RIGHT' : [self.shoulder_width_r,'SHOULDER_WIDTH_RIGHT'],
    'SHOULDER_WIDTH_LEFT' : [self.shoulder_width_l,'SHOULDER_WIDTH_LEFT'],
    'PEAK_PARKING' : [self.peak_parking,'PEAK_PARKING'],
    'WIDENING_POTENTIAL' : [self.widening_potential,'WIDENING_POTENTIAL'],
    'CURVE_CLASSIFICATION' : [self.curves,'CURVE_CLASSIFICATION'],
    'TERRAIN_TYPE' : [self.terrain_types,'TERRAIN_TYPE'],
    'GRADE_CLASSIFICATION' : [self.grades,'GRADE_CLASSIFICATION'],
    'PCT_PASS_SIGHT' : [self.pct_pass_sight,'PEAK_PASSING_SIGHT'],
    'IRI' : [self.iri,'IRI'],
    'SURFACE_TYPE' : [self.surface_type,'SURFACE_TYPE'],
    'RUTTING' : [self.rutting,'RUTTING'],
    'FAULTING' : [self.faulting,'FAULTING'],
    'CRACKING_PERCENT' : [self.cracking_percent,'CRACKING_PERCENT'],
    'YEAR_LAST_IMPROVEMENT' : [self.year_last_improvement,'YEAR_LAST_IMPROVEMENT'],
    'YEAR_LAST_CONSTRUCTION' : [self.year_last_construction,'YEAR_LAST_CONSTRUCTION'],
    'LAST_OVERLAY_THICKNESS' : [self.last_overlay_thickness,'LAST_OVERLAY_THICKNESS'],
    'THICKNESS_RIGID' : [self.thickness_rigid,'THICKNESS_RIGID'],
    'THICKNESS_FLEXIBLE' : [self.thickness_flexible,'THICKNESS_FLEXIBLE'],
    'BASE_TYPE' : [self.base_type,'BASE_TYPE'],
    'BASE_THICKNESS':[self.base_thickness,'BASE_THICKNESS'],
    'COUNTY_ID':[self.county_id,'COUNTY_ID'],
    'NHS':[self.nhs_new_check,'NHS'],
    'STRAHNET_TYPE':[self.strahnet_type,'STRAHNET_TYPE'],
    'NN':[self.nn,'NN'],
    'DIR_THROUGH_LANES':[self.check_dir_through_lanes,'DIR_THROUGH_LANES'],
    'LANES_WIDTH':[self.lane_width,'LANE_WIDTH']
}
        # for k,v in self.correct_format.items():
        #     self.correct_format_inverse[v] = k


    def get_indexes(self,x):
        pair_list=[]
        for n in range(len(x)):
            for b in range(len(x)):
                pair = [n,b]
                if pair not in pair_list and n!=b and n<b:
                    pair_list.append(pair)
        # print(pair_list)
        return pair_list
   
    def rid_overlap(self,df):
        for rid in df['RouteID'].unique():
            tmp = df[df['RouteID']==rid]

            # print(tmp)
            return self.overlap_intersect_check(tmp)

    def overlap_intersect_check(self,x):
        v=False
        indexes=self.get_indexes(x)
        for i1,i2 in indexes:
            s = x[['RouteID','BeginPoint','EndPoint']].iloc[i1]
            s2 = x[['BeginPoint','EndPoint']].iloc[i2]
            rid,bmp1,emp1 = s['RouteID'],s['BeginPoint'],s['EndPoint']
            bmp2,emp2 = s2['BeginPoint'],s2['EndPoint']
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
        if 'Section_Length' not in df.columns.unique():
            df['Section_Length']=df['EndPoint']-df['BeginPoint']
            return df
        else:
            return df
    
    def add_error_df(self,df,error_message):
        if len(df) > 0:
            df['Error'] = error_message
            print('*******', len([i for i in df.columns.tolist()]))
            tmp = []
            count = 0
            # print('df columns',df.columns)
            # df = df.reset_index()
            # self.total_errors = self.total_errors.reset_index(drop=True)
            print("Sum of Columns",len(self.total_errors.columns))
            print('error', self.total_errors)
            for i in self.total_errors.columns:
                if i not in df.columns:
                    tmp.append(i)

            print(tmp)
            print('Total error df columns',self.total_errors.columns)
            print('DF columns',df.columns)
            self.total_errors = pd.concat([self.total_errors,df],ignore_index=True)
            print('Total Error DATAFRAME',self.total_errors)
            # print("hi, how are you",self.total_errors)
            # self.total_errors= self.total_errors.loc[:,~df.columns.duplicated()].copy()
            print('Wrote Errors df with message:%s and size:%s'% (error_message,len(df)))
        else:
            print('No errors found for error message | %s'% error_message)
    
    def read_hpms_csv(self,x):
        df = pd.read_csv(self.mypath + '/' + x,sep='|')
        return df.rename(columns=self.correct_format)
    
    def get_invalid_geom_name(self,x):
        geom_fn=str.split(x,'.')[0]
        return geom_fn + '_geom_check.xlsx'
    
    def output_geom_file(self,geom_check,fn):
        geom_fn = self.get_invalid_geom_name(fn)
        geom_check[geom_check.IsValid==False].to_excel(geom_fn,index=False)
        print('Output file made : ')
    
    def check_fsystem_valid(self,x,check_geom):
        # data = self.read_hpms_csv(x)
        # data2 = self.add_column_section_length(data)
        print(self.df['F_SYSTEM'].unique())
        if check_geom:
            geom_check =add_geom_validation_df(self.df,routeid_field='RouteID',bmp_field='BeginPoint',emp_field='EndPoint')
            self.add_error_df(geom_check[geom_check.IsValid ==False], 'fsystem geometry check invalid!')
        tmpdf = self.df[~self.df['F_SYSTEM'].isin(self.functional_class_list)]
        self.add_error_df(tmpdf, "Check value numeric for fsystem valid")
        # tmpdf2 = self.df[self.df['Section_Length'] == 0]
        print(self.rid_overlap(self.df))
        # self.add_error_df(
        #     tmpdf2, "Check value numeric for fsystem section length is zero")
    
    def check_urban_code(self,x,check_geom):
        # data = self.read_hpms_csv(x)
        # data2 = self.add_column_section_length(data)
        if check_geom:
            geom_check = add_geom_validation_df(
                self.df,routeid_field='RouteID',bmp_field='BeginPoint',emp_field='EndPoint')
            self.add_error_df(geom_check[geom_check.IsValid ==
                     False], 'Urban Code geometry check invalid!')
        tmpdf = self.df[~self.df['URBAN_ID'].isin(self.urban_code_list)]
        self.add_error_df(tmpdf, "Check value numeric for Urban Code valid")
        # tmpdf2 = data2[['Section_Length'] == 0]
        print(self.rid_overlap(self.df))
        # self.add_error_df(
        #     tmpdf2, "Check value numeric for Urban ID section length is zero")
    
    def check_facility_type(self,x,check_geom):
        # data = self.read_hpms_csv(x)
        # data2 = self.add_column_section_length(data)
        if check_geom:
            geom_check = add_geom_validation_df(
                self.df,routeid_field='RouteID',bmp_field='BeginPoint',emp_field='EndPoint')
            self.add_error_df(geom_check[geom_check.IsValid ==
                     False], 'Facility Type geometry check invalid!')
        tmpdf = self.df[~self.df['FACILITY_TYPE'].isin(self.facility_type_list)]
        self.add_error_df(tmpdf, "Check value numeric for Facility Type valid")
        # tmpdf2 = data2[data2['Section_Length'] == 0]
        print(self.rid_overlap(self.df))
        # self.add_error_df(
        #     tmpdf2, "Check value numeric for Facility Type section length is zero")
    
    def check_ownership(self,x,check_geom):
        # data = self.read_hpms_csv(x)
        # data2 = self.add_column_section_length(data)
        if check_geom:
            geom_check = add_geom_validation_df(
                self.df,routeid_field='RouteID',bmp_field='BeginPoint',emp_field='EndPoint')
            self.add_error_df(geom_check[geom_check.IsValid ==
                     False], 'Ownership geometry check invalid!')
        tmpdf = self.df[~self.df['OWNERSHIP'].isin(self.ownership_list)]
        self.add_error_df(tmpdf, "Check value numeric for Ownership valid")
        # tmpdf2 = data2[data2['Section_Length'] == 0]
        print(self.rid_overlap(self.df))
        # self.add_error_df(
        #     tmpdf2, "Check value numeric for Ownership section length is zero")
    
    def check_through_lanes(self,x,check_geom):
        # data = self.read_hpms_csv(x)
        # data2 = self.add_column_section_length(data)
        if check_geom:
            geom_check = add_geom_validation_df(
                self.df, routeid_field='RouteID', bmp_field='BeginPoint', emp_field='EndPoint')
            self.add_error_df(geom_check[geom_check.IsValid == False], "Though Lanes geometry check invalid")
        
        tmpdf = self.df[self.df['THROUGH_LANES'].isna()]
        self.add_error_df(tmpdf,'Through Lanes value numeric is nan')
        # tmpdf2 = data2[data2['Section_Length']==0]
        # self.add_error_df(tmpdf2,"Through Lanes section length is zero")

    def nhs_new_check(self,x,check_geom):
        # data = self.read_hpms_csv(x)
        # data2 = self.add_column_section_length(data)
        if check_geom:
            geom_check = add_geom_validation_df(self.df,routeid_field='RouteID',bmp_field='BeginPoint',emp_field='EndPoint')
            self.add_error_df(geom_check[geom_check.IsValid ==
                     False], 'NHS geometry check invalid!')
        tmpdf = self.df[self.df['NHS'].isna()]
        self.add_error_df(tmpdf, "NHS Value Numeric is nan")
        # tmpdf2 = data2[data2['Section_Length'] == 0]
        print(self.rid_overlap(self.df))
        # self.add_error_df(
        #     tmpdf2, "Check value numeric for NHS value numeric")
    
    def check_dir_through_lanes(self,x,check_geom):
        # data = self.read_hpms_csv(x)
        # data2 = self.add_column_section_length(data)
        if check_geom:
            geom_check = add_geom_validation_df(self.df,routeid_field='RouteID',bmp_field='BeginPoint',emp_field='EndPoint')
            self.add_error_df(geom_check[geom_check.IsValid ==
                     False], 'Direct Through Lanes geometry check invalid!')
        tmpdf = self.df[self.df['DIR_THROUGH_LANES'].isna()]
        self.add_error_df(tmpdf, "Direct Through Lanes value numeric is nan")
        # tmpdf2 = data2[data2['Section_Length'] == 0]
        print(self.rid_overlap(self.df))
        # self.add_error_df(
        #     tmpdf2, "Direct Through Lanes section length is zero")
    
    def check_aadt(self,x,check_geom):
        # data = self.read_hpms_csv(x)
        # data2 = self.add_column_section_length(data)
        if check_geom:
            geom_check = add_geom_validation_df(self.df,routeid_field='RouteID',bmp_field='BeginPoint',emp_field='EndPoint')
            self.add_error_df(geom_check[geom_check.IsValid ==
                     False], 'AADT geometry check invalid!')
        tmpdf = self.df[self.df['AADT'].isna()]
        self.add_error_df(tmpdf, "Check value numeric for AADT valid")
        # tmpdf2 = data2[data2['Section_Length'] == 0]
        print(self.rid_overlap(self.df))
        # self.add_error_df(
        #     tmpdf2, "Check value numeric for AADT section length is zero")
    
    def check_aadt_single_unit(self,x,check_geom):
        # data = self.read_hpms_csv(x)
        # data2 = self.add_column_section_length(data)
        if check_geom:
            geom_check = add_geom_validation_df(self.df,routeid_field='RouteID',bmp_field='BeginPoint',emp_field='EndPoint')
            self.add_error_df(geom_check[geom_check.IsValid ==
                     False], 'AADT Single Unit geometry check invalid!')
        tmpdf = self.df[self.df['AADT_SINGLE_UNIT'].isna()]
        self.add_error_df(tmpdf, " AADT Single Unit is nan")
        # tmpdf2 = data2[data2['Section_Length'] == 0]
        print(self.rid_overlap(self.df))
        # self.add_error_df(
        #     tmpdf2, "Check value numeric for AADT single Unit section length is zero")
    
    def check_pct_peak_single(self,x,check_geom):
        # data = self.read_hpms_csv(x)
        # data2 = self.add_column_section_length(data)
        if check_geom:
            geom_check = add_geom_validation_df(self.df,routeid_field='RouteID',bmp_field='BeginPoint',emp_field='EndPoint')
            self.add_error_df(geom_check[geom_check.IsValid ==
                     False], 'Percent Peak Single geometry check invalid!')
        tmpdf = self.df[self.df['PCT_DH_SINGLE_UNIT'].isna()]
        self.add_error_df(tmpdf, "Percent Peak Single is nan")
        # tmpdf2 = data2[data2['Section_Length'] == 0]
        print(self.rid_overlap(self.df))
        # self.add_error_df(
        #     tmpdf2, "Check value numeric for Percent Peak Single section length is zero")
    
    def check_aadt_combination(self,x,check_geom):
        # data = self.read_hpms_csv(x)
        # data2 = self.add_column_section_length(data)
        if check_geom:
            geom_check = add_geom_validation_df(self.df,routeid_field='RouteID',bmp_field='BeginPoint',emp_field='EndPoint')
            self.add_error_df(geom_check[geom_check.IsValid ==
                     False], 'AADT combination geometry check invalid!')
        tmpdf = self.df[self.df['AADT_COMBINATION'].isna()]
        self.add_error_df(tmpdf, "AADT combination ")
        # tmpdf2 = data2[data2['Section_Length'] == 0]
        print(self.rid_overlap(self.df))
        # self.add_error_df(
        #     tmpdf2, "Check value numeric for AADT  combination section length is zero")
    
    def check_pct_peak_combination(self,x,check_geom):
        # data = self.read_hpms_csv(x)
        # data2 = self.add_column_section_length(data)
        if check_geom:
            geom_check = add_geom_validation_df(self.df,routeid_field='RouteID',bmp_field='BeginPoint',emp_field='EndPoint')
            self.add_error_df(geom_check[geom_check.IsValid ==
                     False], 'percent peak combination geometry check invalid!')
        tmpdf = self.df[self.df['PCT_DH_COMBINATION'].isna()]
        self.add_error_df(tmpdf, "percent peak combination valid")
        # tmpdf2 = data2[data2['Section_Length'] == 0]
        print(self.rid_overlap(self.df))
        # self.add_error_df(
        #     tmpdf2, "Check value numeric for percent peak combination section length is zero")
    
    def check_k_factor(self,x,check_geom):
        # data = self.read_hpms_csv(x)
        # data2 = self.add_column_section_length(data)
        if check_geom:
            geom_check = add_geom_validation_df(self.df,routeid_field='RouteID',bmp_field='BeginPoint',emp_field='EndPoint')
            self.add_error_df(geom_check[geom_check.IsValid ==
                     False], 'K Factor geometry check invalid!')
        tmpdf = self.df[self.df['K_FACTOR'].isna()]
        self.add_error_df(tmpdf, "Check value numeric for K Factor valid")
        tmpdf2 = self.df[self.df['Section_Length'] == 0]
        print(self.rid_overlap(self.df))
        # self.add_error_df(
        #     tmpdf2, "Check value numeric for K Factor section length is zero")
    
    def dir_factor(self,x,check_geom):
        # data = self.read_hpms_csv(x)
        # data2 = self.add_column_section_length(data)
        if check_geom:
            geom_check = add_geom_validation_df(self.df,routeid_field='RouteID',bmp_field='BeginPoint',emp_field='EndPoint')
            self.add_error_df(geom_check[geom_check.IsValid ==
                     False], 'Direct Factor geometry check invalid!')
        tmpdf = self.df[self.df['DIR_FACTOR'].isna()]
        self.add_error_df(tmpdf, "Direct Factor is nan")
        # tmpdf2 = data2[data2['Section_Length'] == 0]
        print(self.rid_overlap(self.df))
        # self.add_error_df(
        #     tmpdf2, "Check value numeric for Direct Factor section length is zero")
    
    def signal_type(self,x,check_geom):
        # data = self.read_hpms_csv(x)
        # data2 = self.add_column_section_length(data)
        if check_geom:
            geom_check = add_geom_validation_df(self.df,routeid_field='RouteID',bmp_field='BeginPoint',emp_field='EndPoint')
            self.add_error_df(geom_check[geom_check.IsValid ==
                     False], 'AADT geometry check invalid!')
        tmpdf = self.df[~self.df['SIGNAL_TYPE'].isin(self.signal_list)]
        self.add_error_df(tmpdf, "Check value numeric for AADT valid")
        # tmpdf2 = data2[data2['Section_Length'] == 0]
        print(self.rid_overlap(self.df))
        # self.add_error_df(
        #     tmpdf2, "Check value numeric for AADT section length is zero")
    
    def pct_green_time(self,x,check_geom):
        # data = self.read_hpms_csv(x)
        # data2 = self.add_column_section_length(data)
        if check_geom:
            geom_check = add_geom_validation_df(self.df,routeid_field='RouteID',bmp_field='BeginPoint',emp_field='EndPoint')
            self.add_error_df(geom_check[geom_check.IsValid ==
                     False], 'Percent Green Time geometry check invalid!')
        tmpdf = self.df[self.df['PCT_GREEN_TIME'].isna()]
        self.add_error_df(tmpdf, "Percent Green Time is nan")
        # tmpdf2 = data2[data2['Section_Length'] == 0]
        print(self.rid_overlap(self.df))
        # self.add_error_df(
        #     tmpdf2, "Percent Green Time section length is zero")
    
    def lane_width(self,x,check_geom):
        # data = self.read_hpms_csv(x)
        # data2 = self.add_column_section_length(data)
        if check_geom:
            geom_check = add_geom_validation_df(self.df, routeid_field='RouteID', bmp_field='BeginPoint', emp_field='EndPoint')
            self.add_error_df(geom_check[geom_check.IsValid == False], 'Lane Width geometry check invalid')
        tmpdf = self.df[(self.df['LANE_WIDTH'].isna())]
        self.add_error_df(tmpdf, "Lane Width is not in accepted range")
        # tmpdf2 = data2[data2['Section_Length'] == 0]
        # self.add_error_df(tmpdf2, "Lane Width section length is zero")
    
    def median_type(self,x,check_geom):
        # data = self.read_hpms_csv(x)
        # data2=self.add_column_section_length(data)
        if check_geom:
            geom_check = add_geom_validation_df(
                self.df, routeid_field='RouteID', bmp_field='BeginPoint', emp_field='EndPoint')
            self.add_error_df(geom_check[geom_check.IsValid ==
                     False], 'Median type geometry check invalid!')
            tmpdf = self.df[(self.df['MEDIAN_TYPE'].isna()) | (~self.df['MEDIAN_TYPE'].isin(self.median_type_list))]
        self.add_error_df(tmpdf, "Median type is not in accepted range")
        # tmpdf2 = data2[data2['Section_Length'] == 0]
        # self.add_error_df(tmpdf2, "Median type section length is zero")
    
    def median_width(self,x,check_geom):
        # data = self.read_hpms_csv(x)
        # data2=self.add_column_section_length(data)
        if check_geom:
            geom_check = add_geom_validation_df(
            self.df, routeid_field='RouteID', bmp_field='BeginPoint', emp_field='EndPoint')
            self.add_error_df(geom_check[geom_check.IsValid ==
                     False], 'Median width geometry check invalid!')
        tmpdf = self.df[self.df['MEDIAN_WIDTH'].isna()]
        self.add_error_df(tmpdf, "Median width numeric is nan")
        # tmpdf2 = data2[data2['Section_Length'] == 0]
        # self.add_error_df(tmpdf2, "Median width section length is zero")

    def shoulder_type(self,x,check_geom):
        # data = self.read_hpms_csv(x)
        # data2=self.add_column_section_length(data)
        if check_geom:
            geom_check = add_geom_validation_df(
            self.df, routeid_field='RouteID', bmp_field='BeginPoint', emp_field='EndPoint')
        self.add_error_df(geom_check[geom_check.IsValid ==
                     False], 'Shoulder type geometry check invalid!')
        tmpdf = self.df[(self.df['SHOULDER_TYPE'].isna()) | (
    ~self.df['SHOULDER_TYPE'].isin(self.shoulder_type_list))]
        self.add_error_df(tmpdf, "Shoulder type is not in accepted range")
        # tmpdf2 = data2[data2['Section_Length'] == 0]
        # self.add_error_df(tmpdf2, "Shoulder type section length is zero")
    
    def shoulder_width_r(self,x,check_geom):
        # data = self.read_hpms_csv(x)
        # data2=self.add_column_section_length(data)
        if check_geom:
            geom_check = add_geom_validation_df(
            self.df, routeid_field='RouteID', bmp_field='BeginPoint', emp_field='EndPoint')
        self.add_error_df(geom_check[geom_check.IsValid ==
                     False], 'Shoulder width R geometry check invalid!')
        tmpdf = self.df[self.df['SHOULDER_WIDTH_R'].isna()]
        self.add_error_df(tmpdf, "Shoulder width R value numeric is nan")
        # tmpdf2 = data2[data2['Section_Length'] == 0]
        # self.add_error_df(tmpdf2, "Shoulder width R section length is zero")

    def shoulder_width_l(self,x,check_geom):
    #  data = self.read_hpms_csv(x)
    #  data2=self.add_column_section_length(data)
     if check_geom:
         geom_check = add_geom_validation_df(
             self.df, routeid_field='RouteID', bmp_field='BeginPoint', emp_field='EndPoint')
         self.add_error_df(geom_check[geom_check.IsValid ==
                      False], 'Shoulder width L geometry check invalid!')
     tmpdf = self.df[self.df['SHOULDER_WIDTH_L'].isna()]
     self.add_error_df(tmpdf, "Shoulder width L value numeric is nan")
    #  tmpdf2 = data2[data2['Section_Length'] == 0]
    #  self.add_error_df(tmpdf2, "Shoulder width L section length is zero")

    def peak_parking(self,x,check_geom):
        # data = self.read_hpms_csv(x)
        # data2=self.add_column_section_length(data)
        if check_geom:
            geom_check = add_geom_validation_df(
                self.df, routeid_field='RouteID', bmp_field='BeginPoint', emp_field='EndPoint')
            self.add_error_df(geom_check[geom_check.IsValid ==
                         False], 'Peak parking geometry check invalid!')
        tmpdf = self.df[(self.df['PEAK_PARKING'].isna()) | (
            ~self.df['PEAK_PARKING'].isin(self.peak_parking_list))]
        self.add_error_df(tmpdf, "Peak parking is not in accepted range")
        # tmpdf2 = data2[data2['Section_Length'] == 0]
        # self.add_error_df(tmpdf2, "Peak parking section length is zero")
    
    def widening_potential(self,x,check_geom):
        # data = self.read_hpms_csv(x)
        # data2=self.add_column_section_length(data)
        if check_geom:
            geom_check = add_geom_validation_df(
                self.df, routeid_field='RouteID', bmp_field='BeginPoint', emp_field='EndPoint')
            self.add_error_df(geom_check[geom_check.IsValid ==
                         False], 'Widening potential geometry check invalid!')
        tmpdf = self.df[(self.df['WIDENING_POTENTIAL'].isna()) | (
            ~self.df['WIDENING_POTENTIAL'].isin(self.widening_potential_list))]
        self.add_error_df(tmpdf, "Widening potential is not in accepted range")
        # tmpdf2 = data2[data2['Section_Length'] == 0]
        # self.add_error_df(tmpdf2, "widening potential section length is zero")
    
    def curves(self,x,check_geom):
        # data = self.read_hpms_csv(x)
        # data2=self.add_column_section_length(data)
        if check_geom:
            geom_check = add_geom_validation_df(
                self.df, routeid_field='RouteID', bmp_field='BeginPoint', emp_field='EndPoint')
            self.add_error_df(geom_check[geom_check.IsValid ==
                         False], 'curves geometry check invalid!')
        tmpdf = self.df[(self.df['CURVE_CLASSIFICATION'].isna()) | (
            ~self.df['CURVE_CLASSIFICATION'].isin(self.curve_list))]
        self.add_error_df(tmpdf, "Curves is not in accepted range")
        # tmpdf2 = data2[data2['Section_Length'] == 0]
        # self.add_error_df(tmpdf2, "Curves section length is zero")

    def terrain_types(self,x,check_geom):
        # data = self.read_hpms_csv(x)
        # data2=self.add_column_section_length(data)
        if check_geom:
            geom_check = add_geom_validation_df(
                self.df, routeid_field='RouteID', bmp_field='BeginPoint', emp_field='EndPoint')
            self.add_error_df(geom_check[geom_check.IsValid ==
                         False], 'Terrain Type geometry check invalid!')
        tmpdf = self.df[(self.df['TERRAIN_TYPE'].isna()) | (
            ~self.df['TERRAIN_TYPE'].isin(self.terrain_types_list))]
        self.add_error_df(tmpdf, "Terrain Type is not in accepted range")
        # tmpdf2 = data2[data2['Section_Length'] == 0]
        # self.add_error_df(tmpdf2, "Terrain section length is zero")

    def grades(self,x,check_geom):
        # data = self.read_hpms_csv(x)
        # data2=self.add_column_section_length(data)
        if check_geom:
            geom_check = add_geom_validation_df(
                self.df, routeid_field='RouteID', bmp_field='BeginPoint', emp_field='EndPoint')
            self.add_error_df(geom_check[geom_check.IsValid ==
                         False], 'grades geometry check invalid!')
        tmpdf = self.df[(self.df['GRADE_CLASSIFICATION'].isna()) | (
        ~self.df['GRADE_CLASSIFICATION'].isin(self.grades_list))]
        self.add_error_df(tmpdf, "Grade is not in accepted range")
        # tmpdf2 = data2[data2['Section_Length'] == 0]
        # self.add_error_df(tmpdf2, "grades section length is zero")
    
    def pct_pass_sight(self,x,check_geom):
        # data = self.read_hpms_csv(x)
        # data2=self.add_column_section_length(data)
        if check_geom:
            geom_check = add_geom_validation_df(
                self.df, routeid_field='RouteID', bmp_field='BeginPoint', emp_field='EndPoint')
            self.add_error_df(geom_check[geom_check.IsValid ==
                         False], 'PCT Pass Sight geometry check invalid!')
        tmpdf = self.df[self.df['PCT_PASS_SIGHT'].isna()]
        self.add_error_df(tmpdf, "PCT Pass Sight value numeric is nan")
        # tmpdf2 = data2[data2['Section_Length'] == 0]
        # self.add_error_df(tmpdf2, "PCT Pass Sight section length is zero")
    
    def iri(self,x,check_geom):
        # data = self.read_hpms_csv(x)
        # data2=self.add_column_section_length(data)
        if check_geom:
            geom_check = add_geom_validation_df(
                self.df, routeid_field='RouteID', bmp_field='BeginPoint', emp_field='EndPoint')
            self.add_error_df(geom_check[geom_check.IsValid ==
                         False], 'IRI geometry check invalid!')
        tmpdf = self.df[self.df['IRI'].isna()]
        self.add_error_df(tmpdf, "IRI value numeric is nan")
        # tmpdf2 = data2[data2['Section_Length'] == 0]
        # self.add_error_df(tmpdf2, "IRI section length is zero")
    
    def surface_type(self,x,check_geom):
        # data = self.read_hpms_csv(x)
        # data2=self.add_column_section_length(data)
        if check_geom:
            geom_check = add_geom_validation_df(
                self.df, routeid_field='RouteID', bmp_field='BeginPoint', emp_field='EndPoint')
            self.add_error_df(geom_check[geom_check.IsValid ==
                         False], 'Surface Type geometry check invalid!')
        tmpdf = self.df[(self.df['SURFACE_TYPE'].isna()) | (
            ~self.df['SURFACE_TYPE'].isin(self.surface_type_list))]
        self.add_error_df(tmpdf, "Surface_Type value numeric is nan")    
        # tmpdf2 = data2[data2['Section_Length'] == 0]
        # self.add_error_df(tmpdf2, "Surface Type section length is zero")
    
    def rutting(self,x,check_geom):
        # data = self.read_hpms_csv(x)
        # data2=self.add_column_section_length(data)
        if check_geom:
            geom_check = add_geom_validation_df(
                self.df, routeid_field='RouteID', bmp_field='BeginPoint', emp_field='EndPoint')
            self.add_error_df(geom_check[geom_check.IsValid ==
                         False], 'Rutting geometry check invalid!')
        tmpdf = self.df[(self.df['RUTTING'].isna())]
        # tmpdf2 = data2[data2['Section_Length'] == 0]
        self.add_error_df(tmpdf, "Rutting value numeric is nan")
        # self.add_error_df(tmpdf2, "Rutting section length is zero")

    def faulting(self,x,check_geom):
        # data = self.read_hpms_csv(x)
        # data2=self.add_column_section_length(data)
        if check_geom:
            geom_check = add_geom_validation_df(
                self.df, routeid_field='RouteID', bmp_field='BeginPoint', emp_field='EndPoint')
            self.add_error_df(geom_check[geom_check.IsValid ==
                         False], 'Faulting geometry check invalid!')
        tmpdf = self.df[self.df['FAULTING'].isna()]
        self.add_error_df(tmpdf, "Faulting value numeric is nan")
        # tmpdf2 = data2[data2['Section_Length'] == 0]
        # self.add_error_df(tmpdf2, "Faulting section length is zero")
    
    def cracking_percent(self,x,check_geom):
        # data = self.read_hpms_csv(x)
        # data2=self.add_column_section_length(data)
        if check_geom:
            geom_check = add_geom_validation_df(
                self.df, routeid_field='RouteID', bmp_field='BeginPoint', emp_field='EndPoint')
            self.add_error_df(geom_check[geom_check.IsValid ==
                         False], 'Cracking Percent geometry check invalid!')
        tmpdf = self.df[self.df['CRACKING_PERCENT'].isna()]
        self.add_error_df(tmpdf, "Cracking Percent value numeric is nan")
        # tmpdf2 = data2[data2['Section_Length'] == 0]
        # self.add_error_df(tmpdf2, "Cracking Percent section length is zero")
    
    def year_last_improvement(self,x,check_geom):
        # data = self.read_hpms_csv(x)
        # data2=self.add_column_section_length(data)
        if check_geom:
            geom_check = add_geom_validation_df(
                self.df, routeid_field='RouteID', bmp_field='BeginPoint', emp_field='EndPoint')
            self.add_error_df(geom_check[geom_check.IsValid ==
                         False], 'Year Last Improvement geometry check invalid!')
        tmpdf = self.df[self.df['YEAR_LAST_IMPROVEMENT'].isna()] #was ['Value_Date']
        self.add_error_df(tmpdf, "Year Last Improvement value numeric is nan")
        # tmpdf2 = data2[data2['Section_Length'] == 0]
        # self.add_error_df(tmpdf2, "Year Last Improvement section length is zero")
    
    def year_last_construction(self,x,check_geom):
        # data = self.read_hpms_csv(x)
        # data2=self.add_column_section_length(data)
        if check_geom:
            geom_check = add_geom_validation_df(
                self.df, routeid_field='RouteID', bmp_field='BeginPoint', emp_field='EndPoint')
            self.add_error_df(geom_check[geom_check.IsValid ==
                         False], 'Year Last Construction geometry check invalid!')
        tmpdf = self.df[self.df['YEAR_LAST_CONSTRUCTION'].isna()] #was ['Value_Date']
        self.add_error_df(tmpdf, "Year Last Construction value numeric is nan")
        # tmpdf2 = data2[data2['Section_Length'] == 0]
        # self.add_error_df(tmpdf2, "Year Last Construction section length is zero")
    
    def last_overlay_thickness(self,x,check_geom):
        # data = self.read_hpms_csv(x)
        # data2=self.add_column_section_length(data)
        if check_geom:
            geom_check = add_geom_validation_df(
                self.df, routeid_field='RouteID', bmp_field='BeginPoint', emp_field='EndPoint')
            self.add_error_df(geom_check[geom_check.IsValid ==
                         False], 'Last Overlay Thickness geometry check invalid!')
        tmpdf = self.df[self.df['LAST_OVERLAY_THICKNESS'].isna()]
        self.add_error_df(tmpdf, "Last Overlay Thickness value numeric is nan")
        # tmpdf2 = data2[data2['Section_Length'] == 0]
        # self.add_error_df(tmpdf2, "Last Overlay thickness section length is zero")
    
    def thickness_rigid(self,x,check_geom):
        # data = self.read_hpms_csv(x)
        # data2=self.add_column_section_length(data)
        if check_geom:
            geom_check = add_geom_validation_df(
                self.df, routeid_field='RouteID', bmp_field='BeginPoint', emp_field='EndPoint')
            self.add_error_df(geom_check[geom_check.IsValid ==
                         False], 'thickness rigid geometry check invalid!')
        tmpdf = self.df[self.df['THICKNESS_RIGID'].isna()]
        self.add_error_df(tmpdf, "Thickness Rigid value numeric is nan")
        # tmpdf2 = data2[data2['Section_Length'] == 0]
        # self.add_error_df(tmpdf2, "Thickness Rigid section length is zero")
    
    def thickness_flexible(self,x,check_geom):
        # data = self.read_hpms_csv(x)
        # data2=self.add_column_section_length(data)
        if check_geom:
            geom_check = add_geom_validation_df(
                self.df, routeid_field='RouteID', bmp_field='BeginPoint', emp_field='EndPoint')
            self.add_error_df(geom_check[geom_check.IsValid ==
                         False], 'thickness flexible geometry check invalid!')
        tmpdf = self.df[self.df['THICKNESS_FLEXIBLE'].isna()]
        self.add_error_df(tmpdf, "Thickness Flexible value numeric is nan")
        # tmpdf2 = data2[data2['Section_Length'] == 0]
        # self.add_error_df(tmpdf2, "Thickness Flexible section length is zero")
    
    def base_type(self,x,check_geom):
        # data = self.read_hpms_csv(x)
        # data2=self.add_column_section_length(data)
        if check_geom:
            geom_check = add_geom_validation_df(
                self.df, routeid_field='RouteID', bmp_field='BeginPoint', emp_field='EndPoint')
            self.add_error_df(geom_check[geom_check.IsValid ==
                         False], 'thickness flexible geometry check invalid!')
        tmpdf = self.df[self.df['BASE_TYPE'].isna()]
        self.add_error_df(tmpdf, "Thickness Flexible value numeric is nan")
        # tmpdf2 = data2[data2['Section_Length'] == 0]
        # self.add_error_df(tmpdf2, "Thickness Flexible section length is zero")
    
    def base_thickness(self,x,check_geom):
        # data = self.read_hpms_csv(x)
        # data2=self.add_column_section_length(data)
        if check_geom:
            geom_check = add_geom_validation_df(
                self.df, routeid_field='RouteID', bmp_field='BeginPoint', emp_field='EndPoint')
            self.add_error_df(geom_check[geom_check.IsValid ==
                         False], 'Base thickness geometry check invalid!')
        tmpdf = self.df[self.df['BASE_THICKNESS'].isna()]
        self.add_error_df(tmpdf, "Base thickness value numeric is nan")
        # tmpdf2 = data2[data2['Section_Length'] == 0]
        # self.add_error_df(tmpdf2, "Base thickness section length is zero")
    
    def county_id(self,x,check_geom):
        # data = self.read_hpms_csv(x)
        # data2=self.add_column_section_length(data)
        if check_geom:
            geom_check = add_geom_validation_df(
                self.df, routeid_field='RouteID', bmp_field='BeginPoint', emp_field='EndPoint')
            self.add_error_df(geom_check[geom_check.IsValid ==
                         False], 'County ID geometry check invalid!')
        tmpdf = self.df[(self.df['COUNTY_ID'].isna()) | (
        ~self.df['COUNTY_ID'].isin(self.county_id_list))]
        # tmpdf2 = data2[data2['Section_Length'] == 0]
        self.add_error_df(tmpdf, "County ID value numeric is nan")
        # self.add_error_df(tmpdf2, "County ID section length is zero")
    
    def strahnet_type(self,x,check_geom):
        # data = self.read_hpms_csv(x)
        # data2=self.add_column_section_length(data)
        if check_geom:
            geom_check = add_geom_validation_df(
                self.df, routeid_field='RouteID', bmp_field='BeginPoint', emp_field='EndPoint')
            self.add_error_df(geom_check[geom_check.IsValid ==
                         False], 'STRAHNET geometry check invalid!')
        tmpdf = self.df[(self.df['STRAHNET_TYPE'].isna()) | (
        ~self.df['STRAHNET_TYPE'].isin(self.strahnet_type_list))]
        # tmpdf2 = data2[data2['Section_Length'] == 0]
        self.add_error_df(tmpdf, "Strahnet Type value numeric is nan")
        # self.add_error_df(tmpdf2, "STRAHNET section length is zero")
    
    def nn(self,x,check_geom):
        # data = self.read_hpms_csv(x)
        # data2=self.add_column_section_length(data)
        if check_geom:
            geom_check = add_geom_validation_df(
                self.df, routeid_field='RouteID', bmp_field='BeginPoint', emp_field='EndPoint')
            self.add_error_df(geom_check[geom_check.IsValid ==
                         False], 'NN geometry check invalid!')
        tmpdf = self.df[(self.df['NN'].isna()) | (
        ~self.df['NN'].isin(self.nn_list))]
        # tmpdf2 = data2[data2['Section_Length'] == 0]
        self.add_error_df(tmpdf, "NN value numeric error")
        # self.add_error_df(tmpdf2, "NN section length is zero")
    
    def maintenance_operations(self,x,check_geom):
        # data = self.read_hpms_csv(x)
        # data2=self.add_column_section_length(data)
        if check_geom:
            geom_check = add_geom_validation_df(
                self.df, routeid_field='RouteID', bmp_field='BeginPoint', emp_field='EndPoint')
            self.add_error_df(geom_check[geom_check.IsValid ==
                         False], 'Maintence operations geometry check invalid!')
        tmpdf = self.df[(self.df['MAINTENANCE_OPERATIONS'].isna()) | (
        ~self.df['MAINTENANCE_OPERATIONS'].isin(self.   main_ops_list))]
        # tmpdf2 = data2[data2['Section_Length'] == 0]
        self.add_error_df(tmpdf, "Maintenance Operations value numeric error")
        # self.add_error_df(tmpdf2, "Main operations section length is zero")
    
    def dir_through_lanes(self,x,check_geom):
        # data = self.read_hpms_csv(x)
        # data2=self.add_column_section_length(data)
        if check_geom:
            geom_check = add_geom_validation_df(
                self.df, routeid_field='RouteID', bmp_field='BeginPoint', emp_field='EndPoint')
            self.add_error_df(geom_check[geom_check.IsValid ==
                         False], 'Direct Through Lanes geometry check invalid!')
        tmpdf = self.df[self.df['DIR_THROUGH_LANES'].isna()]
        self.add_error_df(tmpdf, "DIrect Through Lanes value numeric is nan")
        # tmpdf2 = data2[data2['Section_Length'] == 0]
        # self.add_error_df(tmpdf2, "Direct Through Lanes section length is zero")
    
    def travel_time_code(self,x,check_geom):
        # data = self.read_hpms_csv(x)
        # data2=self.add_column_section_length(data)
        if check_geom:
            geom_check = add_geom_validation_df(
                self.df, routeid_field='RouteID', bmp_field='BeginPoint', emp_field='EndPoint')
            self.add_error_df(geom_check[geom_check.IsValid ==
                         False], 'Travel Time Code geometry check invalid!')
        tmpdf = self.df[self.df['TRAVEL_TIME_CODE'].isna()]
        self.add_error_df(tmpdf, "Travel Time Code value numeric is nan")
        # tmpdf2 = data2[data2['Section_Length'] == 0]
        # self.add_error_df(tmpdf2, "Travel Time Code section length is zero")
    
    def nhfn(self,x,check_geom):
        # data = self.read_hpms_csv(x)
        # data2=self.add_column_section_length(data)
        if check_geom:
            geom_check = add_geom_validation_df(
                self.df, routeid_field='RouteID', bmp_field='BeginPoint', emp_field='EndPoint')
            self.add_error_df(geom_check[geom_check.IsValid ==
                         False], 'NHFN geometry check invalid!')
        tmpdf = self.df[(self.df['NHFN'].isna()) | (
        ~self.df['NHFN'].isin(self.nhfn_list))]
        self.add_error_df(tmpdf, "NHFN value numeric is nan")
        # tmpdf2 = data2[data2['Section_Length'] == 0]
        # self.add_error_df(tmpdf2, "NHFN section length is zero")
    
    def is_restricted(self,x,check_geom):
        # data = self.read_hpms_csv(x)
        # data2=self.add_column_section_length(data)
        if check_geom:
            geom_check = add_geom_validation_df(
                self.df, routeid_field='RouteID', bmp_field='BeginPoint', emp_field='EndPoint')
            self.add_error_df(geom_check[geom_check.IsValid ==
                         False], 'K Factor geometry check invalid!')
        tmpdf = self.df[(self.df['IS_RESTRICTED'].isna()) | (
        ~self.df['IS_RESTRICTED'].isin(self.is_restricted_list))]
        # tmpdf2 = data2[data2['Section_Length'] == 0]
        self.add_error_df(tmpdf, "Is Restricted value numeric is nan")
        # self.add_error_df(tmpdf2, "is_restricted section length is zero")
    
    def access_control(self,x,check_geom):
        # data = self.read_hpms_csv(x)
        # data2=self.add_column_section_length(data)
        if check_geom:
            geom_check = add_geom_validation_df(
                self.df, routeid_field='RouteID', bmp_field='BeginPoint', emp_field='EndPoint')
            self.add_error_df(geom_check[geom_check.IsValid ==
                         False], 'Access Control geometry check invalid!')
        tmpdf = self.df[(self.df['ACCESS_CONTROL'].isna()) | (
        ~self.df['ACCESS_CONTROL'].isin(self.access_control_list))]
        self.add_error_df(tmpdf, "Access Control value numeric is nan")
        # tmpdf2 = data2[data2['Section_Length'] == 0]
        # self.add_error_df(tmpdf2, "Access Control section length is zero")
    
    def peak_lanes(self,df,check_geom):
        # data = self.read_hpms_csv(x)
        # data2=self.add_column_section_length(data)
        # print(data)
        if check_geom:
            geom_check = add_geom_validation_df(self.df, routeid_field='RouteID', bmp_field='BeginPoint', emp_field='EndPoint')
            self.add_error_df(geom_check[geom_check.IsValid ==False], 'Peak Lanes geometry check invalid!')
        tmpdf = self.df[(self.df['PEAK_LANES'].isna())]
        print('TMPDF of PEAK LANES',tmpdf)
        self.add_error_df(tmpdf, "Peak Lanes value numeric is nan")
        # tmpdf2 = self.df[self.df['Section_Length'] == 0]
        # self.add_error_df(tmpdf2, "Peak Lanes section length is zero")
    
    def counter_peak_lanes(self,x,check_geom):
        # data = self.read_hpms_csv(x)
        # data2=self.add_column_section_length(data)
        # print(data)
        if check_geom:
            geom_check = add_geom_validation_df(
                self.df, routeid_field='RouteID', bmp_field='BeginPoint', emp_field='EndPoint')
            self.add_error_df(geom_check[geom_check.IsValid ==
                         False], 'Counter Peak Lanes geometry check invalid!')
        tmpdf = self.df[(self.df['COUNTER_PEAK_LANES'].isna())]
        self.add_error_df(tmpdf, "Counter Peak Lanes value numeric is nan")
        # tmpdf2 = data2[data2['Section_Length'] == 0]
        # self.add_error_df(tmpdf2, "Counter Peak Lanes section length is zero")
    
    def turn_lanes_r(self,x,check_geom):
        # data = self.read_hpms_csv(x)
        # data2=self.add_column_section_length(data)
        # print(data)
        if check_geom:
            geom_check = add_geom_validation_df(
                self.df, routeid_field='RouteID', bmp_field='BeginPoint', emp_field='EndPoint')
            self.add_error_df(geom_check[geom_check.IsValid ==
                         False], 'Turn Lanes R geometry check invalid!')
        tmpdf = self.df[(self.df['TURN_LANES_R'].isna())]
        # tmpdf2 = data2[data2['Section_Length'] == 0]
        self.add_error_df(tmpdf, "Turn Lanes R value numeric is nan")
        # self.add_error_df(tmpdf2, "Turn Lanes R section length is zero")
    
    def turn_lanes_l(self,x,check_geom):
        # data = self.read_hpms_csv(x)
        # data2=self.add_column_section_length(data)
        # print(data)
        if check_geom:
            geom_check = add_geom_validation_df(
                self.df, routeid_field='RouteID', bmp_field='BeginPoint', emp_field='EndPoint')
            self.add_error_df(geom_check[geom_check.IsValid ==
                         False], 'Turn Lanes L geometry check invalid!')
        tmpdf = self.df[(self.df['TURN_LANES_L'].isna())]
        # tmpdf2 = data2[data2['Section_Length'] == 0]
        self.add_error_df(tmpdf, "Turn Lanes L value numeric is nan")
        # self.add_error_df(tmpdf2, "Turn Lanes L section length is zero")
    
    def speed_limit(self,x,check_geom):
        # data = self.read_hpms_csv(x)
        # data2=self.add_column_section_length(data)
        # print(data)
        if check_geom:
            geom_check = add_geom_validation_df(
                self.df, routeid_field='RouteID', bmp_field='BeginPoint', emp_field='EndPoint')
            self.add_error_df(geom_check[geom_check.IsValid ==
                         False], 'Speed Limit geometry check invalid!')
        tmpdf = self.df[(self.df['SPEED_LIMIT'].isna())]
        # tmpdf2 = data2[data2['Section_Length'] == 0]
        self.add_error_df(tmpdf, "Speed Limit value numeric is nan")
        # self.add_error_df(tmpdf2, "Speed Limit section length is zero")
    
    def toll_id(self,x,check_geom):
        # data = self.read_hpms_csv(x)
        # data2=self.add_column_section_length(data)
        # print(data)
        if check_geom:
            geom_check = add_geom_validation_df(
                self.df, routeid_field='RouteID', bmp_field='BeginPoint', emp_field='EndPoint')
            self.add_error_df(geom_check[geom_check.IsValid ==
                         False], 'Toll ID geometry check invalid!')
        tmpdf = self.df[(self.df['TOLL_ID'].isna()) | (self.df['TOLL_ID'] != 288)]
        # tmpdf2 = data2[data2['Section_Length'] == 0]
        self.add_error_df(tmpdf, "Toll ID value numeric is nan")
        # self.add_error_df(tmpdf2, "Toll ID section length is zero")
    
    def route_number(self,x,check_geom):
        # data = self.read_hpms_csv(x)
        # data2=self.add_column_section_length(data)
        # print(data)
        if check_geom:
            geom_check = add_geom_validation_df(
                self.df, routeid_field='RouteID', bmp_field='BeginPoint', emp_field='EndPoint')
            self.add_error_df(geom_check[geom_check.IsValid ==
                         False], 'Route Number geometry check invalid!')
        tmpdf = self.df[(self.df['ROUTE_NUMBER'].isna())]
        # tmpdf2 = data2[data2['Section_Length'] == 0]
        self.add_error_df(tmpdf, "Route Number value numeric is nan")
        # self.add_error_df(tmpdf2, "Route Number section length is zero")
    
    def route_signing(self,x,check_geom):
        # data = self.read_hpms_csv(x)
        # data2=self.add_column_section_length(data)
        # print(data)
        if check_geom:
            geom_check = add_geom_validation_df(
                self.df, routeid_field='RouteID', bmp_field='BeginPoint', emp_field='EndPoint')
            self.add_error_df(geom_check[geom_check.IsValid ==
                         False], 'Route Signing geometry check invalid!')
        tmpdf = self.df[(self.df['ROUTE_SIGNING'].isna())| ~self.df['ROUTE_SIGNING'].isin(self.route_qualifier_list)]
        # tmpdf2 = data2[data2['Section_Length'] == 0]
        self.add_error_df(tmpdf, "Route Signing value numeric is nan or not in list")
        # self.add_error_df(tmpdf2, "Route Signing section length is zero")

    def route_qualifier(self,x,check_geom):
        # data = self.read_hpms_csv(x)
        # data2=self.add_column_section_length(data)
        # print(data)
        if check_geom:
            geom_check = add_geom_validation_df(
                self.df, routeid_field='RouteID', bmp_field='BeginPoint', emp_field='EndPoint')
            self.add_error_df(geom_check[geom_check.IsValid ==
                         False], 'Route Qualifier geometry check invalid!')
        tmpdf = self.df[(self.df['ROUTE_QUALIFIER'].isna()) | ~self.df['ROUTE_QUALIFIER'].isin(self.route_signing_list) ]
        # tmpdf2 = data2[data2['Section_Length'] == 0]
        self.add_error_df(tmpdf, "Route Qualifier value numeric is nan or not in list")
        # self.add_error_df(tmpdf2, "Route Qualifier section length is zero")
    
    def alternative_route_name(self,x,check_geom):
        # data = self.read_hpms_csv(x)
        # print(.columns)
        # data2=self.add_column_section_length(data)
        # print('Should Have Section Length : ',data2.columns)
        # print(data)
        if check_geom:
            geom_check = add_geom_validation_df(
                self.df, routeid_field='RouteID', bmp_field='BeginPoint', emp_field='EndPoint')
            self.add_error_df(geom_check[geom_check.IsValid ==
                         False], 'Alternative Route Name geometry check invalid!')
        tmpdf = self.df[(self.df['ALTERNATIVE_ROUTE_NAME'].isna())] #was ['Value_Text']
        # tmpdf2 = data2[data2['Section_Length'] == 0]
        self.add_error_df(tmpdf, "Alternative Route Name value text is nan")
        # self.add_error_df(tmpdf2, "Alternative Route Name section length is zero")
    
    def future_aadt(self,x,check_geom):
        # data = self.read_hpms_csv(x)
        # data2=self.add_column_section_length(data)
        # print(data)
        if check_geom:
            geom_check = add_geom_validation_df(
                self.df, routeid_field='RouteID', bmp_field='BeginPoint', emp_field='EndPoint')
            self.add_error_df(geom_check[geom_check.IsValid ==
                         False], 'Future AADT geometry check invalid!')
        tmpdf = self.df[(self.df['FUTURE_AADT'].isna())]
        # tmpdf2 = data2[data2['Section_Length'] == 0]
        self.add_error_df(tmpdf, "Future AADT value numeric is nan or not in list")
        # self.add_error_df(tmpdf2, "Future AADT section length is zero")
    
    def number_signals(self,x,check_geom):
        # data = self.read_hpms_csv(x)
        # data2=self.add_column_section_length(data)
        # print(data)
        if check_geom:
            geom_check = add_geom_validation_df(
                self.df, routeid_field='RouteID', bmp_field='BeginPoint', emp_field='EndPoint')
            self.add_error_df(geom_check[geom_check.IsValid ==
                         False], 'Number signals geometry check invalid!')
        tmpdf = self.df[(self.df['NUMBER_SIGNALS'].isna())]
        # tmpdf2 = data2[data2['Section_Length'] == 0]
        self.add_error_df(tmpdf, "Number Signals value numeric is nan or not in list")
        # self.add_error_df(tmpdf2, "Number Signals section length is zero")
    
    def stop_signs(self,x,check_geom):
        # data = self.read_hpms_csv(x)
        # data2=self.add_column_section_length(data)
        # print(data)
        if check_geom:
            geom_check = add_geom_validation_df(
                self.df, routeid_field='RouteID', bmp_field='BeginPoint', emp_field='EndPoint')
            self.add_error_df(geom_check[geom_check.IsValid ==
                         False], 'Stop Signs geometry check invalid!')
        tmpdf = self.df[(self.df['STOP_SIGNS'].isna())]
        # tmpdf2 = data2[data2['Section_Length'] == 0]
        self.add_error_df(tmpdf, "Stop Signs value numeric is nan or not in list")
        # self.add_error_df(tmpdf2, "Stop Signs section length is zero")
    
    def at_grade_others(self,x,check_geom):
        # data = self.read_hpms_csv(x)
        # data2=self.add_column_section_length(data)
        # print(data)
        if check_geom:
            geom_check = add_geom_validation_df(
                self.df, routeid_field='RouteID', bmp_field='BeginPoint', emp_field='EndPoint')
            self.add_error_df(geom_check[geom_check.IsValid ==
                         False], 'At Grade Others geometry check invalid!')
        tmpdf = self.df[(self.df['AT_GRADE_OTHER'].isna())]
        # tmpdf2 = data2[data2['Section_Length'] == 0]
        self.add_error_df(tmpdf, "At Grade Others value numeric is nan or not in list")
        # self.add_error_df(tmpdf2, "At Grade Others section length is zero")
    
    def check_columns(self,fn,col):
        df = self.read_hpms_csv(fn)
        print(df.columns)
        self.add_error_df(df[df['DataItem']!=col],'%s field not in DataItem Column' % col)
    
    # def check_all(self,check_geom=True):
        # for fn in self.onlyfiles:
        #     print(fn, "processing file")
        #     vals = self.validation_dict.get(fn, False)
        #     if vals != False:
        #         myfunc,col = vals 
        #         self.check_columns(fn,col)
        #         myfunc(fn, check_geom=check_geom)
        # print("Total Error Columns : ",self.total_errors.columns)
        # return self.total_errors[self.cols].to_excel('errors.xlsx', index=False)
    def check_all(self,check_geom=True):
        print(self.df)
        for fn in self.df:
            vals = self.validation_dict.get(fn,False)
            print(vals,'This here woodchuck')
            if vals != False:
                myfunc,col = vals
                print(myfunc(fn,check_geom=check_geom))
                # print(myfunc(fn))
            else:
                print(vals,'Vals are false')
        
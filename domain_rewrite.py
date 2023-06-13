import pandas as pd
import geopandas as gpd



class DomainCheck():
    def __init__(self, df):
        self.master = df
        # print(self.master.columns.to_list())
        self.errors = pd.DataFrame()

    # def check_for_na(self,df):
    #     for column in df.columns:
    #         for value in df[column.astype('string')]:
    #             if value.isna():
    #                 return df[column.astype('string')]

    def check_for_na(self,df):
        tmp = df[['ROUTE_ID','BEGIN_POINT,"END_POINT']].loc[df[['THROUGH_LANES','NHS','DIR_THROUGH_LANES','AADT','AADT_SINGLE_UNIT','PCT_DH_SINGLE_UNIT','AADT_COMBINATION','PCT_DH_COMBINATION','K_FACTOR','DIR_FACTOR','PCT_GREEN_TIME','LANE_WIDTH','MEDIAN_TYPE','MEDIAN_WIDTH','SHOULDER_WIDTH_R','SHOULDER_WIDTH_L','PCT_PASS_SIGHT','IRI','RUTTING','FAULTING','CRACKING_PERCENT','YEAR_LAST_IMPROVEMENT','YEAR_LAST_CONSTRUCTION','LAST_OVERLAY_THICKNESS','THICKNESS_FLEXIBLE','BASE_TYPE','BASE_THICKNESS','TRAVEL_TIME_CODE','PEAK_LANES','COUNTER_PEAK_LANES','TURN_LANES_R','TURN_LANES_L','SPEED_LIMIT','ROUTE_NUMBER','ALT_ROUTE_NAME','FUTURE_AADT','NUMBER_SIGNALS','STOP_SIGNS','AT_GRADE_OTHER']].isna().any()]
        self.errors = pd.concat(self.errors,tmp)


    def f_system(self,df):
        errors = df[~df['F_SYSTEM'].isin([1,2,3,4,5,6,7])]
        self.errors = pd.concat([self.errors, errors])

    def urban_code(self,df):
        errors = df[~df['URBAN_ID'].astype('string').isin(['99999','99998','15481','06139','21745','36190','40753','59275','67672','93592','94726'])]
        self.errors = pd.concat([self.errors, errors])
    
    def facility_type(self,df):
        errors = df[~df['FACILITY_TYPE'].isin([1,2,4,5,6,7])]
        self.errors = pd.concat([self.errors,errors])
    
    def access_control(self,df):
        errors = df[~df['ACCESS_CONTROL'].isin([1,2,3])]
        self.errors = pd.concat([self.errors,errors])    

    def ownership(self,df):
        errors = df[~df['OWNERSHIP'].isin([1,2,3,4,11,12,21,25,26,27,31])]
        self.errors = pd.concat([self.errors,errors])

    def signal_type(self,df):
        errors = df[~df['SIGNAL_TYPE'].isin([1,2,3,4,5])]
        self.errors = pd.concat([self.errors,errors])

    def shoulder_type(self,df):
        errors = df[~df['SHOULDER_TYPE'].isin([1,2,3,4,5,6])]
        self.errors = pd.concat([self.errors,errors])

    def peak_parking(self,df):
        errors = df[~df['PEAK_PARKING'].isin([1,2,3])]
        self.errors = pd.concat([self.errors,errors])
    
    def widening_potential(self,df):
        errors = df[~df['WIDENING_POTENTIAL'].isin([1,2,3,4])]
        self.errors = pd.concat([self.errors,errors])

    def curve_classifcation(self,df):
        errors = df[~df['CURVE_CLASSIFICATION'].astype('string').isin(['A','B','C','D','E','F'])]  
        self.errors = pd.concat([self.errors,errors])
    
    def terrain_types(self,df):
        errors = df[~df['TERRAIN_TYPE'].isin([1,2,3])]
        self.errors = pd.concat([self.errors,errors])
    
    def grade_classification(self,df):
        errors = df[~df['GRADE_CLASSIFICATION'].astype('string').isin(['A','B','C','D','E','F'])]
        self.errors = pd.concat([self.errors,errors])
    
    def surface_type(self,df):
        errors = df[~df['SURFACE_TYPE'].isin([1,2,3,4,5,6,7,8,9,10,11])]
        self.errors = pd.concat([self.errors,errors])
               
    def count_id(self,df):
        errors = df[~df['COUNTY_ID'].isin([1,3,5,7,9,11,13,15,17,19,21,23,25,27,29,31,33,35,37,39,41,43,45,47,49,51,53,55,57,59,61,63,65,67,69,71,73,75,77,79,91,93,95,97,99,101,103,105,107,109,111])]
        self.errors = pd.concat([self.errors,errors])
    
    def strahnet_type(self,df):
        errors = df[~df['STRAHNET'].isin([1,2])]
        self.errors = pd.concat([self.errors,errors])
    
    def nn(self,df):
        errors = df[~df['NN'].isin([1,2])]
        self.errors = pd.concat([self.errors,errors])
    
    def maintenance_operations(self,df):
        errors = df[~df['MAINTENANCE_OPERATIONS'].isin([1,2,3,4,11,12,21,25,26,27])]
        self.errors = pd.concat([self.errors,errors])

    def nhfn(self,df):
        errors = df[~df['NHFN'].isin([1,2,3])]
        self.errors = pd.concat([self.errors,errors])
    
    def is_restricted(self,df):
        errors = df[~df['IS_RESTRICTED'].isin([1])]
        self.errors = pd.concat([self.errors,errors])
    
    def toll_id(self,df):
        errors = df[df['TOLL_ID']!=288]
        self.errors = pd.concat([self.errors,errors])

    def route_signing(self,df):
        errors = df[~df['ROUTE_SIGNING'].isin([1,2,3,4,5,6,7,8,9,10])]
        self.errors = pd.concat([self.errors,errors])
    
    def route_qualifier(self,df):
        errors = df[~df['ROUTE_QUALIFIER'].isin([1,2,3,4,5,6,7,8,9,10])]
        self.errors = pd.concat([self.errors,errors])

    def main(self):
        self.f_system(self.master)
        self.urban_code(self.master)
        self.count_id(self.master)
        self.access_control(self.master)
        self.curve_classifcation(self.master)
        self.nn(self.master)
        self.facility_type(self.master)
        self.grade_classification(self.master)
        self.ownership(self.master)
        self.shoulder_type(self.master)
        self.signal_type(self.master)
        self.surface_type(self.master)
        self.terrain_types(self.master)
        self.toll_id(self.master)
        self.widening_potential(self.master)
        self.strahnet_type(self.master)
        # self.travel_time_code(self.master)
        self.maintenance_operations(self.master)
        self.peak_parking(self.master)
        self.route_signing(self.master)
        self.peak_parking(self.master)
        self.route_qualifier(self.master)
        # return self.errors.to_csv('domain_errors_df.csv',sep = '|',index = False)
        return self.errors
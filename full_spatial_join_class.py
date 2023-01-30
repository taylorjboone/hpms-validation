import pandas as pd
import numpy as np
from pandarallel import pandarallel
from datetime import date,timedelta
from dateutil.relativedelta import relativedelta
import random
pandarallel.initialize()
from os import listdir
from os.path import isfile,join


class full_spatial_join_class:

    def __init__(self,filename):
        self.df=pd.read_csv(filename,sep='|')
        self.f_system_dict = {
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
        self.facility_list = [1,2,4,5,6]
        self.facility_list2 = [1,2,4]
        self.f_system_list = [1,2,3,4,5,6,7]
        self.urban_id_list=['06139',15481,21745,36190,40753,59275,67672,93592,94726]
        self.convert_dict={
        'BMP':'BeginPoint',
        'EMP':'EndPoint',
        '12_COUNTY':'COUNTY_ID',
        '2_AADT':'AADT',
        '2_FUTURE_AADT':'FUTURE_AADT',
        '3_ACCESS_CONTROL':'ACCESS_CONTROL',
        '4_ALT_ROUTE_NAME':'ALT_ROUTE_NAME',
        '5_AT_GRADE_OTHER':'AT_GRADE_OTHER',
        '6_AVG_LANE_WIDTH_FT':'LANE_WIDTH',
        '8_BASE_TYPE':'BASE_TYPE',
        '14_CRACKING_PERCENT':'CRACKING_PERCENT',
        '16_CURVE_CLASS':'CURVE_CLASSIFICATION',
        '17_DES_TRUCK_ROUTE':'NN',
        '20_FACILITY':'FACILITY_TYPE',
        '21_FAULTING':'FAULTING',
        '25_GRADE_CLASS':'GRADE_CLASSIFICATION',
        '29_HPMS_SAMPLE_NO':'is_sample',
        '30_IRI_VALUE':'IRI',
        '34_MEDIAN_WIDTH_FT':'MEDIAN_WIDTH',
        '34_HPMS_MEDIAN_BARRIER_TYPE':'MEDIAN_TYPE',
        '36_NHS':'NHS',
        '37_NUMBER_SIGNALS':'NUMBER_SIGNALS',
        '39_OWNERSHIP':'OWNERSHIP',
        '41_PCT_GREEN_TIME':'PCT_GREEN_TIME',
        '42_PCT_PASS_SIGHT':'PCT_PASS_SIGHT',
        '43_PEAK_LANES':'PEAK_LANES',
        '43_COUNTER_PEAK_LANES':'COUNTER_PEAK_LANES',
        '45_PSR':'PSR',
        '52_RUTTING':'RUTTING',
        '56_SHOULDER_TYPE_RT':'SHOULDER_TYPE',
        '57_SHOULDER_WIDTH_LFT_FT':'SHOULDER_WIDTH_L',
        '58_SHOULDER_WIDTH_RT_FT':"SHOULDER_WIDTH_R",
        '59_SIGNAL_TYPE':'SIGNAL_TYPE',
        '63_SPEED_LIMIT_MPH':'SPEED_LIMIT',
        '65_STATE_FUNCTIONAL_CLASS':'F_SYSTEM',
        '66_STOP_SIGNS':'STOP_SIGNS',
        '70_SURFACE_TYPE':'SURFACE_TYPE',
        '71_TERRAIN_TYPE':'TERRAIN_TYPE',
        '74_NUM_THROUGH_LANES':'THROUGH_LANES',
        '75_TOLL_CHARGED':'TOLL_ID',
        '76_TOLL_CHARGED':'TOLL_ID',
        '77_AADT_SINGLE':'AADT_SINGLE_UNIT',
        '77_AADT_COMBINATION':'AADT_COMBINATION',
        '77_PCT_PEAK_SINGLE':'PCT_DH_SINGLE_UNIT',
        '77_PCT_PEAK_COMBINATION':'PCT_DH_COMBINATION',
        '77_K_FACTOR':"K_FACTOR",
        '77_DIR_FACTOR':'DIR_FACTOR',
        '80_TURN_LANES_LFT':'TURN_LANES_L',
        '81_TURN_LANES_R':'TURN_LANES_R',
        '83_URBAN_CODE':'URBAN_ID',
        '84_WIDENING_OBSTACLE':'WIDENING_POTENTIAL',
        '85_WIDENING_POTENTIAL':'WIDENING_POTENTIAL',
        '88_YEAR_LAST_IMPROV':'YEAR_LAST_IMPROVEMENT',
        '115_STRAHNET':'STRAHNET'}

    def dummy_data(self):
        self.df['RouteID']=self.df['RouteID'].astype(str)
        self.df['sup_code'] = self.df['RouteID'].str.slice(9,11)
        self.df['BeginDate'] = date.fromisoformat('2022-12-31')
        self.df['sign_system'] = self.df['RouteID'].str.slice(2,3)
        self.df['section_length'] = self.df['EMP'] - self.df['BMP']
        self.df['ValueDate'] =date.fromisoformat('2022-10-31')
        self.df['ValueText'] ='A'
        self.df['TRAVEL_TIME_CODE'] = 'asdfasdfasf'
        self.df['YEAR_LAST_CONSTRUCTION'] =date.fromisoformat('2010-12-31') 
        self.df['THICKNESS_RIGID']=np.random.randint(1, 20, self.df.shape[0])
        self.df['THICKNESS_FLEXIBLE']=np.random.randint(1, 20, self.df.shape[0])
        self.df['MAINTENANCE_OPERATIONS']=np.random.randint(1, 12, self.df.shape[0])
    
    def create_data_files(self):
        mypath = 'C:\PythonTest\Voltron\district_chrystal_report_website\hpms-validation\hpms_data_items'
        onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))]
        for a in onlyfiles:
            b= pd.read_csv(mypath + a,sep='|')
        return b

        
meh=full_spatial_join_class()
print(meh.create_data_files)
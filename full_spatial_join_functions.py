import pandas as pd

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
urban_id_list = ['06139','15481','21745','36190','40753','59275','67672','93592','94726']



class full_spatial_functions():
    
    
    
    def __init__(self,df):
        self.error_df = pd.DataFrame()
        self.df = df
        self.urban_id = self.df['URBAN_ID']
        self.facility_type = self.df['FACILITY_TYPE']
        self.f_system = self.df['F_System']
        self.dir_through_lanes = self.df['DIR_THROUGH_LANES']
        self.iri = self.df['IRI']
        self.samples = self.df['is_sample']
        self.through_lanes = self.df['THROUGH_LANES']
        self.access_control = self.df['ACCESS_CONTROL']
        self.turn_lanes_l = self.df['TURN_LANES_L']
        self.peak_lanes = self.df['PEAK_LANES']
        self.counter_peak_lanes = self.df['COUNTER_PEAK_LANES']
    
    def sjf01(self):
        tmp_errors = (~((self.f_system.notna())&(self.facility_type.isin([1,2,3,4,5,6]))))
        print('SJF01 Completed',tmp_errors)
        return tmp_errors
        # tmp_errors['ErrorMessage'] ='F_SYSTEM must exist where FACILITY_TYPE is in (1;2;4;5;6) and Must not be NULL'
        # pd.concat([self.error_df,tmp_errors],ignore_index=True)
    
    def sjf02(self):
        tmp_errors = (~((self.urban_id.notna())&\
        (self.facility_type.isin(facility_list2))&\
        (self.f_system.isin(f_system_list)))|\
        ((self.facility_type==6)&\
        (self.dir_through_lanes>0)&\
        (self.f_system==1)&\
        (self.iri.notna())))
        print('SJF02 Completed',tmp_errors)
        
        return tmp_errors
    
    def sjf03(self):
        tmp_errors = (~((self.facility_type.notna())&\
        (self.f_system.isin(f_system_list))))
        print('SJF03 Completed',tmp_errors)
        return tmp_errors
    
    def sjf04(self):
        return True
    
    def sjf05(self):
        tmp_errors = (~( (self.f_system.isin([1,2,3]))& (self.facility_type.isin([1,2])) ) )
        print('sjf05',tmp_errors)
        return tmp_errors
    
    def sjf06(self):
        tmp_errors = (~((self.f_system.isin([1,2,3,4,5,6,7]))&(self.facility_type.isin([1,2,5,6]))))
        print('sjf06',tmp_errors)
        return tmp_errors
    
    def sjf07(self):
        tmp_errors = (~((self.facility_type.isin([1,2,4]))&(self.f_system.isin([1,2,5,6]))))
        print('sjf07',tmp_errors)
        return tmp_errors
    
    def sjf08(self):
        return True
    
    def sjf09(self):
        return True
    
    def sjf10(self):
        tmp_errors = (~((self.peak_lanes.notna())&(self.samples.notna())))
        print('sjf10',tmp_errors)
        return tmp_errors
    
    def sjf11(self):
        tmp_errors = (~((self.counter_peak_lanes.notna())&(self.samples.notna())&(self.facility_type==2)&((self.urban_id<99999)|(self.through_lanes>=4))))
        print('sjf11',tmp_errors)
        return tmp_errors
    
    def sjf12(self):
        tmp_errors = (~((self.samples.notna())&(self.urban_id<99999)&(self.access_control>1)))
        print('sjf12',tmp_errors)
        return tmp_errors
    
    def sjf13(self):
        tmp_errors = (~(self.samples.notna())&())

    

    
    
    
    
    
    
    
    
    
    
    def run(self):
        #when it returns false, it means the data has no errors itself
        self.df['SJF-01'] = self.sjf01()
        self.df['SJF-02'] = self.sjf02()
        self.df['SJF-03'] = self.sjf03()
        # self.df['SJF-04'] = self.sjf04()
        self.df['SJF-05'] = self.sjf05()
        self.df['SJF-06'] = self.sjf06()
        self.df['SJF-07'] = self.sjf07()
        # self.df['SJF-08'] = self.sjf08()
        # self.df['SJF-09'] = self.sjf09()
        self.df['SJF-10'] = self.sjf10()
        self.df['SJF-11'] = self.sjf11()
        self.df['SJF-12'] = self.sjf12()
        print(self.df)



df = pd.read_csv('better_test_data.csv')

c = full_spatial_functions(df)  
# c.run()
c.run()

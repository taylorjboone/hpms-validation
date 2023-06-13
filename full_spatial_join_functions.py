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
        tmp_errors = (~(self.facility_type.notna())&\
        (self.f_system.isin(f_system_list)))
        print('SJF03 Completed',tmp_errors)
        return tmp_errors
    

    
    
    
    
    
    
    
    
    
    
    def run(self):
        self.df['SJF-01'] = self.sjf01()
        self.df['SJF-02'] = self.sjf02()
        self.df['SJF-03'] = self.sjf03()
        print(self.df)



df = pd.read_csv('better_test_data.csv')

c = full_spatial_functions(df)  
# c.run()
c.run()

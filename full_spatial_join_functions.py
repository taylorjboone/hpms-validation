import pandas as pd
from datetime import datetime,timedelta
from dateutil.relativedelta import relativedelta

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
  #if the sjf columsn return false, 
  # it means the data passes that validation, 
  # if the sjf column returns True, the
  # data faios the validation
  # Comments in the code are the actual rule that is being applied in the function  
    
    
    def __init__(self,df):
        self.error_df = pd.DataFrame()
        self.df = df
        self.urban_id = self.df['URBAN_ID']
        self.facility_type = self.df['FACILITY_TYPE']
        self.f_system = self.df['F_SYSTEM']
        self.dir_through_lanes = self.df['DIR_THROUGH_LANES']
        self.iri = self.df['IRI']
        self.samples = self.df['IS_SAMPLE']
        self.through_lanes = self.df['THROUGH_LANES']
        self.access_control = self.df['ACCESS_CONTROL']
        self.turn_lanes_l = self.df['TURN_LANES_L']
        self.turn_lanes_r = self.df['TURN_LANES_R']
        self.peak_lanes = self.df['PEAK_LANES']
        self.counter_peak_lanes = self.df['COUNTER_PEAK_LANES']
        self.speed_limit = self.df['SPEED_LIMIT']
        self.nhs = self.df['NHS']
        self.ownership = self.df['OWNERSHIP']
        self.toll_id = self.df['TOLL_ID']
        self.route_number = self.df['ROUTE_NUMBER']
        self.route_signing = self.df['ROUTE_SIGNING']
        self.route_qualifier = self.df['ROUTE_QUALIFIER']
        self.route_name = self.df['ROUTE_NAME']
        self.aadt = self.df['AADT']
        self.aadt_single_unit = self.df['AADT_SINGLE_UNIT']
        self.pct_dh_single_unit = self.df['PCt_DH_SINGLE_UNIT']
        self.aad_combination = self.df['AADT_COMBINATION']
        self.pct_dh_combination = self.df['PCT_DH_COMBINATION']
        self.k_factor = self.df['K_FACTOR']
        self.dir_factor = self.df['DIR_FACTOR']
        self.future_aadt = self.df['FUTURE_AADT']
        self.signal_type = self.df['SIGNAL_TYPE']
        self.number_signals = self.df['NUMBER_SIGNALS']
        self.pct_green_time = self.df['PCT_GREEN_TIME']
        self.stop_signs = self.df['STOP_SIGNS']
        self.at_grade_other = self.df['AT_GRADE_OTHER']
        self.lane_width = self.df['LANE_WIDTH']
        self.median_type = self.df['MEDIAN_TYPE']
        self.median_width = self.df['MEDIAN_WIDTH']
        self.shoulder_type = self.df['SHOULDER_TYPE']
        self.shoulder_width_r = self.df['SHOULDER_WIDTH_R']
        self.shoulder_width_l = self.df['SHOULDER_WIDTH_L']
        self.widening_potential = self.df['WIDENING_POTENTIAL']
        self.peak_parking = self.df['PEAK_PARKING']
        self.curve_classification = self.df['CURVE_CLASSIFICATION']
        self.surface_type = self.df['SURFACE_TYPE']
        self.terrain_type = self.df['TERRAIN_TYPE']
        self.grade_classification = self.df['GRADE_CLASSIFICATION']
        self.pct_pass_sight = self.df['PCT_PASS_SIGHT']
        self.psr_value_text = self.df['PSR_VALUE_TEXT']
        self.psr = self.df['PSR']
        self.rutting = self.df['RUTTING']
        self.cracking_percent = self.df['CRACKING_PERCENT']
        self.faulting = self.df['FAULTING']
        self.year_last_improvement_value_date = self.df['YEAR_LAST_IMPROVEMENT_VALUE_DATE']
        self.year_last_construction_value_date = self.df['YEAR_LAST_CONSTRUCTION_VALUE_DATE']
        self.begin_date = self.df['BEGIN_DATE']
        self.last_overlay_thickness = self.df['LAST_OVERLAY_THICKNESS']
        self.thickness_rigid = self.df['THICKNESS_RIGID']
        self.base_type = self.df['BASE_TYPE']
        self.base_thickness = self.df['BASE_THICKNESS']
        self.thickness_flexible = self.df['THICKNESS_FLEXIBLE']
        self.county_id = self.df['COUNTY_ID']

    def check_rule_sjf43(self):
        #sums up the section lengths of samples and the section length of curves in order to execute rule sjf43
        sum_curve = self.df.loc[( self.curve_classification.notna()),'Section_Length'].sum()
        sum_sample = self.df.loc[ (((self.samples.notna())&( self.f_system.isin([1,2,3]) )) | ( (self.f_system==4) & (self.urban_id==99999) ) ),'Section_Length'].sum()
        if sum_curve != sum_sample:
            return True
        else:
            return False
    
    def check_rule_sjf47(self):
        #sums up the section lengths of samples and the section length of grades in order to execute rule sjf47
        sum_grade = self.df.loc[( self.grade_classification.notna()),'Section_Length'].sum()
        sum_sample = self.df.loc[ (((self.samples.notna())&( self.f_system.isin([1,2,3]) )) | ( (self.f_system==4) & (self.urban_id==99999) ) ),'Section_Length'].sum()
        if sum_grade != sum_sample:
            print('Sums are not equal, please review')
            return True
        else:
            print('Sums are equal')
            return False
    
    def sjf01(self):
        #F_SYSTEM|F_SYSTEM must exist where FACILITY_TYPE is in (1;2;4;5;6) and Must not be NULL
        tmp_errors = (((self.f_system.notna())&(self.facility_type.isin([1,2,3,4,5,6]))))
        print('SJF01 Completed',tmp_errors)
        return tmp_errors
        # tmp_errors['ErrorMessage'] ='F_SYSTEM must exist where FACILITY_TYPE is in (1;2;4;5;6) and Must not be NULL'
        # pd.concat([self.error_df,tmp_errors],ignore_index=True)
    
    def sjf02(self):
        #URBAN_ID|"URBAN_ID must exist and must not be NULL where: 1. FACILITY_TYPE in (1;2;4) AND F_SYSTEM in (1;2;3;4;5) [OR] 2. FACILITY_TYPE = 6 AND DIR_THROUGH_LANES > 0 and F_SYSTEM = 1 AND (IRI IS NOT NULL OR PSR IS NOT NULL)"
        tmp_errors = (((self.urban_id.notna())&\
        (self.facility_type.isin(facility_list2))&\
        (self.f_system.isin(f_system_list)))|\
        ((self.facility_type==6)&\
        (self.dir_through_lanes>0)&\
        (self.f_system==1)&\
        (self.iri.notna())))
        print('SJF02 Completed',tmp_errors)
        
        return tmp_errors
    
    def sjf03(self):
        #FACILITY_TYPE|FACILITY_TYPE must exist where F_SYSTEM in (1;2;3;4;5;6;7)  and must not be NULL
        tmp_errors = (((self.facility_type.notna())&\
        (self.f_system.isin(f_system_list))))
        print('SJF03 Completed',tmp_errors)
        return tmp_errors
    
    def sjf04(self):
        print('sjf04 : Do not submit to HPMS')
        return True
    
    def sjf05(self):
        #ACCESS_CONTROL|ACCESS_CONTROL must exist where (F_SYSTEM in (1;2;3) or Sample or NHS) AND FACILITY_TYPE IN (1;2) and must not be NULL
        tmp_errors = (( (self.access_control.notna())&(self.f_system.isin([1,2,3]))& (self.facility_type.isin([1,2])) ) )
        print('sjf05 Completed',tmp_errors)
        return tmp_errors
    
    def sjf06(self):
        #OWNERSHIP|OWNERSHIP must exist where (F_SYSTEM in (1;2;3;4;5;6;7) and FACILITY_TYPE (1;2;5;6) and must not be NULL
        tmp_errors = (((self.ownership.notna())&(self.f_system.isin([1,2,3,4,5,6,7]))&(self.facility_type.isin([1,2,5,6]))))
        print('sjf06 Completed',tmp_errors)
        return tmp_errors
    
    def sjf07(self):
        #THROUGH_LANES|THROUGH_LANES must exist where FACILITY_TYPE in (1;2;4) AND (F_SYSTEM in (1;2;3;4;5) or (F_SYSTEM = 6 and URBAN_ID <99999) or NHS ValueNumeric <> NULL) and must not be NULL
        tmp_errors = (((self.through_lanes.notna())&\
        (self.facility_type.isin([1,2,4]))&\
        ((self.f_system.isin([1,2,3,4,5]))|\
        ((self.f_system==6)&(self.urban_id<99999))|\
        (self.nhs.notna()))))
        print('sjf07 Completed',tmp_errors)
        return tmp_errors
    
    def sjf08(self):
        print('sjf08 : Do not submit to HPMS')
        return True
    
    def sjf09(self):
        print('sjf09 : Do not submit to HPMS')
        return True
    
    def sjf10(self):
        #PEAK_LANES|"PEAK_LANES must exist on Samples"
        tmp_errors = (((self.peak_lanes.notna())&(self.samples.notna())))
        print('sjf10 Completed',tmp_errors)
        return tmp_errors
    
    def sjf11(self):
        #COUNTER_PEAK_LANES|COUNTER_PEAK_LANES must exist on Samples where FACILITY_TYPE = 2 AND (URBAN_ID < 99999 OR THROUGH_LANES >=4)
        tmp_errors = (((self.counter_peak_lanes.notna())&\
        (self.samples.notna())&\
        (self.facility_type==2)&\
        ((self.urban_id<99999)|(self.through_lanes>=4))))
        print('sjf11 Completed',tmp_errors)
        return tmp_errors
    
    def sjf12(self):
        #TURN_LANES_R|TURN_LANES_R must exist on Samples where URBAN_ID  < 99999 and ACCESS_CONTROL >1
        tmp_errors = (((self.turn_lanes_r.notna())&(self.samples.notna())&(self.urban_id<99999)&(self.access_control>1)))
        print('sjf12 Completed',tmp_errors)
        return tmp_errors
    
    def sjf13(self):
        #TURN_LANES_L|TURN_LANES_L must exist on Samples where URBAN_ID  < 99999 and ACCESS_CONTROL >1
        tmp_errors = (((self.turn_lanes_l.notna()) & (self.samples.notna())&(self.urban_id<99999)&(self.access_control>1)))
        print('sjf13 Completed',tmp_errors)
        return tmp_errors
    
    def sjf14(self):
        tmp_errors = (((self.speed_limit.notna())&(self.samples.notna())&(self.nhs.notna())))
        print('sjf14',tmp_errors)
        return tmp_errors
    
    def sjf15(self):
        tmp_errors = ((self.toll_id.notna()))
        print('sjf15 Completed',tmp_errors)
        return tmp_errors
    
    def sjf16(self):
        tmp_errors = (((self.route_number.notna())&\
        (((self.f_system.isin([1,2,3,4]))|(self.nhs.notna()))&\
        (self.facility_type.isin([1,2]))&\
        (self.route_signing.isin([2,3,4,5,6,7,8,9]))|\
        ((self.f_system==1)&(self.facility_type==6)&\
        (self.dir_through_lanes>0)&(self.iri.notna())))))
        print('sjf16',tmp_errors)
        return tmp_errors
    
    def sjf17(self):
        tmp_errors = (((self.route_signing.notna())&\
        ((self.f_system.isin([1,2,3,4]))|(self.nhs.notna()))&\
        (self.facility_type.isin([1,2]))))
        print('sjf17 Completed',tmp_errors)
        return tmp_errors
    
    def sjf18(self):
        tmp_errors = (((self.route_qualifier.notna())&\
        ((self.f_system.isin([1,2,3,4]))|\
        (self.nhs.notna()))&(self.facility_type.isin([1,2]))))
        print('sjf18',tmp_errors)
        return tmp_errors
    
    def sjf19(self):
        tmp_errors = (((self.route_name.notna())&\
        ((self.f_system.isin([1,2,3,4]))|(self.nhs.notna()))&\
        (self.facility_type.isin([1,2]))))
        print('sjf19 Completed',tmp_errors)
        return tmp_errors
    
    def sjf20(self):
        tmp_errors = (((self.aadt.notna())&((self.facility_type.isin([1,2,4]))&(self.f_system.isin([1,2,3,4,5]))|((self.f_system==6)&(self.urban_id<99999))|(self.nhs.notna()))))
        print('sjf20',tmp_errors)
        return tmp_errors
    
    def sjf21(self):
        tmp_errors = (((self.aadt_single_unit.notna())&(((self.f_system==1)|(self.nhs.notna()))&(self.facility_type.isin([1,2])))& self.samples.notna()))
        print('sjf21',tmp_errors)
        return tmp_errors

    def sjf22(self):
        tmp_errors = (((self.pct_dh_single_unit.notna())&(self.samples.notna())))
        print('sjf22',tmp_errors)
        return tmp_errors
    
    def sjf23(self):
        tmp_errors = (((self.aad_combination.notna())&(((self.f_system==1)|(self.nhs.notna()))&(self.facility_type.isin([1,2])))& self.samples.notna()))
        print('sjf23',tmp_errors)
        return tmp_errors
    
    def sjf24(self):
        tmp_errors = (((self.pct_dh_combination.notna())&(self.samples.notna())))
        print('sjf24',tmp_errors)
        return tmp_errors
    
    def sjf25(self):
        tmp_errors = (((self.k_factor.notna())&(self.samples.notna())))
        print('sjf25',tmp_errors)
        return tmp_errors
    
    def sjf26(self):
        tmp_errors = (((self.dir_factor.notna())&(self.samples.notna())))
        print('sjf26',tmp_errors)
        return tmp_errors
    
    def sjf27(self):
        tmp_errors = (((self.future_aadt.notna())&(self.samples.notna())))
        print('sjf27',tmp_errors)
        return tmp_errors 
    
    def sjf28(self):
        tmp_errors = (((self.signal_type.notna())&(self.urban_id!=99999)&(self.number_signals>=1)&(self.samples.notna())))
        print('sjf28',tmp_errors)
        return tmp_errors
    
    def sjf29(self):
        tmp_errors = (((self.pct_green_time.notna())&(self.number_signals>=1)&(self.urban_id<99999)&(self.samples.notna())))
        print('sjf29',tmp_errors)
        return tmp_errors
    
    def sjf30(self):
        tmp_errors = (((self.number_signals.notna())&(self.signal_type.isin([1,2,3,4]))&(self.samples.notna())))
        print('sjf30',tmp_errors)
        return tmp_errors
    
    def sjf31(self):
        tmp_errors = (((self.stop_signs.notna())&(self.samples.notna())))
        print('sjf31',tmp_errors)
        return tmp_errors
    
    def sjf32(self):
        tmp_errors = (((self.at_grade_other.notna())&(self.samples.notna())))
        print('sjf32',tmp_errors)
        return tmp_errors
    
    def sjf33(self):
        tmp_errors = (((self.lane_width.notna())&(self.samples.notna())))
        print('sjf33',tmp_errors)
        return tmp_errors
    
    def sjf34(self):
        tmp_errors = (((self.median_type.notna())&(self.samples.notna())))
        print('sjf34',tmp_errors)
        return tmp_errors
    
    def sjf35(self):
        tmp_errors = (((self.median_width.notna())&(self.median_type.isin([2,3,4,5,6,7]))&(self.samples.notna())))
        print('sjf35',tmp_errors)
        return tmp_errors
    
    def sjf36(self):
        tmp_errors = ((self.samples.isna())|((self.shoulder_type.notna())&(self.samples.notna())))
        print('sjf36',tmp_errors)
        return tmp_errors
    
    def sjf37(self):
        tmp_errors = ((self.samples.isna())|((self.shoulder_width_r.notna())&\
        (self.shoulder_type.isin([2,3,4,5,6]))&\
        (self.samples.notna())))
        print('sjf37',tmp_errors)
        return tmp_errors
    
    def sjf38(self):
        tmp_errors = ((self.samples.isna())|((self.shoulder_width_l.notna())&\
        (self.shoulder_type.isin([2,3,4,5,6]))&\
        (self.median_type.isin([2,3,4,5,6,7]))&\
        (self.samples.notna())))
        print('sjf38',tmp_errors)
        return tmp_errors
    
    def sjf39(self):
        tmp_errors = ((self.samples.isna())|((self.peak_parking.notna())&\
        (self.urban_id<99999)&(self.samples.notna())))
        print('sjf39',tmp_errors)
        return tmp_errors
    
    def sjf40(self):
        tmp_errors = ((self.samples.isna())|((self.widening_potential.notna())&\
        (self.samples.notna())))
        print('sjf40',tmp_errors)
        return tmp_errors
    
    def sjf41(self):
        tmp_errors = (((self.curve_classification.notna())&\
        ((self.f_system.isin([1,2,3]))|\
        ((self.f_system==4)&(self.urban_id==99999)&\
        (self.surface_type>1)))))
        print('sjf41',tmp_errors)
        return tmp_errors
    
    def sjf42(self):#revisit, this may not suppose to be a direct copy of SJF41
        tmp_errors = (((self.curve_classification.notna())&\
        ((self.f_system.isin([1,2,3]))|\
        ((self.f_system==4)&(self.urban_id==99999)&\
        (self.surface_type>1)))))
        print('sjf42',tmp_errors)
        return tmp_errors
    
    def sjf43(self):
        tmp_errors = self.check_rule_sjf43()
        print('sjf43',tmp_errors)
        return tmp_errors
    
    def sjf44(self):
        #TERRAIN_TYPE must exist on Samples WHERE (URBAN_ID = 99999 AND F_SYSTEM in (1;2;3;4;5))
        tmp_error = ((self.urban_id<99999)|((self.terrain_type.notna())&\
        (self.urban_id==99999)&(self.f_system.isin([1,2,3,4,5]))))
        print('sjf44',tmp_error)
        return tmp_error
    
    def sjf45(self):
        tmp_errors = (((self.grade_classification.notna())&\
        ((self.f_system.isin([1,2,3]))|\
        ((self.f_system==4)&(self.urban_id==99999)&\
        (self.surface_type>1)))))
        print('sjf45',tmp_errors)
        return tmp_errors
    
    def sjf46(self):
        tmp_errors = (((self.grade_classification.notna())&\
        ((self.f_system.isin([1,2,3]))|\
        ((self.f_system==4)&(self.urban_id==99999)&\
        (self.surface_type>1)))))
        print('sjf46',tmp_errors)
        return tmp_errors
    
    def sjf47(self):
        tmp_errors = self.check_rule_sjf47()
        print('sjf47',tmp_errors)
        return tmp_errors
    
    def sjf48(self):
        #PCT_PASS_SIGHT must exist on Samples WHERE: (URBAN_ID = 99999 and THROUGH_LANES =2 and MEDIAN_TYPE in (1;2) and SURFACE_TYPE > 1
        tmp_errors = ((self.urban_id<99999)|((self.pct_pass_sight.notna())&\
        (self.urban_id==99999)&(self.through_lanes==2)&\
        (self.median_type.isin([1,2]))&(self.surface_type>1)&\
        (self.samples.notna())))
        print('sjf48',tmp_errors)
        return tmp_errors
    
    def sjf49(self):
        #IRI|"IRI ValueNumeric Must Exist Where SURFACE_TYPE >1 AND (FACILITY_TYPE IN (1;2) AND (PSR ValueText <> 'A' AND (F_SYSTEM in (1;2;3) OR NHS ValueNumeric <>1) OR Sample sections WHERE (F_SYSTEM = 4 and URBAN_ID = 99999)OR DIR_THROUGH_LANES >0"
        tmp_errors = ((self.iri.notna())|((self.iri.isna())&(((self.surface_type<=1)|\
        (~self.facility_type.isin([1,2]))|(self.psr_value_text=='A')|\
        ((~self.f_system.isin([1,2,3]))|(self.nhs!=1)))|((self.f_system!=4)|\
        (self.urban_id!=99999)&(self.samples.isna()))|(self.dir_through_lanes<=0))))
        print('sjf49',tmp_errors)
        return tmp_errors
    
    def sjf50(self):
        # PSR|"PSR ValueNumeric Must Exist Where IRI ValueNumeric IS NULL AND FACILITY_TYPE IN (1;2) AND SURFACE_TYPE >1 AND(Sample exists AND (F_SYSTEM in (4;6) AND URBAN_ID <99999 OR F_SYSTEM = 5) OR (F_SYSTEM = 1 or NHS ValueNumeric <>NULL) AND PSR ValueText = ‘A’)"
        tmp_errors = ((self.psr.isna())|((self.psr.notna())&(self.facility_type.isin([1,2]))&\
        (self.surface_type>1)&((self.samples.notna())&(((self.f_system.isin([4,6]))&\
        (self.urban_id<99999)|(self.f_system==5))|((self.f_system==1)|(self.nhs.notna()))&\
        (self.psr_value_text=='A')))))
        print('sjf50',tmp_errors)
        return tmp_errors
    
    def sjf51(self):
        #SURACE_TYPE|"SURFACE_TYPE ValueNumeric Must Exist Where FACILITY_TYPE in (1;2) AND (F_SYSTEM = 1 OR NHS ValueNumeric <> NULL OR Sample exists) OR DIR_THROUGH_LANES >0 AND (IRI IS NOT NULL OR PSR IS NOT NULL) "
        tmp_errors = (((self.facility_type.isin([1,2]))&((self.f_system==1)|\
        (self.nhs.notna())|(self.samples.notna()))|(self.dir_through_lanes>0)&\
        ((self.iri.notna())|(self.psr.notna()))))
        print('sjf51',tmp_errors)
        return tmp_errors
    
    def sjf52(self):
        #RUTTING|"RUTTING ValueNumeric Must Exist Where SURFACE_TYPE in (2;6;7;8) AND (FACILITY_TYPE in (1;2) AND (F_SYSTEM = 1 OR NHS OR Sample) OR DIR_THROUGH_LANES >0 AND (IRI IS NOT NULL OR PSR IS NOT NULL))
        tmp_errors = ((~self.surface_type.isin([2,6,7,8]))|((self.rutting.notna())&(self.surface_type.isin([2,6,7,8]))&\
        ((self.facility_type.isin([1,2]))&((self.f_system==1)|(self.nhs.notna())|\
        (self.samples.notna()))|(self.dir_through_lanes>0)&((self.iri.notna())|(self.psr.notna())))))
        print('sjf52',tmp_errors)
        return tmp_errors
    
    def sjf53(self):
        #FAULTING|"Faulting ValueNumeric Must Exist Where SURFACE_TYPE in (3;4;9;10) AND (FACILITY_TYPE in (1;2)  AND  (F_SYSTEM = 1 OR NHS OR Sample) OR  DIR_THROUGH_LANES >0 AND (IRI IS NOT NULL OR PSR IS NOT NULL))
        tmp_errors = ((~self.surface_type.isin([3,4,9,10]))|((self.faulting.notna())&(self.surface_type.isin([3,4,9,10]))&\
        ((self.facility_type.isin([1,2]))&((self.f_system==1)|(self.nhs.notna())|\
        (self.samples.notna()))|(self.dir_through_lanes>0)&((self.iri.notna())|(self.psr.notna())))))
        print('sjf53',tmp_errors)
        return tmp_errors
    
    def sjf54(self):
        #CRACKING_PERCENT|"SURFACE_TYPE in (2;3;4;5;6;7;8;9;10) AND (FACILITY_TYPE in (1;2) AND (F_SYSTEM = 1 OR  NHS  OR Sample) OR (DIR_THROUGH_LANES >0 AND (IRI IS NOT NULL OR PSR IS NOT NULL)))
        tmp_errors = (((self.cracking_percent.notna())&(self.surface_type.isin([2,3,4,5,6,7,8,9,10]))&\
        ((self.facility_type.isin([1,2]))&((self.f_system==1)|(self.nhs.notna())|\
        (self.samples.notna()))|(self.dir_through_lanes>0)&((self.iri.notna())|(self.psr.notna())))))
        print('sjf54',tmp_errors)
        return tmp_errors
    
    def sjf55(self):
        #YEAR_LAST_IMPROVEMENT|YEAR_LAST_IMPROVEMENT must exist on Samples where SURFACE_TYPE is in the range (2;3;4;5;6;7;8;9;10) OR where  (YEAR_LAST_CONSTRUCTION < BeginDate Year - 20)
        tmp_errors = ((self.surface_type.isin([1]))|((self.year_last_improvement_value_date.notna())&\
        (self.samples.notna())&((self.surface_type.isin([2,3,4,5,6,7,8,9,10]))|\
        (pd.to_datetime(self.year_last_construction_value_date)<(pd.to_datetime(self.begin_date)-timedelta(days =7305 ))))))
        print('sjf55',tmp_errors)
        return tmp_errors
    
    def sjf56(self):
        #YEAR_LAST_CONSTRUCTION|YEAR_LAST_CONSTRUCTION must exist on Samples where SURFACE_TYPE is in the range (2;3;4;5;6;7;8;9;10)
        tmp_errors = ((self.surface_type.isin([1]))|((self.year_last_construction_value_date.notna())&\
        (self.samples.notna())&(self.surface_type.isin([2,3,4,5,6,7,8,9,10]))))
        print('sjf56',tmp_errors)
        return tmp_errors
    
    def sjf57(self):
        #LAST_OVERLAY_THICKNESS|Sample and YEAR_LAST_IMPROVEMENT exist
        tmp_errors = (((self.samples.notna())&(self.year_last_improvement_value_date.notna())))
        print('sjf57',tmp_errors)
        return tmp_errors
    
    def sjf58(self):
        #THICKNESS_RIGID|SURFACE_TYPE (3;4;5;7;8;9;10) and Sample
        tmp_errors = (((self.thickness_rigid.notna())&(self.samples.notna())&(self.surface_type.isin([3,4,5,7,8,9,10]))))
        print('sjf58',tmp_errors)
        return tmp_errors
    
    def sjf59(self):
        #THICKNESS_FLEXIBLE|SURFACE_TYPE (2;6;7;8) and Sample
        tmp_errors = (((self.thickness_flexible.notna())&(self.surface_type.isin([2,6,7,8]))&(self.samples.notna())))
        print('sjf59',tmp_errors)
        return tmp_errors

    def sjf60(self):
        #BASE_TYPE|Sample and SURFACE_TYPE >1
        tmp_errors = (((self.base_type.notna())&(self.samples.notna())&(self.surface_type>1)))
        print('sjf60',tmp_errors)
        return tmp_errors
    
    def sjf61(self):
        #BASE_THICKNESS|Where BASE_TYPE >1; SURFACE_TYPE >1  and Sample
        tmp_errors = ((self.base_type<1)|((self.base_type>1)&(self.surface_type>1)&(self.samples.notna())&(self.base_thickness.notna())))
        print('sjf61',tmp_errors)
        return tmp_errors
    
    def sjf62(self):
        print('Do not collect soil type data item')
        return False
    
    def sjf63(self):
        #COUNTY_ID|FACILITY_TYPE in (1;2) AND (F_SYSTEM in (1;2;3;4;5) or (F_SYSTEM = 6 and URBAN_ID <99999) or NHS
        tmp_errors = (((self.county_id.notna())&(self.facility_type.isin([1,2]))&\
        ((self.f_system.isin([1,2,3,4,5]))|((self.f_system==6)&(self.urban_id<99999))|\
        (self.nhs.notna()))))
        print('sjf63',tmp_errors)
        return tmp_errors
    
    def sjf64(self):
        #NHS|(F_SYSTEM = 1 AND (FACILITY_TYPE  in 1;2;6))|
        tmp_errors = (((self.nhs.notna())&(self.f_system==1)&(self.facility_type.isin([1,2,6]))))
        print('sjf64',tmp_errors)
        return tmp_errors
    
    def sjf65(self):
        #strhanet
        print('STRAHNET: No coverage validations')
        return False
    
    def sjf66(self):
        #NN
        print("NN : No coverage validations")
        return False
    
    def sjf67(self):
        #Maintenance Operations
        print('MAINTENANCE OPERATIONS: No coverage validations')
        return False
    
    def sjf68(self):
        #DIR_THROUGH_LANES|F_SYSTEM =1 AND (FACILITY_TYPE = 6) AND (IRI OR PSR >0)
        tmp_errors = (((self.dir_through_lanes.notna())&(self.f_system==1)&\
        (self.facility_type==6)&((self.iri>0)|(self.psr>0))))
        print('sjf68',tmp_errors)
        return tmp_errors
    
    def sjf69(self):
        #THROUGH_LANES|THROUGH_LANES must be >1 WHERE FACILITY_TYPE = 2|
        tmp_errors = ((self.facility_type!=2)|(self.through_lanes>1)&(self.facility_type==2))
        print('sjf69',tmp_errors)
        return tmp_errors
    
    def sjf70(self):
        #THROUGH_LANES|The sum of COUNTER_PEAK_LANES + PEAK_LANES must be >= THROUGH_LANES
        tmp_errors = ((((self.counter_peak_lanes)+(self.peak_lanes))>=(self.through_lanes)))
        print('sjf70',tmp_errors)
        return tmp_errors
    
    def sjf71(self):
        #COUNTER_PEAK_LANES|COUNTER_PEAK_LANES must be NULL if FACILITY_TYPE is 1|
        tmp_errors = ((self.facility_type!=1)|((self.facility_type==1)&(self.counter_peak_lanes.isna())))
    

    
    
    
    
    def run(self):
        #when it returns True, it means the data has no errors itself
        # self.df['SJF-01'] = self.sjf01()
        # self.df['SJF-02'] = self.sjf02()
        # self.df['SJF-03'] = self.sjf03()
        # self.df['SJF-04'] = self.sjf04()
        # self.df['SJF-05'] = self.sjf05()
        # self.df['SJF-06'] = self.sjf06()
        # self.df['SJF-07'] = self.sjf07()
        # self.df['SJF-08'] = self.sjf08()
        # self.df['SJF-09'] = self.sjf09()
        # self.df['SJF-10'] = self.sjf10()
        # self.df['SJF-11'] = self.sjf11()
        # self.df['SJF-12'] = self.sjf12()
        # self.df['SJF-13'] = self.sjf13()
        # self.df['SJF-14'] = self.sjf14()
        # self.df['SJF-15'] = self.sjf15()
        # self.df['SJF-16'] = self.sjf16()
        # self.df['SJF-17'] = self.sjf17()
        # self.df['SJF-18'] = self.sjf18()
        # self.df['SJF-19'] = self.sjf19()
        # self.df['SJF-20'] = self.sjf20()
        # self.df['SJF-21'] = self.sjf21()
        # self.df['SJF-22'] = self.sjf22()
        # self.df['SJF-23'] = self.sjf23()
        # self.df['SJF-24'] = self.sjf24()
        # self.df['SJF-25'] = self.sjf25()
        # self.df['SJF-26'] = self.sjf26()
        # self.df['SJF-27'] = self.sjf27()
        # self.df['SJF-28'] = self.sjf28()
        # self.df['SJF-29'] = self.sjf29()
        # self.df['SJF-30'] = self.sjf30()
        # self.df['SJF-31'] = self.sjf31()
        # self.df['SJF-32'] = self.sjf32()
        # self.df['SJF-33'] = self.sjf33()
        # self.df['SJF-34'] = self.sjf34()
        # self.df['SJF-35'] = self.sjf35()
        # self.df['SJF-36'] = self.sjf36()
        # self.df['SJF-37'] = self.sjf37()
        # self.df['SJF-38'] = self.sjf38()
        # self.df['SJF-39'] = self.sjf39()
        # self.df['SJF-40'] = self.sjf40()
        # self.df['SJF-41'] = self.sjf41()
        # self.df['SJF-42'] = self.sjf42()
        # self.df['SJF-43'] = self.sjf43()
        # self.df['SJF-44'] = self.sjf44()
        # self.df['SJF-45'] = self.sjf45()
        # self.df['SJF-46'] = self.sjf46()
        # self.df['SJF-47'] = self.sjf47()
        # self.df['SJF-48'] = self.sjf48()
        self.df['SJF-49'] = self.sjf49()
        # self.df['SJF-50'] = self.sjf50()
        # self.df['SJF-51'] = self.sjf51()
        # self.df['SJF-52'] = self.sjf52()
        # self.df['SJF-53'] = self.sjf53()
        # self.df['SJF-54'] = self.sjf54()
        # self.df['SJF-55'] = self.sjf55()
        # self.df['SJF-56'] = self.sjf56()
        # self.df['SJF-57'] = self.sjf57()
        # self.df['SJF-58'] = self.sjf58()
        # self.df['SJF-59'] = self.sjf59()
        # self.df['SJF-60'] = self.sjf60()
        # self.df['SJF-61'] = self.sjf61()
        # self.df['SJF-62'] = self.sjf62()
        # self.df['SJF-63'] = self.sjf63()
        # self.df['SJF-64'] = self.sjf64()
        # self.df['SJF-65'] = self.sjf65()
        # self.df['SJF-66'] = self.sjf66()
        # self.df['SJF-67'] = self.sjf67()
        # self.df['SJF-68'] = self.sjf68()
        # self.df['SJF-69'] = self.sjf69()
        # self.df['SJF-70'] = self.sjf70()





        print(self.df[['SJF-49','SURFACE_TYPE','IS_SAMPLE','IRI','PSR_VALUE_TEXT','FACILITY_TYPE','F_SYSTEM','URBAN_ID','NHS','DIR_THROUGH_LANES']])



df = pd.read_csv('better_test_data.csv')

c = full_spatial_functions(df)  
# c.run()
c.run()

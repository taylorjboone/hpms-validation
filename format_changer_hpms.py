from fileinput import filename
import pandas as pd
from os import listdir
from os.path import isfile, join
import os

pavement_list=['June_Submission\\HPMS_2021_NON_INTERSTATE_NHS_CRACKING_PERCENT.csv','June_Submission\\HPMS_2021_NON_INTERSTATE_NHS_FAULTING.csv','June_Submission\\HPMS_2021_NON_INTERSTATE_NHS_IRI.csv','June_Submission\\HPMS_2021_NON_INTERSTATE_NHS_RUTTING.csv']
mehlist=['June_Submission\\year_last_improved.csv','June_Submission\\year_last_construct.csv']
month_dict_1 = {'Jan-': '1/', 'Feb-': '2/', 'Mar-': '3/', 'Apr-': '4/', 'May-': '5/', 'Jun-': '6/', 'Jul-': '7/', 'Aug-': '8/', 'Sep-': '9/', 'Oct-': '10/', 'Nov-': '11/', 'Dec-': '12/'}
month_dict_2 = {'-Jan': '1/', '-Feb': '2/', '-Mar': '3/', '-Apr': '4/', '-May': '5/', '-Jun': '6/', '-Jul': '7/', '-Aug': '8/', '-Sep': '9/', '-Oct': '10/', '-Nov': '11/', '-Dec': '12/'}
correct_format={'County_Code':'CountyID','Pct_MC':'PctMotorcycles','Pct_Lgt_Trucks':'PctLightTrucks','Pct_SU_Trucks':'PctSingleUnit','Pct_CU_Trucks':'PctCombination','SectionLength':'Section_Length','SECTION_LENGTH':'Section_Length','YEAR_RECORD':'BeginDate','END_POINT':'EndPoint','STATE_CODE':'StateID','ROUTE_ID':'RouteID','VALUE_DATE':'ValueDate','DATA_ITEM':'DataItem','COMMENTS':'Comments','Value_Numeric':'ValueNumeric','VALUE_NUMERIC':'ValueNumeric','Year_Record':'BeginDate','End_Point':'EndPoint','State_Code':'StateID','Value_Date':'ValueDate','Data_Item':'DataItem','Route_ID':'RouteID','Urban_Code':'UrbanID','Value_Text':'ValueText','VALUE_TEXT':'ValueText','BEGIN_POINT':'BeginPoint','Begin_Point':'BeginPoint'}
exclude_list = ['June_Submission\\DataItem67-Future_Facility.csv']
widening_potential_dict={4:3, 1:2, 0:1, 9:4, 8:4, 7:4, 6:4, 5:4,2:2,3:3}
mypath='June_Submission'

onlyfiles = [os.path.join(mypath,f) for f in listdir(mypath) if isfile(join(mypath, f)) if f.endswith('csv')]
# fsystem_dict = {1: 1, 11: 1, 4: 2, 12: 2, 2: 3, 14: 3, 6: 4, 16: 4, 7: 5, 17: 5, 8: 6, 18: 6, 9: 7, 19: 7}

def check_upper(c):
    if c >= 'A' and c <= 'Z':
        return True
    else:
        return False


# Fixes case and format of data items (e.g. 'BaseType' to be 'BASE_TYPE')
def fix_this(x):
    if not '_' in x:
        splits = []
        for p,c in enumerate(x):
            if check_upper(c): 
                splits.append(p)
        if len(splits) == 1 or len(splits) == len(x):
            return x.upper()
        # print(splits)
        oldsplit = 0 
        parts = []
        for split in splits[1:]:
            part = x[oldsplit:split].upper()
            parts.append(part)
            oldsplit = split 
        parts.append(x[oldsplit:].upper())
        return '_'.join(parts)
    else:
        return x.upper()


# Fixes data frame item name 
def fix_item_name(df):
    val = df.iloc[0].DataItem
    val = fix_this(val)
    df['DataItem'] = val 
    return df 


def map_me(x):
    myb = str(x).isdigit() or '.' in str(x) or str(x) == 'nan'
    return myb


for a in onlyfiles:
    if a not in exclude_list:
        print(a)

        b=pd.read_csv(a,sep='|')
        b = b.rename(columns=correct_format)
        b['BeginDate']='12/31/2021'

        # Drops unnecessary columns
        if 'Section_Length' in b.columns:
            b=b.drop('Section_Length',axis=1)
        if 'State_Portion_Pop' in b.columns:
            b=b.drop('State_Portion_Pop',axis=1)
        if 'State_Portion_Land' in b.columns:
            b=b.drop('State_Portion_Land',axis=1)
        
        # Fixes format of ValueDates in Pavement Items (e.g. Apr-21 -> 4/2021)
        if a in pavement_list:
            b = b[b['ValueDate'].notna()]
            for k, v in month_dict_1.items():
                for line in b['ValueDate']: 
                    if k in line:
                        string = line.replace(k, v + '20')
                        b['ValueDate'].loc[b['ValueDate'] == line] = string
            for k, v in month_dict_2.items():
                for line in b['ValueDate']:
                    if k in line:
                        string = v + '2021'
                        b['ValueDate'].loc[b['ValueDate'] == line] = string

        if 'DataItem' in b.columns and a not in ['June_Submission\\HPMS_2021_SAMPLES_GRADES.csv', 'June_Submission\\HPMS_2021_SAMPLES_CURVES.csv']: 
            b = fix_item_name(b)
        
        if a in ['June_Submission\\HPMS_2021_SAMPLES_GRADES.csv', 'June_Submission\\HPMS_2021_SAMPLES_CURVES.csv']:
            b['DataItem']=b['DataItem'].apply(lambda x: x.upper())

        # Drops empty strings, null values, and duplicates
        if 'ValueNumeric' in b.columns and a not in mehlist:
            b = b[b['ValueNumeric'] != ' ']
            b = b[b['ValueNumeric'].notna()]
            b = b.drop_duplicates()

        # Fixes ValueNumeric edge cases
        if a in ['June_Submission\\HPMS_2021_SAMPLES_SHOULDER_TYPE.csv']:
            b['ValueNumeric'] = b['ValueNumeric'].replace(7,1)
        if a in ['June_Submission\\HPMS_2021_WV_DataItem49_SURFACE_TYPE.csv']:
            b['ValueDate'] = ''
        if a in ['June_Submission\\base_type.csv']:
            b['ValueNumeric'] = b['ValueNumeric'].replace('Base 2',2)
            b['ValueNumeric'] = b['ValueNumeric'].replace('Asphalt',3)
            b['ValueNumeric'] = b['ValueNumeric'].replace('Concrete',3)
        if a in ['June_Submission\\last_overlay_thickness.csv']:
            b = b[b['ValueNumeric'] != 'micro']
            b = b[b['ValueNumeric'] != 0]
        if a in ['June_Submission\\thick_flex.csv']:
            b = b[b['ValueNumeric'] != 0]
        if a in ['June_Submission\\thick_rigid.csv']:
            b = b[b['ValueNumeric'] != 0]

        if a in ['June_Submission\\DataItem42-Widening_Potential.csv']:
            b['ValueNumeric'] = b.ValueNumeric.map(lambda x:widening_potential_dict[x])

        # Fixes new data item names
        if a in ['June_Submission\\DataItem23-Pct_Peak_Single.csv']:
            a['DataItem'] = 'PCT_DH_SINGLE_UNIT'
        if a in ['June_Submission\\DataItem25-Pct_Peak_Combination.csv']:
            a['DataItem'] = 'PCT_DH_COMBINATION'
        if a in ['June_Submission\\thick_rigid.csv']:
            b['DataItem'] = 'THICKNESS_RIGID'
            b=b[b['ValueNumeric']!=0]
        if a in ['June_Submission\\base_thick.csv']:
            b['DataItem'] = 'BASE_THICKNESS'
        if a in['June_Submission\\thick_flex.csv']:
            b['DataItem'] = 'THICKNESS_FLEXIBLE'
            b=b[b['ValueNumeric']!=0]
        if a in['June_Submission\\year_last_construct.csv']:
            b['DataItem'] = 'YEAR_LAST_CONSTRUCTION'
        if a in['June_Submission\\year_last_improved.csv']:
            b['DataItem'] = 'YEAR_LAST_IMPROVEMENT'
        if a in ['June_Submission\\DataItem63-County_Code.csv']:
            b['DataItem'] = 'COUNTY_ID'
        if a in ['June_Submission\\DataItem66-Truck.csv']:
            b['DataItem'] = 'NN'


            # print(b[b.ValueNumeric.map(lambda x: (str(x).isdigit() and not '.' in str(x)))==False])
            # print(b[b.ValueNumeric.map(map_me)==False])
            #print(b[(b.ValueNumeric.isna())&(b.ValueText.isna())])
            # print(b)
            

        bs=a.split('\\')[-1]
        c=bs.split('.')[0]
        d='modified_june_submission_data\\' + c + '_test.csv'
        # q='modified_june_submission_data\\' + a.split('\\')[-1] + ''
        b.to_csv(d,sep='|',index=False)
    
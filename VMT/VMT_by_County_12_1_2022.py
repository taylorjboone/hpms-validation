import geopandas as gpd
import pandas as pd
import numpy as np

# gdf_2021 = gpd.read_file('Traffic.gdb', driver='FileGDB', layer='WV2021')
gdf_2021 = pd.read_csv('2021_combined.csv')
# df = pd.read_excel('_NEW_joined_for_Mainframe_MedianMK.xlsx')


columns = {

    'FSystem_ValueNumeric':'F_System',
    'FacilityType_ValueNumeric':'FACILITY_TYPE',
    'UrbanCode_ValueNumeric':'URBAN_ID',
    'AADT_ValueNumeric':'AADT',
    'BMP':'BeginPoint',
    'EMP':'EndPoint',
}

gdf_2021.rename(columns=columns,inplace=True)
county = {1: 'Barbour', 2: 'Berkeley', 3: 'Boone', 4: 'Braxton', 5: 'Brooke', 6: 'Cabell', 7: 'Calhoun', 8: 'Clay', 9: 'Doddridge', 10: 'Fayette', 11: 'Gilmer', 12: 'Grant', 13: 'Greenbrier', 14: 'Hampshire', 15: 'Hancock', 16: 'Hardy', 17: 'Harrison', 18: 'Jackson', 19: 'Jefferson', 20: 'Kanawha', 21: 'Lewis', 22: 'Lincoln', 23: 'Logan', 24: 'McDowell', 25: 'Marion', 26: 'Marshall', 27: 'Mason', 28: 'Mercer', 29: 'Mineral', 30: 'Mingo', 31: 'Monongalia', 32: 'Monroe', 33: 'Morgan', 34: 'Nicholas', 35: 'Ohio', 36: 'Pendleton', 37: 'Pleasants', 38: 'Pocahontas', 39: 'Preston', 40: 'Putnam', 41: 'Raleigh', 42: 'Randolph', 43: 'Ritchie', 44: 'Roane', 45: 'Summers', 46: 'Taylor', 47: 'Tucker', 48: 'Tyler', 49: 'Upshur', 50: 'Wayne', 51: 'Webster', 52: 'Wetzel', 53: 'Wirt', 54: 'Wood', 55: 'Wyoming'}

# Converting NAT_FUNCTIONAL_CLASS into F_SYSTEM accepted by HPMS (FHWA).
f_system = {1:1, 11:1, 4:2, 12:2, 2:3, 14:3, 6:4, 16:4, 7:5, 17:5, 8:6, 18:6, 9:7, 19:7}
f_system_desc = {1:'1 - Interstate', 2:'2 - Principal Arterial - Other Freeways and Expressways', 3:'3 - Principal Arterial - Other', 4:'4 - Minor Arterial', 5:'5 - Major Collector', 6:'6 - Minor Collector', 7:'7 - Local'}

df = gdf_2021[['RouteID', 'BeginPoint', 'EndPoint', 'F_System', 'FACILITY_TYPE', 'URBAN_ID', 'AADT']].copy()
df.drop(df[df["F_System"].isna()].index, inplace=True)
df.drop(df[df["FACILITY_TYPE"].isna()].index, inplace=True)
df.drop(df[df["URBAN_ID"].isna()].index, inplace=True)

#  BeginPoint  EndPoint
df['len'] = round(abs(df['EndPoint'] - df['BeginPoint']),3)

columns = ['RouteID', 'BeginPoint', 'EndPoint','F_System','FACILITY_TYPE','URBAN_ID', 'len','AADT']
test = df[columns].copy()


df['isUrban']= (
                ((
                    ((test['F_System'].astype(int).isin([1,2,3,4,5])) & (test['FACILITY_TYPE'].astype(int).isin([1,2]))) |
                    ((test['F_System'].astype(int) == 6) & (test['URBAN_ID'].astype(int) < 99999))
                ) & (test['URBAN_ID'].astype(int) < 99999)) == True
            )

df['isRural'] = (
                ((
                    ((test['F_System'].astype(int).isin([1,2,3,4,5])) & (test['FACILITY_TYPE'].astype(int).isin([1,2]))) |
                    ((test['F_System'].astype(int) == 6) & (test['URBAN_ID'].astype(int) < 99999))
                ) & (test['URBAN_ID'].astype(int) == 99999)) == True
            )

def mapme(x):
    if x['isUrban']:
        return 'Urban'
    elif x['isRural']:
        return 'Rural'
    else:
        return ''


df['RuralUrban'] = df.apply(mapme,axis=1)

# defaults
newlist = []
for i in county.keys():
    for rur in ['Rural','Urban']:
        for f in [1,2,3,4,5,6,7]:
            newlist.append({'RouteID':str(i).zfill(2),'County':county[i],'RuralUrban':rur,'F_System':f,'LocalVMT':0,'len':0}) 
tmpdf = pd.DataFrame(newlist)
df = pd.concat([df,tmpdf],ignore_index=True)

# filters 
df['VMT'] = df['len'] * df['AADT']
df = df[df.RuralUrban!='']

# d = {'len':'Sum1','AADT':'Average'}
df = df[df['F_System']<=6]
df['County'] = df.RouteID.str[:2].fillna(value=0).astype(int).map(lambda x:county.get(int(x),''))

# cleaning up df2 
df2 = pd.read_excel('City_Fed_State_Mileage_byCounty_UrbanCode_Fsystem.xlsx')
df2['RuralUrban'] = df2.Urban_Code.map(lambda x: 'Rural' if x==99999 else 'Urban') 
df2.drop(['VMT'],axis=1,inplace=True)
df2['County'] = df2.County_Code.fillna(value=0).map(lambda x: county.get(int(x),-1))
df2.rename(columns={'Length':"len",'Local_VMT':"VMT"},inplace=True)
df2 = pd.concat([df2,tmpdf],ignore_index=True)

# combining both
df = pd.concat([df,df2])

df['F_System'] = df['F_System'].map(lambda x: f_system_desc.get(x))
print(df.columns.tolist())
df = df.rename(columns={'F_System':'Functional Classification', 'RuralUrban':'Rural or Urban', 'len':'Miles', 'VMT':'Daily Vehicle Miles'})
df['Annual Vehicle Miles'] = (df['Daily Vehicle Miles'] * 365)


grp = df.groupby(['County',"Rural or Urban",'Functional Classification']).agg({'Miles':'sum','Daily Vehicle Miles':'sum','Annual Vehicle Miles':'sum'})

grp = grp.fillna(value=0.0)

writer = pd.ExcelWriter('output.xlsx')

grp.to_excel(writer,sheet_name='County')

grp.unstack().fillna(value=0).to_excel(writer,sheet_name="Other View")
writer.save()
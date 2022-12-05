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

# 1 or 11  =  1
# 4 or 12  =  2
# 2 or 14  =  3
# 6 or 16  =  4
# 7 or 17  =  5
# 8 or 18  =  6
# 9 or 19  =  7


# 'RouteID':'RouteID', 'BeginPoint', 'EndPoint', 'F_System', 'FACILITY_TYPE',
    #    'URBAN_ID', 'Through_Lanes', 'AADT', 'NHS', 'OWNERSHIP', 'ROUTE_NUMBER',


df = gdf_2021[['RouteID', 'BeginPoint', 'EndPoint', 'F_System', 'FACILITY_TYPE', 'URBAN_ID', 'AADT']].copy()
# df['F_System'] = df['NAT_FUNCTIONAL_CLASS'].map(f_system)
# df['supp_code'] = df['RouteID'].str.slice(9,11)

# df.drop(df[df["ROUTE_STATUS"].isna()].index, inplace=True)
# df.drop(df[df["ROUTE_STATUS"].astype(int) != 5].index, inplace=True)
df.drop(df[df["F_System"].isna()].index, inplace=True)
df.drop(df[df["FACILITY_TYPE"].isna()].index, inplace=True)
df.drop(df[df["URBAN_ID"].isna()].index, inplace=True)
# df.drop(df[df['supp_code'].isna()])
# df.drop(df[df['supp_code'].astype(int) < 24])

#  BeginPoint  EndPoint
df['len'] = round(abs(df['EndPoint'] - df['BeginPoint']),3)

columns = ['RouteID', 'BeginPoint', 'EndPoint','F_System','FACILITY_TYPE','URBAN_ID', 'len','AADT']
test = df[columns].copy()


df['isUrban'] = (
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
# urban = test[conditions]
# rural = test[conditions_rural]

# urban7 = test[((test['F_System'].astype(int) == 7) & (test['URBAN_ID'].astype(int) < 99999))]
# rural = test[test['URBAN_ID'].astype(int) == 99999]

# urban['VMT'] = urban['len'] * urban['AADT']
# rural['VMT'] = rural['len'] * rural['AADT']

df['VMT'] = df['len'] * df['AADT']
df = df[df.RuralUrban!='']

# d = {'len':'Sum1','AADT':'Average'}
df = df[df['F_System']<=5]
df2 = pd.read_excel('City_Fed_State_Mileage_byCounty_UrbanCode_Fsystem.xlsx')
df2['RuralUrban'] = df2.Urban_Code.map(lambda x: 'Rural' if x==99999 else 'Urban') 
df2.drop(['VMT'],axis=1,inplace=True)
df2.rename(columns={'Length':"len",'Local_VMT':"VMT"},inplace=True)
df2['County'] = df2['County_Code'].map(lambda x: county.get(int(x),''))
df['County'] = df.RouteID.str[:2].fillna(value=0).map(lambda x:county.get(int(x),''))

df = pd.concat([df,df2],ignore_index=True)


# for county in df['County'].unique():
#     tmp = df[df['County'] == county]
#     for i in range(7):
#         if not i in tmp['F_System'].unique():
#             tmp.append({'County':county, 'F_System':i})

grp = df.groupby(['County',"RuralUrban",'F_System']).agg({'len':'sum','VMT':'sum'})
grp = grp.fillna(value=0.0)
writer = pd.ExcelWriter('output.xlsx')

grp.to_excel(writer,sheet_name='County')

grp.unstack().fillna(value=0).to_excel(writer,sheet_name="Other View")
writer.save()
# grp2 = df2.groupby([['F_System',"RuralUrban"]).agg({'len':'sum','VMT':'sum'})




# rural_grouped = rural.groupby(["F_System"]).agg({'len':'sum','VMT':'sum'})



# urban_grouped = urban.groupby(["F_System"]).agg({'len':'sum','AADT':'mean'})

# urban_grouped['AADT'] = round(urban_grouped['AADT'], 0)
# urban_grouped['vmt'] = urban_grouped['len'] * urban_grouped['AADT']



# rural_grouped = rural.groupby(["F_System"])['len','AADT'].sum().reset_index()

# Index(['Year_Record', 'State_Code', 'ROUTE_ID', 'FROM_MEASURE', 'TO_MEASURE',
#        'Data_Item', 'Section_Length', 'AADT_dataitem', 'Value_Text',
#        'Value_Date', 'Comments'],


# 

    #  BeginPoint  EndPoint
# df_aadt_old['len'] = round(abs(df_aadt_old['TO_MEASURE'] - df_aadt_old['FROM_MEASURE']),3)
# df_aadt_old['VMT'] = df_aadt_old['len'] * df_aadt_old['AADT_dataitem']
# df_aadt_old = df_aadt_old.groupby(["Year_Record"]).agg({'len':'sum','VMT':'sum'})

# In [94]: df_aadt_old
# Out[94]:
#                    len          VMT
# Year_Record
# 2021         36554.754  42481392.84


# df['VMT'] = df['len'] * df['AADT']
# df['year'] = '2021'
# df_grouped = df.groupby(["year"]).agg({'len':'sum','VMT':'sum'})



# ------------- 2021
# df_aadt_v9 = pd.read_csv('DataItem21-AADT.csv', sep='|')
# df_aadt_v9['len'] = df_aadt_v9['EndPoint'] - df_aadt_v9['BeginPoint']
# df_aadt_v9['VMT'] = df_aadt_v9['len'] * df_aadt_v9['ValueNumeric']
# df_aadt_v9 = df_aadt_v9.groupby(["RouteID"]).agg({'len':'sum','VMT':'sum','ValueNumeric':'mean'})
# df_aadt_v9.to_csv('VMT_ByRouteID_AADT_v9.csv')
# # df_aadt_v9 = df_aadt_v9.groupby(["BeginDate"]).agg({'len':'sum','VMT':'sum'})


# # ------------- 2020
# # Year_Record|State_Code|Route_ID|Begin_Point|End_Point|Data_Item|Section_Length|Value_Numeric|Value_Text|Value_Date|Comments
# df_aadt_20 = pd.read_csv('2020_DataItem21-AADT.csv', sep='|')
# df_aadt_20['len'] = df_aadt_20['End_Point'] - df_aadt_20['Begin_Point']
# df_aadt_20['VMT'] = df_aadt_20['len'] * df_aadt_20['Value_Numeric']
# df_aadt_20 = df_aadt_20.groupby(["Route_ID"]).agg({'len':'sum','VMT':'sum', 'Value_Numeric':'mean'})
# df_aadt_20.to_csv('VMT_ByRouteID_AADT_2020.csv')


# # ----- Comparing - VMT 2020 vs VMT 2021 --------
# df_vmt20 = pd.read_csv('VMT_ByRouteID_AADT_2020.csv')
# df_vmt21 = pd.read_csv('VMT_ByRouteID_AADT_v9.csv')

# df_all = pd.merge(df_vmt20, df_vmt21, on='RouteID', how='outer')
# df_all = df_all.rename(columns={'len_x':'len_20','VMT_x':'VMT_20','ValueNumeric_x':'AADT_20', 'len_y':'len_21','VMT_y':'VMT_21','ValueNumeric_y':'AADT_21'})
# df_all['diff_VMT'] = df_all['VMT_21'] - df_all['VMT_20']
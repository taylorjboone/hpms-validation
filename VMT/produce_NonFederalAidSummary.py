from numpy import average
import pandas as pd

def get_f_system(value):
    if(value in [1,11]):
        return int(1)
    if(value in [4,12]):
        return int(2)
    if(value in [2,14]):
        return int(3)
    if(value in [6,16]):
        return int(4)
    if(value in [7,17]):
        return int(5)
    if(value in [8,18]):
        return int(6)
    if(value in [9,19]):
        return int(7)


result = []

### HPMS v9 ---- 
def write_result(row):
    return {'BeginDate': '12/31/2021',
            'StateID': 54,
            'FSystem': int(row['F_System']),
            'UrbanID': str(row['Urban_Code']).zfill(5),
            'VMT': int(row['Local_VMT']),
            'Comments': ""
            }

'''

'''
def filter_input(df):
    # filter out the Urban_Code 99998 and 99999
    df = df[df['URBAN_CODE'] != ""]
    # df = df[df['URBAN_CODE'].astype(int) < 99998]
    # df = df[df['URBAN_CODE'] != '99998']
    # df = df[df['URBAN_CODE'] != '99999']

    # filter out the surface type 1
    df = df[df['SURFACE_TYPE'] != ""]
    df = df[df['SURFACE_TYPE'].astype(float).astype(int) > 1]
    # where aadt have data (only main direction)
    

    df = df[df['NAT_FUNCTIONAL_CLASS'] != ""]
    df = df[df['NAT_FUNCTIONAL_CLASS'].astype(float).astype(int).isin([8,9,19])]
    df['F_System'] = df.apply(lambda x: get_f_system(x['NAT_FUNCTIONAL_CLASS']),axis=1)
    return df


'''
out:
URBAN_CODE|AverageAADT|StateUrbanizedAreaVMT
36190|576|202728.53
'''
# Wrong
# def group_averageAADT_length(df):
#     # Calculate the length
#     df['Length'] = round(abs(df['End_Point'] - df['Begin_Point']),3)
#     dfg_length = df.groupby(["URBAN_CODE", "F_System"])["Length"].sum().reset_index()
    
#     ### Calculate the Average
#     urban_codes = df['URBAN_CODE'].unique()

#     print(dfg_length)

#     average = []
#     for u in urban_codes:
#         df_filtered = df[df['URBAN_CODE'] == u]
#         average.append([int(u), round(df_filtered['AADT'].mean(),3)])
    
#     dfg_length['AverageAADT'] = 0
#     for index, row in dfg_length.iterrows():
#         for r in average:
#             if(int(row['URBAN_CODE']) == int(r[0])):
#                 dfg_length.loc[index,'AverageAADT'] = r[1]

#     dfg_length['StateUrbanizedAreaVMT'] = round(dfg_length['AverageAADT'] * dfg_length['Length'])

    
#     return dfg_length


def calc_VMT_AADT_2020(df):
    df = df[df['AADT'] != ""]
    print(df.columns)
    # Calculate the length
    df['Length'] = df['End_Point'] - df['Begin_Point']
    df['VMT'] = df['Length'] * df['AADT']
    

    # ### Calculate the Average
    urban_codes = df['URBAN_CODE'].unique()


    average = []
    for u in urban_codes:
        df_filtered = df[df['URBAN_CODE'] == u]
        average.append([int(u), round(df_filtered['AADT'].mean(),3)])
    
    df = df.groupby(["URBAN_CODE", "F_System"]).agg({"Length":'sum',"VMT":'sum'})
    df = df.reset_index()
    
    df['AverageAADT'] = 0
    for index, row in df.iterrows():
        for r in average:
            if(int(row['URBAN_CODE']) == int(r[0])):
                df.loc[index,'AverageAADT'] = r[1]

    # df['StateUrbanizedAreaVMT'] = round(df['AverageAADT'] * df['Length'])
    
    df['StateUrbanizedAreaVMT'] = round(df['VMT'])
    return df

def calc_VMT_AADT_2021(df):
    df = df[df['AADT_dataitem'] != ""]
    print(df.columns)
    # Calculate the length
    df['Length'] = df['End_Point'] - df['Begin_Point']
    df['VMT'] = df['Length'] * df['AADT_dataitem']
    
    # ### Calculate the Average
    urban_codes = df['URBAN_CODE'].unique()

    average = []
    for u in urban_codes:
        df_filtered = df[df['URBAN_CODE'] == u]
        average.append([int(u), round(df_filtered['AADT_dataitem'].mean(),3)])
    
    df = df.groupby(["URBAN_CODE", "F_System"]).agg({"Length":'sum',"VMT":'sum'})
    df = df.reset_index()
    
    df['AverageVMT'] = df['VMT'] / df['Length']
    # for index, row in df.iterrows():
    #     for r in average:
    #         if(int(row['URBAN_CODE']) == int(r[0])):
    #             df.loc[index,'AverageAADT'] = r[1]

    # df['StateUrbanizedAreaVMT'] = round(df['AverageAADT'] * df['Length'])
    
    df['StateUrbanizedAreaVMT'] = round(df['VMT'])
    return df


def federalRoads():
    # Read Fed_Roads_Summary.csv
    df_fed_roads = pd.read_csv("Fed_Roads_Summary.csv", dtype={"Urban_Code": int})
    # df_fed_roads = pd.read_excel("Fed_Roads_Summary.xlsx", dtype={"Urban_Code":int})
    df_fed_roads = df_fed_roads[df_fed_roads['Urban_Code'] != ""]
    # df_fed_roads['Urban_Code'] = df_fed_roads['Urban_Code'].astype(int)
    df_fed_grouped = df_fed_roads.groupby(["Urban_Code", "F_System"])["Section_Length"].sum().reset_index().set_index("Urban_Code")
    return df_fed_grouped


def cityMileage():
    # Read City_NonFedAid_Summary.xlsx
    df_city_nonFedAid = pd.read_excel('City_NonFedAid_Summary.xlsx', dtype={"Urban_Code":int})
    df_city_nonFedAid = df_city_nonFedAid[df_city_nonFedAid['Urban_Code'] != ""]
    df_city_grouped = df_city_nonFedAid.groupby(["Urban_Code", "F_System"])["Section_Length"].sum().reset_index().set_index("Urban_Code")
    return df_city_grouped


def main():
    # df = pd.read_excel('_joined_Urban_Summaries.xlsx', dtype={"URBAN_CODE":str}).fillna("")
    df = pd.read_excel('_NEW_joined_for_Mainframe.xlsx', dtype={"URBAN_CODE":str}).fillna("")
    df = df[df['URBAN_CODE'] != ""]
    df['URBAN_CODE'] = df['URBAN_CODE'].astype(int)
    df = filter_input(df)

    df_fed_grouped = federalRoads()
    df_city_grouped = cityMileage()

    df_average = calc_VMT_AADT_2021(df)
    df_average.rename(columns={'URBAN_CODE':'Urban_Code'}, inplace=True)

    df_average = pd.merge(df_average, df_fed_grouped, how='left', left_on=['Urban_Code','F_System'], right_on = ['Urban_Code','F_System'] ).fillna(0)
    df_average = pd.merge(df_average, df_city_grouped, how='outer', left_on=['Urban_Code','F_System'], right_on = ['Urban_Code','F_System'] ).fillna(0)
    df_average.rename(columns={'Section_Length_x':'UrbanizedFedRoadLength', 'Section_Length_y':'UrbanizedMunicipalRoadLength'}, inplace=True)

    '''
        Calculate the Local VMT
        - create the column FedUrbanizedAreaVMT
        - update the value as: UrbanizedFedRoadLength * AverageAADT = FedUrbanizedAreaVMT
        - create the column MunicipalUrbanizedAreaVMT
        - update the value as: UrbanizedMunicipalRoadLength * AverageAADT = MunicipalUrbanizedAreaVMT 
        - Create the column Local_VMT that will receive (StateUrbanizedAreaVMT + FedUrbanizedAreaVMT + MunicipalUrbanizedAreaVMT) for each Urban_Code
    '''
    df_average['FedUrbanizedAreaVMT'] = round(df_average['UrbanizedFedRoadLength'] * df_average['AverageVMT'],3)
    df_average['MunicipalUrbanizedAreaVMT'] = round(df_average['UrbanizedMunicipalRoadLength'] * df_average['AverageVMT'],3)
    df_average['Local_VMT'] = round(df_average['StateUrbanizedAreaVMT'] + df_average['FedUrbanizedAreaVMT'] + df_average['MunicipalUrbanizedAreaVMT'],0)
    
    print(df_average)

    for index,row in df_average.iterrows():
        result.append(write_result(row))

    

main()

df_result = pd.DataFrame(result)
df_result.to_excel("Non_Federal_Aid_Summaries_Nov_21.xlsx", index=False)
df_result.to_csv("Non_Federal_Aid_Summaries_Nov_21.csv", sep='|', index=False)

print(df_result.head)
print("Finished!")



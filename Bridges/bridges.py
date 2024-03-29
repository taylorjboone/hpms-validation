import pandas as pd
import numpy as np
from datetime import date

# --- Last year submision file
df_lastSub = pd.read_csv('LastYearSubmitted\DataItem_4_structure_type.csv', sep="|")

# --- NBI File, sheet "All Structures (7,609)" 
df_NBI_all = pd.read_excel('Inventory_HPMS_Summary_Data_Bridges_2024 reformatted with LRS data_V1.xlsx', sheet_name="All Structures (7,609)") 

# --- NBI File, sheet "Archived BARS" 
df_NBI_archiveBars = pd.read_excel('Inventory_HPMS_Summary_Data_Bridges_2024 reformatted with LRS data_V1.xlsx', sheet_name="Archived BARS") 



def dropArchiveBridges(df_lastSub, df_NBI_archiveBars):
    # 'NBI 8' has a lot of leading 0, solve it by extracting the last 6 digits of the string.
    df_NBI_archiveBars['NBI 8'] = df_NBI_archiveBars['NBI 8'].str[-6:]
    barsidToDelete = df_NBI_archiveBars['NBI 8'].unique()
    barsidLastSub = df_lastSub['ValueText'].unique()
    for d in barsidToDelete:
        if d in barsidLastSub:
            print(d, " retire bridge is in the main file")






### - TODO - Better way to do this 
'''
    - Get the NBI file, filter the column 42A
        - Column 42A will filter out the 0,2,3,"" (empty) as showing below
            - '0 - Other'
            - '2 - Railroad'
            - '3 - Pedestrian Exclusively'
            - ''
        - 

        - NBI columns - 
            - BARS Number 
            - LRS RouteID
            - LRS Milepoint
            - NBI 49: Structure Length
        
        - Filter last submission with the NBI Barsid to see each Bridge still active or df_KeepSub
'''
def main(df_NBI_all, df_lastSub):
    print(len(df_NBI_all))
    df_NBI_all.drop(df_NBI_all[df_NBI_all['NBI 42A: Type of Service: ON Bridge'].isna()].index, inplace=True)
    df_NBI_all.drop(df_NBI_all[df_NBI_all['NBI 42A: Type of Service: ON Bridge'].isin(['0 - Other','2 - Railroad','3 - Pedestrian Exclusively'])].index, inplace=True)
    print(len(df_NBI_all))

    uniqBarsidNBI = df_NBI_all['BARS Number'].unique()
    barsidLastSub = df_lastSub['ValueText'].unique()

    df_keepSub = df_lastSub[df_lastSub['ValueText'].isin(uniqBarsidNBI)] #BeginPoint|EndPoint|RouteID|ValueText - where ValueText is the barsid
    df_keepSub.rename(columns={'BeginPoint':'bmp','EndPoint':'emp','RouteID':'RouteID','ValueText':'barsid'}, inplace=True)
    df_keepSub = df_keepSub[['bmp','emp','RouteID','barsid']]

    ## get NBI bridges that are not on the submission for last year
    df_NBI_work = df_NBI_all[~df_NBI_all['BARS Number'].isin(df_keepSub['barsid'].unique())]
    df_NBI_work.rename(columns={'LRS Milepoint':'mp','NBI 49: Structure Length':'len','LRS RouteID':'RouteID','BARS Number':'barsid'}, inplace=True)
    df_NBI_work = df_NBI_work[['mp','len','RouteID','barsid','NBI 16: Latitude','NBI 17: Longitude']]
    # assuming that the point is in the middle of the bridge and knowing that the length is collect in feet and must be converted to miles.
    # Halfing the length and converting it to Mile then use the LRS Milepoint - the halfed mile length to get the BMP
    df_NBI_work['len_halfed_mile'] = ((df_NBI_work['len']/2) * 0.0001894) 
    df_NBI_work['bmp'] = round(df_NBI_work['mp'] - df_NBI_work['len_halfed_mile'],3)
    df_NBI_work['emp'] = round(df_NBI_work['mp'] + df_NBI_work['len_halfed_mile'],3)

    df_NBI_work['bmp'] = np.where(df_NBI_work['bmp'] < 0, 0 , df_NBI_work['bmp'])
    df_NBI_work['emp'] = np.where(df_NBI_work['emp'] < 0, 0 , df_NBI_work['emp'])

    df_NBI_work = df_NBI_work[['bmp','emp','RouteID','barsid']]
    df_result = pd.concat([df_keepSub,df_NBI_work])
    df_result['barsid'] = df_result['barsid'].str.strip()

    return df_result



def format_df(df_result):
    '''
        Format the file to match the Dataitem
        BeginPoint|EndPoint|RouteID|ValueText|BeginDate|ValueNumeric|DataItem|StateID|Comments|ValueDate

        df_NBI_work[['bmp','emp','RouteID','barsid']]
        
    '''
    print(f'01/01/{date.today().year -1}')
    # format the result 
    df_result['BeginDate']= f'01/01/{date.today().year -1}'
    df_result['StateID']= 54
    df_result['DataItem']= 'STRUCTURE_TYPE'
    df_result['ValueDate']= ''
    df_result['ValueNumeric']= ''
    df_result['Comments']= ''

    df_result.rename(columns={'bmp':'BeginPoint','emp':'EndPoint','barsid':'ValueText'}, inplace=True)
    return df_result[['BeginDate','StateID','RouteID','BeginPoint','EndPoint','DataItem', 'ValueNumeric','ValueText','ValueDate', 'Comments']]

    




df_result = main(df_NBI_all, df_lastSub)
df_result = format_df(df_result)
print(df_result)


# df_result = format_df(df_result)
df_result.to_csv('result_DataItem_4_structure_type.csv', sep='|')










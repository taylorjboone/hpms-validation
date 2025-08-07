import pandas as pd
import numpy as np
from datetime import date
import os
import wvdot_utils as wu


# from ..._util import printProgressBar

IN_PROGRESS = os.path.exists('Bridges_in_progress.xlsx')

# --- Last year submision file
df_lastSub = pd.read_csv('LastYearSubmitted\DataItem_4_structure_type.csv', sep="|")
df_lastSub.rename(columns={'BeginPoint':'bmp','EndPoint':'emp','RouteID':'RouteID','ValueText':'barsid'}, inplace=True)


# --- NBI File, sheet "All Structures (7,609)" 
df_NBI_all = pd.read_excel('Inventory_HPMS_SUMMARY_DATA_BRIDGE_3-25-2025_Submitted.xlsx', sheet_name="Vehicular Bridges (7,275)") 
df_NBI_all.rename(columns={'LRS Milepoint':'mp','NBI 49: Structure Length':'len_feet','LRS RouteID':'RouteID','BARS Number':'barsid','NBI 16: Latitude':'latitude','NBI 17: Longitude':'longitude'}, inplace=True)
# From the Bridge file, drop NAN,'0 - Other','2 - Railroad','3 - Pedestrian Exclusively'
df_NBI_all.drop(df_NBI_all[df_NBI_all['NBI 42A: Type of Service: ON Bridge'].isna()].index, inplace=True)
df_NBI_all.drop(df_NBI_all[df_NBI_all['NBI 42A: Type of Service: ON Bridge'].isin(['0 - Other','2 - Railroad','3 - Pedestrian Exclusively'])].index, inplace=True)


# --- NBI File, sheet "Archived BARS" 
df_NBI_archiveBars = pd.read_excel('Inventory_HPMS_SUMMARY_DATA_BRIDGE_3-25-2025_Submitted.xlsx', sheet_name="Archived BARS") 

# df_arnold = pd.read_csv('Routes_Arnold_2025.csv')
df_arnold = pd.read_csv('Arnold_25.csv')

def drop_archived_bridges(df_lastSub, df_NBI_archiveBars):
    # 'NBI 8' has a lot of leading 0, solve it by extracting the last 6 digits of the string.
    df_NBI_archiveBars['NBI 8'] = df_NBI_archiveBars['NBI 8'].str[-6:] # the barsid on this file looks like this 00000000006A322
    barsidToDelete = df_NBI_archiveBars['NBI 8'].unique()
    barsidLastSub = df_lastSub['ValueText'].unique()
    for d in barsidToDelete:
        if d in barsidLastSub:
            print(d, " retire bridge is in the main file")


def filter_keep_from_last_submission(uniqBarsidNBI):
    '''
        Based on the last submission file, check the BarsId that are still present on the NBI file
        @param uniqBarsidNBI - List of the unique BarsIds on the NBI file

        @global @param df_lastSub
        @return df['bmp','emp','RouteID','barsid'] with the bridges still active from last submission file.
    '''
    global df_lastSub
    df_keepSub = df_lastSub[df_lastSub['barsid'].isin(uniqBarsidNBI)] #BeginPoint|EndPoint|RouteID|ValueText - where ValueText is the barsid
    return df_keepSub[['bmp','emp','RouteID','barsid']]


def filter_new_bridges():
    '''
        Based on the NBI file, filter for the bridges that weren't on the last submission.

        @global @param df_NBI_all
        @global @param df_lastSub

        @return df[['mp','len','RouteID','barsid','latitude','longitude']] with the NBI data on the new bridges
    '''
    global df_NBI_all
    global df_lastSub

    ## get NBI bridges that are not on the submission for last year
    df = df_NBI_all[~df_NBI_all['barsid'].isin(df_lastSub['barsid'].unique())]
    return df[['mp','len_feet','RouteID','barsid','latitude','longitude']]


# Print iterations progress
def printProgressBar (iteration, total, prefix = '', suffix = '', decimals = 1, length = 100, fill = '█', printEnd = "\r"):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
        printEnd    - Optional  : end character (e.g. "\r", "\r\n") (Str)
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print(f'\r{prefix} |{bar}| {percent}% {suffix}', end = printEnd)
    # Print New Line on Complete
    if iteration == total: 
        print()


def convert_len_feet_to_miles(df):
    '''
        Convert the NBI original feet lenght to miles, create the columns "len_mile_half" and "len_mile"
        @param df dataframe with Brige information - require the column "len_feet"
        @return df with the created columns "len_mile_half" and "len_mile"
    '''
    df['len_mile_half'] = ((df['len_feet']/2) * 0.0001894) 
    df['len_mile'] = ((df['len_feet']) * 0.0001894) 
    
    return df

# def calcutate_bmp_emp(df):
#     '''
#         Calculate the BMP and EMP, assume MP is in the middle of the Bridge.
#         \n - create the columns "bmp" and "emp".
#         \n
#         @param df dataframe with Brige information - require the columns "mp" and "len_mile_half"

#         @return df with the created columns "bmp" and "emp"
#     '''
#     df['bmp'] = round(df['mp'] - df['len_mile_half'],3)
#     df['emp'] = round(df['mp'] + df['len_mile_half'],3)
    
#     df['bmp'] = np.where(df['bmp'] < 0, 0 , df['bmp'])
#     df['emp'] = np.where(df['emp'] < 0, 0 , df['emp'])

#     return df

def calculate_bmp_emp(df):
    '''
        Calculate the BMP and EMP, assume MP is in the middle of the Bridge.
        Please note this year (2025) MP is now assumed to be at the beginning of the bridge
        \n - create the columns "bmp" and "emp".
        \n
        @param df dataframe with Brige information - require the columns "mp" and "len_mile_half"

        @return df with the created columns "bmp" and "emp"
    '''
    df['bmp'] = round(df['mp'],3)
    df['emp'] = round(df['mp'] + (df['len_feet']*(1/5280)),3)
    
    df['bmp'] = np.where(df['bmp'] < 0, 0 , df['bmp'])
    df['emp'] = np.where(df['emp'] < 0, 0 , df['emp'])

    return df


def validations_bmp_emp_same_seg(row):
    error = []
    bmp_emp_in_bounds_different_segments = []
    bmp_emp_out_of_bounds = []
    bmp_out_of_bounds = None
    emp_out_of_bounds = None

    df_arnold_byRowRouteID = df_arnold[df_arnold['ROUTE_ID']==row['RouteID']]
    if(len(df_arnold_byRowRouteID) == 0):
        return 'Route ID not found'

    # print(f'---- This Routeid has {len(df_arnold_byRowRouteID)} segment(s).')

    for i, r in df_arnold_byRowRouteID.iterrows():
        from_measure,to_measure = float(r.BMP),float(r.EMP)
        from_measure,to_measure = round(from_measure,3),round(to_measure,3)

        # if find a valid record finish the validation
        if (row.bmp >= from_measure and row.emp <= to_measure):
            return pd.NA if len(list(filter(None,error))) == 0 else ''.join(str(x) for x in list(filter(None,error)))

        # check if BMP is in the lenght and EMP is out and create the message
        elif (row.bmp >= from_measure and row.bmp < to_measure):
            print(from_measure,to_measure,row.bmp,row.emp,'here')
            bmp_out_of_bounds = 'EMP out of bounds. Actual EMP: %s' % to_measure
            bmp_emp_in_bounds_different_segments.append('Both BMP and EMP are within bounds, but in different segments of this road. BMP: %s and EMP: %s' % (from_measure,to_measure))

        # check if EMP is in the lenght and BMP is out and create the message
        elif (row.emp > from_measure and row.emp <= to_measure):
            emp_out_of_bounds = 'BMP out of bounds. Actual BMP: %s' % from_measure
            bmp_emp_in_bounds_different_segments.append('Both BMP and EMP are within bounds, but in different segments of this road. BMP: %s and EMP: %s' % (from_measure,to_measure))
        
        else:
            bmp_emp_out_of_bounds.append('Both BMP and EMP are out of bounds. Actual BMP: %s and actual EMP: %s' % (from_measure,to_measure))


    #validation for the messages 
    if (bmp_out_of_bounds and emp_out_of_bounds):
        error += bmp_emp_in_bounds_different_segments
    elif(bmp_out_of_bounds):
        error.append(bmp_out_of_bounds)
    elif(emp_out_of_bounds):
        error.append(emp_out_of_bounds)
    else:
        error += bmp_emp_out_of_bounds

    

    error_msg = pd.NA if len(list(filter(None,error))) == 0 else ''.join(str(x) for x in list(filter(None,error)))
    print(error_msg)
    return error_msg # Remove None values


def validations(df, in_progress):
    '''
        Validations
            - Check if Bridge Routeid exists in the Arnold file
            - Check if the BMP and/or EMP of the Bridge exists within the Road

        @param df - Bridge dataframe 
    
    '''
    global df_arnold

    # Do the analisis on the Route Id at the first time
    if(not in_progress): 
        # Call wvdot_utils conflate point df to get the closest dominant road from the point
        df.rename(columns={'RouteID':'RouteID_original', 'mp':'mp_original'},inplace=True)
        df = wu.geom_to_measures_progressive_tolerance.conflate_point_df(df, 'latitude','longitude')
        df.rename(columns={'RouteID':'suggest_RouteID', 'mp':'suggest_mp','RouteID_original':'RouteID', 'mp_original':'mp'},inplace=True)


    df['suggest_bmp'] = pd.NA
    df['suggest_emp'] = pd.NA

    printProgressBar(0, len(df), prefix = 'Progress:', suffix = 'Complete', length = 50)
    for index, row in df.iterrows():
        error_ = validations_bmp_emp_same_seg(row)
        df.loc[index,'error'] = error_
        
        if (pd.notna(error_) == True):
            if('BMP out of bounds. Actual BMP: ' in error_):
                suggest_bmp = float(error_.replace('BMP out of bounds. Actual BMP: ',''))
                df.loc[index,'suggest_bmp'] = suggest_bmp
                df.loc[index,'suggest_emp']  = abs(suggest_bmp + row['len_mile'])

            if('EMP out of bounds. Actual EMP: ' in error_):
                suggest_emp = float(error_.replace('EMP out of bounds. Actual EMP: ',''))
                df.loc[index,'suggest_emp'] = suggest_emp
                df.loc[index,'suggest_bmp']  = abs(suggest_emp - row['len_mile'])



        # bmp_out_of_bounds = 'EMP out of bounds. Actual EMP: %s' % to_measure
        # emp_out_of_bounds = 'BMP out of bounds. Actual BMP: %s' % from_measure

        print(df.loc[index,'error'])


        printProgressBar(index, len(df), prefix = 'Progress:', suffix = 'Complete', length = 50)

        # exit()
    df['RouteID'] = df['RouteID'].astype('string')
    df.to_excel('Bridges_in_progress.xlsx', index=False)
    return df

def get_lat_long(df):
    '''
        Get the Latitute and Longitute from the NBI file
        @param df with the bridges without Latitute and Longitute
        @return df with the added columns 
    '''
    global df_NBI_all

    # 'barsid','NBI 16: Latitude':'latitude','NBI 17: Longitude':'longitude'}, inplace=True)
    df_nbi = df_NBI_all[['mp','len_feet','barsid','latitude','longitude']]
    df_nbi = df_nbi[df_nbi['barsid'].isin(df.barsid.unique())]

    return  pd.merge(df, df_nbi, left_on="barsid", right_on="barsid", how='outer')


def main():
    '''
        @require @global df_NBI_all 
        @require @global df_lastSub 
        - Get the NBI file, filter the column 42A
            - Column 42A will filter out the 0,2,3,"" (empty) as showing below
                - '0 - Other'
                - '2 - Railroad'
                - '3 - Pedestrian Exclusively'
                - ''

            - NBI columns - 
                - BARS Number 
                - LRS RouteID
                - LRS Milepoint
                - NBI 49: Structure Length
            
            - Filter last submission with the NBI Barsid to see each Bridge still active or df_KeepSub
    '''
    print(" ----- MAIN ----- ")
    global df_NBI_all 
    global df_lastSub 

    uniqBarsidNBI = df_NBI_all['barsid'].unique()

    df_keep_from_last_sub = filter_keep_from_last_submission(uniqBarsidNBI)
    df_keep_from_last_sub = get_lat_long(df_keep_from_last_sub)
    df_keep_from_last_sub = convert_len_feet_to_miles(df_keep_from_last_sub)

    print(df_keep_from_last_sub.columns)

    df_NBI_new_bridges = filter_new_bridges()
    df_NBI_new_bridges = convert_len_feet_to_miles(df_NBI_new_bridges)
    df_NBI_new_bridges = calculate_bmp_emp(df_NBI_new_bridges)
    print(df_NBI_new_bridges.columns)
    
    # exit()
    df_result = pd.concat([df_NBI_new_bridges,df_keep_from_last_sub])
    df_result['barsid'] = df_result['barsid'].str.strip()

    print(df_result.columns)

    return df_result



def format_df(df_result):
    '''
        Format the file to match the Dataitem
        BeginPoint|EndPoint|RouteID|ValueText|BeginDate|ValueNumeric|DataItem|StateID|Comments|ValueDate

        df_NBI_new_bridges[['bmp','emp','RouteID','barsid']]
        
    '''
    # df_result = df_result[['bmp','emp','RouteID','barsid']]
    print(f'01/01/{date.today().year -1}')
    # format the result 
    df_result['BeginDate']= f'01/01/{date.today().year -1}'
    df_result['StateID']= 54
    df_result['DataItem']= 'STRUCTURE_TYPE'
    df_result['ValueDate']= ''
    df_result['ValueNumeric']= '1'

    df_result.sort_values(by=['RouteID', 'bmp', 'barsid'],inplace=True)

    df_result.rename(columns={'bmp':'BeginPoint','emp':'EndPoint','barsid':'ValueText'}, inplace=True)
    return df_result[['BeginDate','StateID','RouteID','BeginPoint','EndPoint','DataItem', 'ValueNumeric','ValueText','ValueDate', 'Comments']]

    


# Check in case the Bridges_in_progress.xlsx created in the validation exists. 
# The existance of this file means that someone already run the create bridge and they are working on fixing the bugs.
if not IN_PROGRESS:
    df_result = main()
    df_result['Comments']= ''
else: 
    df_result = pd.read_excel('Bridges_in_progress.xlsx')

df_result = validations(df_result, IN_PROGRESS)

print(df_result[df_result['error'].notna()])



if(len(df_result['error'].notna()) > 0 ):
    print('- - - Bridges file has errors, please correct the -- issues presented on the Bridges_in_progress.xlsx')


df_result = format_df(df_result)
print(df_result)
# df_result = format_df(df_result)
df_result.to_csv('DataItem_4_structure_type.csv', sep='|', index=False)










import pandas as pd
import os
import json
import shutil
import time
import hashlib
import geopandas as gpd

def get_sep(fn):
    with open(fn,'r') as f:
        bs = f.read(2000)
        ccount,pcount,semicount,tab_count = 0,0,0,0
        for i in bs:
            if i == ',':
                ccount+=1 
            elif i == '|':
                pcount+=1 
            elif i == 'semicount':
                semicount+=1
            elif i == '\t':
                tab_count+=1 

            if ccount>5: return ','
            if pcount>5: return '|'
            if semicount>5: return ';'
            if tab_count>5: return '\t'
    return ''

def init_file(fn='all_submission_data.csv'):
    basedf = pd.DataFrame(columns=['ROUTEID', 'BMP', 'EMP'])
    basedf.to_csv(fn)

# creating an individual operation
def create_op(fn,cols,is_end=False,is_beg=False,outfilename='all_submission_data.csv',prefix=0):
    if prefix>1: 
        prefix = f'_{prefix}'
    else:
        prefix = ''
    
    if is_beg: 
        return {
                "operation":"overlay",
                "base_file":outfilename,
                "split_file":fn,
                "split_fields":cols+['Begin_Date'],
                "out_file":"temp",
                'prefix':prefix
        }
    elif is_end:
        return {
                "operation":"overlay",
                "base_file":"previous",
                "split_file":fn,
                "split_fields":cols,
                "out_file":outfilename,
                'prefix':prefix
        }
    else:
        return {
                "operation":"overlay",
                "base_file":"previous",
                "split_file":fn,
                "split_fields":cols,
                "out_file":"temp",
                'prefix':prefix
        }

# Gets MD5 from file 
def getmd5(filename):
    return hashlib.md5(open(filename,'rb').read()).hexdigest()


def combine_items(fns,outfilename='all_submission_data.csv'):
    item_dict = {}
    '''
    Takes a list of data item files (must be unique) and writes the output 
    csv to a filename. 
    '''
    s = time.time()
    if not os.path.exists('tmp'):os.mkdir('tmp')

    # number of files and initializing output
    numfns,pos,operations = len(fns),0,[]
    init_file(fn=outfilename)    
    
    for file in fns:
        # getting sep and reading in file
        sep = get_sep(file)
        df = pd.read_csv(file,sep=sep,dtype={'ValueText':str,'Value_Text':str})
        itemcol = ''
        # formatting the columns properly 
        if 'Data_Item' in df.columns.tolist():
            bv = False
            data_item = df['Data_Item'].iloc[0].upper()        
            itemcol = 'Data_Item'
            if data_item == 'ALTERNATIVE_ROUTE_NAME': 
                data_item = 'ROUTE_NAME'
                bv = True
            elif data_item == 'PCT_PEAK_SINGLE':
                data_item = 'PCT_DH_SINGLE_UNIT'
                bv = True
            elif data_item == 'PCT_PEAK_COMBINATION':
                data_item = 'PCT_DH_COMBINATION'
                bv = True
            elif data_item == 'YEAR_LAST_CONSTRUCT':
                data_item = 'YEAR_LAST_CONSTRUCTION'
                bv = True
            elif data_item == 'YEAR_LAST_IMPROVE':
                data_item = 'YEAR_LAST_IMPROVEMENT'
                bv = True
            elif data_item == 'THICK_RIGID':
                data_item = 'THICKNESS_RIGID'
                bv = True
            elif data_item == 'THICK_FLEX':
                data_item = 'THICKNESS_FLEXIBLE'
                bv = True
            elif data_item == 'BASE_THICK':
                data_item = 'BASE_THICKNESS'
                bv = True
            elif data_item == 'COUNTY_CODE':
                data_item = 'COUNTY_ID'
                bv = True
            elif data_item == 'Yr_Last_Improv'.upper():
                data_item = 'YEAR_LAST_IMPROVEMENT'
                bv = True
            elif data_item == 'Yr_Last_Construction'.upper():
                data_item = 'YEAR_LAST_CONSTRUCTION'
                bv = True
            

            if bv: df['Data_Item'] = data_item
            if 'Value_Date' in df.columns:
                # t = df[df['Value_Date'].isin(['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'])] 
                # i = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
                if 2 in df.Value_Date.astype(str).str.split('-').map(lambda x:len(x)).unique(): 
                    df['Value_Date'] = '06-06-2021'


            df.rename(columns={'Value_Numeric': f'{data_item}', 'Value_Date':f'{data_item}_VALUE_DATE', 'Value_Text':f'{data_item}_VALUE_TEXT', 'Begin_Point':'BMP', 'End_Point':'EMP', 'Route_ID':'ROUTEID'}, inplace=True)
        elif 'DataItem' in df.columns.tolist():
            data_item = df['DataItem'].iloc[0]
            itemcol = 'DataItem'
            df.rename(columns={'ValueNumeric': f'{data_item}', 'ValueDate':f'{data_item}_VALUE_DATE', 'ValueText':f'{data_item}_VALUE_TEXT', 'BeginPoint':'BMP', 'EndPoint':'EMP', 'RouteID':'ROUTEID'}, inplace=True)
        elif 'HPMS_SAMPLE_NO' in df.columns.tolist():
            data_item = 'HPMS_SAMPLE_NO'
            df.rename(columns={'ValueNumeric': f'{data_item}', 'ValueDate':f'{data_item}_VALUE_DATE', 'ValueText':f'{data_item}_VALUE_TEXT', 'BeginPoint':'BMP', 'EndPoint':'EMP', 'RouteID':'ROUTEID'}, inplace=True)
        elif 'Sample_ID' in df.columns.tolist():
            data_item = 'HPMS_SAMPLE_NO'
            df.rename(columns={'Sample_ID': f'{data_item}', 'ValueDate':f'{data_item}_VALUE_DATE', 'ValueText':f'{data_item}_VALUE_TEXT', 'BeginPoint':'BMP', 'EndPoint':'EMP', 'RouteID':'ROUTEID'}, inplace=True)
        else:
            print('COLUMN FORMAT ERROR')
            print(df)
        

        # doing curves and grades 
        # defining columns to be carried over
        cols = [f'{data_item}', f'{data_item}_VALUE_DATE', f'{data_item}_VALUE_TEXT']
        if 'CURVES_' in data_item and df[itemcol].nunique()>1:
            df = pd.read_csv(file,sep=sep,dtype={'ValueText':str,'Value_Text':str})
            pos2 = 0
            nuniq = df[itemcol].nunique()
            for ind,tmpdf in df.groupby(itemcol):
                data_item = ind.upper()
                cd = item_dict.get(data_item,False)
                if cd ==False:
                    item_dict[data_item] = {'file':file,'md5hex':getmd5(file),'seen':1}
                else:
                    cd['seen'] = cd['seen']+1
                    item_dict[data_item] = cd
                if itemcol == 'DataItem':
                    tmpdf.rename(columns={'ValueNumeric': f'{data_item}', 'ValueDate':f'{data_item}_VALUE_DATE', 'ValueText':f'{data_item}_VALUE_TEXT', 'BeginPoint':'BMP', 'EndPoint':'EMP', 'RouteID':'ROUTEID'}, inplace=True)
                elif itemcol == 'Data_Item': 
                    tmpdf.rename(columns={'Value_Numeric': f'{data_item}', 'Value_Date':f'{data_item}_VALUE_DATE', 'Value_Text':f'{data_item}_VALUE_TEXT', 'Begin_Point':'BMP', 'End_Point':'EMP', 'Route_ID':'ROUTEID'}, inplace=True)

                filename = f'tmp/{data_item}.csv'
                tmpdf.to_csv(filename,index=False)
                cols = [f'{data_item}', f'{data_item}_VALUE_DATE', f'{data_item}_VALUE_TEXT']
                operations.append(create_op(filename,cols,is_beg=pos==0 and pos2==0,is_end=pos==(numfns-1) and pos2==(nuniq-1),outfilename=outfilename,prefix=item_dict.get(data_item,{}).get('seen',0)))
                pos2+=1
        elif 'GRADES_' in data_item and df[itemcol].nunique()>1:
            df = pd.read_csv(file,sep=sep,dtype={'ValueText':str,'Value_Text':str})
            # df.drop(cols,axis=1,inplace=True)
            pos2 = 0
            nuniq = df[itemcol].nunique()
            for ind,tmpdf in df.groupby(itemcol):
                data_item = ind.upper()
                cd = item_dict.get(data_item,False)
                if cd ==False:
                    item_dict[data_item] = {'file':file,'md5hex':getmd5(file),'seen':1}
                else:
                    cd['seen'] = cd['seen']+1
                    item_dict[data_item] = cd
                if itemcol == 'DataItem':
                    tmpdf = tmpdf.rename(columns={'ValueNumeric': f'{data_item}', 'ValueDate':f'{data_item}_VALUE_DATE', 'ValueText':f'{data_item}_VALUE_TEXT', 'BeginPoint':'BMP', 'EndPoint':'EMP', 'RouteID':'ROUTEID'})
                elif itemcol == 'Data_Item': 
                    tmpdf = tmpdf.rename(columns={'Value_Numeric': f'{data_item}', 'Value_Date':f'{data_item}_VALUE_DATE', 'Value_Text':f'{data_item}_VALUE_TEXT', 'Begin_Point':'BMP', 'End_Point':'EMP', 'Route_ID':'ROUTEID'})

                # tmpdf.rename(columns={'ValueNumeric': f'{data_item}', 'ValueDate':f'{data_item}_VALUE_DATE', 'ValueText':f'{data_item}_VALUE_TEXT', 'BeginPoint':'BMP', 'EndPoint':'EMP', 'RouteID':'ROUTEID'}, inplace=True)
                filename = f'tmp/{data_item}.csv'
                tmpdf.to_csv(filename,index=False)
                cols = [f'{data_item}', f'{data_item}_VALUE_DATE', f'{data_item}_VALUE_TEXT']
                operations.append(create_op(filename,cols,is_beg=pos==0 and pos2==0,is_end=pos==(numfns-1) and pos2==(nuniq-1),outfilename=outfilename,prefix=item_dict.get(data_item,{}).get('seen',0)))
                pos2+=1
        else:
            # accounting for the fucked value_numeric issue with urban code, really this should be value text so were 
            # zfilling this value to 5
            if data_item == 'URBAN_CODE': df['URBAN_CODE'] = df.URBAN_CODE.astype(str).str.zfill(5)
            cd = item_dict.get(data_item,False)
            if cd ==False:
                item_dict[data_item] = {'file':file,'md5hex':getmd5(file),'seen':1}
            else:
                    cd['seen'] = cd['seen']+1
                    item_dict[data_item] = cd
                    # print('seen twice!!!!',data_item,'\n\n\n\n')
            filename = f'tmp/{data_item}.csv'
            df.to_csv(filename,index=False)
            operations.append(create_op(filename,cols,is_beg=pos==0,is_end=pos==(numfns-1),outfilename=outfilename,prefix=item_dict.get(data_item,{}).get('seen',0)))
        pos+=1
        print(f'[{str(pos).zfill(2)}/{str(pos).zfill(2)}] Initial Column Remap')

    myjson = {'operations':operations}
    with open('myfile.json','w') as f:
        f.write(json.dumps(myjson))
    os.system('lrsops overlay --operations myfile.json')
    shutil.rmtree('tmp')
    # os.remove('myfile.json')
    print(time.time()-s,'Time to Overlay HPMS Files')
    newlist = []
    for k,v, in item_dict.items():
        v['item'] = k 
        newlist.append(v)
    return pd.DataFrame(newlist)


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
    val = df.iloc[0].Data_Item
    val = fix_this(val)
    val = 'URBAN_CODE' if val == 'URBAN_ID' else val
    df['Data_Item'] = val 
    return df 

# returns wheter the columns exist 
# and wheether the schema is v9 
def is_event_item_type(df):
    cols = df.columns 
    cols_v9 = ['Begin_Point','End_Point','Data_Item','Route_ID']
    cols_v8 = ['BeginPoint','EndPoint','DataItem','RouteID']
    v8b = len([i for i  in cols_v8 if i in df.columns]) == len(cols_v8 )
    v9b = len([i for i  in cols_v9 if i in df.columns]) == len(cols_v9 )
    if 'Sample_ID' in df: 
        df['Data_Item'] = 'HPMS_SAMPLE_NO'
    if 'HPMS_SAMPLE_NO' in df: 
        df['Data_Item'] = 'HPMS_SAMPLE_NO'
    
    return v8b or v9b or ('Sample_ID' in cols or 'HPMS_SAMPLE_NO' in cols),v9b,df

v8_v9 =  {'BeginDate':'Begin_Date',"StateID":'State_Code',"RouteID":'Route_ID',
            "BeginPoint":'Begin_Point',"EndPoint":'End_Point',"DataItem":'Data_Item',
            "SectionLength":'Section_Length','SECTION_LENGTH':'Section_Length','ValueText':'Value_Text','ValueNumeric':'Value_Numeric','ValueDate':'Value_Date','BeginDate':'Year_Record'}

def convert_fn_if_needed(fn,outdir='tmp_normalized'):
    df = pd.read_csv(fn,sep="|")
    if len([i for i in df.columns if i.upper()==i]) == len(df.columns) and len(df.columns) > 5:
        df.columns = ['_'.join([ii.title() for ii in i.lower().split('_')]) for i in df.columns]
        df.rename(columns={'Route_Id':'Route_ID'},inplace=True)
        print('Remapped columns',df.columns)
    endfn = fn.split('/')[-1]

    is_event,is_v9,df = is_event_item_type(df)
    if is_event: 
        if not is_v9:
            df.rename(columns=v8_v9,inplace=True)
            df = fix_item_name(df)
        if 'Year_Record' in  df.columns: 
            val = df.Year_Record.iloc[0]
            if len(str(val)) == 4:
                df['Year_Record'] = '12/31/%s' % str(val)
        df.to_csv(f'{outdir}/{endfn}',index=False)
        print(f'Created intermediate file {outdir}/{endfn}')
        return f'{outdir}/{endfn}',True 
    else:
        return '',False

# normalizing all hpms files to v9 
def convert_files(mylist,outdir='tmp_normalized'):
    if not os.path.exists(outdir): os.mkdir(outdir)
    newlist = []
    for fn in onlyfiles:
        newfn,bv = convert_fn_if_needed(fn)
        if bv: newlist.append(newfn)
    return newlist 


'''
this combines the errors from the fhwa file gdb to 
'''
def combine_errors(df,combined_file,dtype={'URBAN_CODE':str,'HPMS_SAMPLE_NO':str}):
    if not os.path.exists('error_full'):
        os.mkdir('error_full')
    if 'geometry' in df.columns:
        df.drop(['geometry'],axis=1,inplace=True)
    df['ValidationRule'] = df.ValidationRule.str.replace("-",'')
    df.rename(columns={'RouteId':'RouteID','BeginPoint':'BMP','EndPoint':'EMP'},inplace=True)
    num_errs = df['ValidationRule'].nunique()
    pos,ops = 0 ,[]
    rule_cols = []
    for name,tmpdf in df.groupby('ValidationRule'):
        rule = name+'_FHWA'
        outfile = f'error_full/{name}.csv'
        tmpdf[rule] = False 
        rule_cols.append(rule)
        ops.append(create_op(outfile,[rule],is_beg=pos==0,is_end=pos==(num_errs-1),outfilename='tot_errors.csv'))
        tmpdf[['RouteID','BMP','EMP',rule]].to_csv(outfile,index=False)
        pos+=1
    init_file('tot_errors.csv')
    with open('myerrors.json','w') as f:
        f.write(json.dumps({'operations':ops}))

    print('rule cols', rule_cols)

    os.system('lrsops overlay --operations myerrors.json')
    os.system(f'lrsops overlay -b {combined_file} -s tot_errors.csv -c {",".join(rule_cols)} -o tmpout.csv')
    
    df = pd.read_csv('tmpout.csv',dtype=dtype)
    df[rule_cols] = df[rule_cols].fillna(value=True)
    os.remove('tmpout.csv')
    os.remove('tot_errors.csv')
    shutil.rmtree('error_full')
    return df

mypath = '/Users/charlesbmurphy/Downloads/hpms-validation/tmp2'

# onlyfiles = [os.path.join(mypath,f) for f in os.listdir(mypath) if os.path.isfile(os.path.join(mypath, f)) and f.endswith('.csv')]
# print(combine_errors('/Users/charlesbmurphy/Downloads/Non-Conformances_2023a.gdb','all_submission_data.csv'))
# df = pd.read_csv('/Users/charlesbmurphy/Downloads/full_spatial_errors_table.csv')
# print(combine_errors(df,'all_submission_data.csv'))

# myfiles = convert_files(onlyfiles)
# df = combine_items(onlyfiles)
# df.to_excel('summary_combined.xlsx',index=False)
# print(len(myfiles))
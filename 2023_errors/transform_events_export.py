import pandas as pd 
import os 
import json
newlist = []


df1 = pd.read_csv('HPMS-RoadEvent.csv')
df2 = pd.read_csv('HPMS-RoadDesignation.csv')
df = pd.concat([df1,df2],ignore_index=True)
pos = 0
numfiles = df['DataItem'].nunique()
cols = 'StateId,RouteId,BeginPoint,EndPoint,DataItem,BeginDate,ValueNumeric,ValueText,ValueDate,Comments,ReportDate,EndDate,SectionLength,ProcessId,IsWarning'.split(',')
newdf = pd.DataFrame(columns=cols)
print('Created base file dataframe',newdf.columns)
newdf.to_csv('base_file.csv')


for ind,tmpdf in df.groupby('DataItem'):
    value_text_name = f'{ind}_VT'
    value_date_name = f'{ind}_VD'
    value_num_name = f'{ind}_VN'
    filename = f'intermediate/{ind}.csv'
    cols = [value_date_name,value_text_name,value_num_name]
    tmpdf.rename(columns={'ValueNumeric':value_num_name,'ValueText':value_text_name,'ValueDate':value_date_name},inplace=True)  

    tmpdf.to_csv(filename,index=False)
    if pos == 0:
        op = {
            "operation":"overlay",
            "base_file":'base_file.csv',
            "split_file":filename,
            "split_fields":cols,
            "out_file":"temp"
        }
    elif pos == numfiles-1:
        op = {
            "operation":"overlay",
            "base_file":"previous",
            "split_file":filename,
            "split_fields":cols,
            "out_file":"base_file.csv"
         }   
    else:
        op = {
            "operation":"overlay",
            "base_file":"previous",
            "split_file":filename,
            "split_fields":cols,
            "out_file":"temp"
        }
    newlist.append(op)
    pos+=1 
print(newlist)
myjson = {'operations':newlist}
with open('myfile.json','w') as f:
    f.write(json.dumps(myjson))
os.system('lrsops overlay --operations myfile.json')

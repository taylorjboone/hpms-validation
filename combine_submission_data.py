import pandas as pd
import os
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

gauth = GoogleAuth()
gauth.LocalWebserverAuth()

drive = GoogleDrive(gauth)


folder_name = 'HPMS 2023 Submission'
folder_id = '1DEujcYC9mNWbUJ1dIJuk5Zkb-DWj-CJF'

file_list = drive.ListFile({'q': f"'{folder_id}' in parents and trashed=false"}).GetList()


print('\nHPMS 2023 Submission')
for file in file_list:
    print(f'title: {file["title"]}, id: {file["id"]}')



abril_submission = drive.ListFile({'q': "'1p3-83rG2AndBFqa4JXBB6nLPb7zx8FgW' in parents and trashed=False"}).GetList()
abril_submission = [file for file in abril_submission if (file['mimeType'] == 'text/csv') & (file['title'][0:8] == 'DataItem')]

pavement_data = drive.ListFile({'q': "'1p3-83rG2AndBFqa4JXBB6nLPb7zx8FgW' in parents and trashed=False"}).GetList()
pavement_data = [file for file in pavement_data if (file['mimeType'] == 'text/csv') & (file['title'][0:8] == 'DataItem')]

june_submission = drive.ListFile({'q': "'1hWWgNLRPL609oXt_9iUmQgmPGzapwXUl' in parents and trashed=False"}).GetList()
june_submission = [file for file in june_submission if (file['mimeType'] == 'text/csv') & (file['title'][0:8] == 'DataItem')]

traffic_data = drive.ListFile({'q': "'1p3-83rG2AndBFqa4JXBB6nLPb7zx8FgW' in parents and trashed=False"}).GetList()
traffic_data = [file for file in traffic_data if (file['mimeType'] == 'text/csv') & (file['title'][0:8] == 'DataItem')]

fixed_files = drive.ListFile({'q': "'1HKojRkG3Da630GlvYvRevf2WV5hyVZPJ' in parents and trashed=False"}).GetList()
fixed_files = [file for file in fixed_files if (file['mimeType'] == 'text/csv') & (file['title'][0:8] == 'DataItem')]


dataset_list = [abril_submission, pavement_data, june_submission, traffic_data, fixed_files]
unique_items = []
for dataset in dataset_list:
    for i in dataset:
        if not i['title'] in unique_items:
            unique_items.append(i['title'])

print(f'Number of Data Items: {len(unique_items)}')


pos = 0
total = [i['title'] for i in fixed_files] + [i['title'] for i in june_submission if not i['title'] in [j['title'] for j in fixed_files]]
print('total')
[print(i) for i in sorted(total)]
newlist = []

if not os.path.exists('tmp'):
    os.mkdir('tmp')

for dataset in dataset_list:
    for file in dataset:
        print(file['title'])
        download = drive.CreateFile({'id':file['id']})
        download.GetContentFile('example.csv')
        df = pd.read_csv('example.csv', sep='|')

        if 'Data_Item' in df.columns.tolist():
            data_item = df['Data_Item'].iloc[0]
            df.rename(columns={'Value_Numeric': f'{data_item}', 'Value_Date':f'{data_item}_VALUE_DATE', 'Value_Text':f'{data_item}_VALUE_TEXT', 'Begin_Point':'BMP', 'End_Point':'EMP', 'Route_ID':'ROUTEID'})
        elif 'DataItem' in df.columns.tolist():
            data_item = df['DataItem'].iloc[0]
            df.rename(columns={'ValueNumeric': f'{data_item}', 'ValueDate':f'{data_item}_VALUE_DATE', 'ValueText':f'{data_item}_VALUE_TEXT', 'BeginPoint':'BMP', 'EndPoint':'EMP', 'RouteID':'ROUTEID'})
        else:
            print('COLUMN FORMAT ERROR')
        
        cols = [f'{data_item}', f'{data_item}_VALUE_DATE', f'{data_item}_VALUE_TEXT']
        filename = f'tmp/{data_item}.csv'
        df.to_csv(filename)


# for file in june_submission:
#     print(file['title'])
#     download = drive.CreateFile({'id':file['id']})
#     download.GetContentFile('example.csv')
#     df = pd.read_csv('example.csv', sep='|')


#     data_item = df['Data_Item'].iloc[0]
#     cols = [f'{data_item}', f'{data_item}_VALUE_DATE', f'{data_item}_VALUE_TEXT']
#     df.rename(columns={'Value_Numeric': f'{data_item}', 'Value_Date':f'{data_item}_VALUE_DATE', 'Value_Text':f'{data_item}_VALUE_TEXT', 'Begin_Point':'BMP', 'End_Point':'EMP', 'Route_ID':'ROUTEID'})
#     filename = f'tmp/{data_item}.csv'
#     df.to_csv(filename)


# for fixed_file in fixed_files:
#     download = drive.CreateFile({'id':fixed_file['id']})
#     download.GetContentFile('example.csv')
#     df = pd.read_csv('example.csv', sep='|')


#     data_item = df['Data_Item'].iloc[0]
#     cols = [f'{data_item}', f'{data_item}_VALUE_DATE', f'{data_item}_VALUE_TEXT']
#     df.rename(columns={'Value_Numeric': f'{data_item}', 'Value_Date':f'{data_item}_VALUE_DATE', 'Value_Text':f'{data_item}_VALUE_TEXT', 'Begin_Point':'BMP', 'End_Point':'EMP', 'Route_ID':'ROUTEID'})
#     filename = f'tmp/{data_item}.csv'
#     df.to_csv(filename)


#     if pos == 0:
#         op = {
#             "operation":"overlay",
#             "base_file":'example.csv',
#             "split_file":filename,
#             "split_fields":cols,
#             "out_file":"temp"
#         }
#     elif pos == numfiles-1:
#         op = {
#             "operation":"overlay",
#             "base_file":"previous",
#             "split_file":filename,
#             "split_fields":cols,
#             "out_file":"example.csv"
#          }   
#     else:
#         op = {
#             "operation":"overlay",
#             "base_file":"previous",
#             "split_file":filename,
#             "split_fields":cols,
#             "out_file":"temp"
#         }
#     newlist.append(op)
#     pos+=1
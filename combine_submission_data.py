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


# print('file list')
# [print(i) for i in file_list]


print('\nHPMS 2023 Submission')
for file in file_list:
    print(f'title: {file["title"]}, id: {file["id"]}')

fixed_files = drive.ListFile({'q': "'1HKojRkG3Da630GlvYvRevf2WV5hyVZPJ' in parents and trashed=False"}).GetList()
fixed_files_ids = [file['id'] for file in fixed_files if (file['mimeType'] == 'text/csv') & (file['title'][0:8] == 'DataItem')]
fixed_files_titles = [file['title'] for file in fixed_files if (file['mimeType'] == 'text/csv') & (file['title'][0:8] == 'DataItem')]

print('Fixed Files')
[print(i) for i in fixed_files_titles]

data_items = drive.ListFile({'q': "'1hWWgNLRPL609oXt_9iUmQgmPGzapwXUl' in parents and trashed=False"}).GetList()

print('****************')
[print(i['mimeType']) for i in data_items]
data_items_ids = [file['id'] for file in data_items if (file['mimeType'] == 'text/csv') & (file['title'][0:8] == 'DataItem')]
data_items_titles = [file['title'] for file in data_items if (file['mimeType'] == 'text/csv') & (file['title'][0:8] == 'DataItem')]

pos = 0
total = fixed_files_titles + [i for i in data_items_titles if not i in fixed_files_titles]
print('total')
[print(i) for i in sorted(total)]
newlist = []

if not os.path.exists('tmp'):
    os.mkdir('tmp')

# for fixed_file in fixed_files:
#     download = drive.CreateFile({'id':fixed_file['id']})
#     download.getContentFile('example.csv')
#     df = pd.read_csv('example.csv')
#     data_item = df['Data_Item'].iloc[0]
#     cols = [f'{data_item}', f'{data_item}_VALUE_DATE', f'{data_item}_VALUE_TEXT']
#     df.rename(columns={'Value_Numeric': f'{data_item}', 'Value_Date':f'{data_item}_VALUE_DATE', 'Value_Text':f'{data_item}_VALUE_TEXT', 'Begin_Point':'BMP', 'End_Point':'EMP', 'Route_ID':'ROUTEID'})
#     filename = f'tmp/{data_item}.csv'


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
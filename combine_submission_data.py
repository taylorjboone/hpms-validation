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

full_spatial_fixed = drive.ListFile({'q': "'1HKojRkG3Da630GlvYvRevf2WV5hyVZPJ' in parents and trashed=False"}).GetList()


# print('file list', file_list)


print('\nHPMS 2023 Submission')
for file in file_list:
    print(f'title: {file["title"]}, id: {file["id"]}')

print('\nFull Spatial - Fixed')
for file in full_spatial_fixed:
    print(f'title: {file["title"]}, id: {file["id"]}')
    




# hpms_folder = drive.CreateFile({'id': hpms['id']})
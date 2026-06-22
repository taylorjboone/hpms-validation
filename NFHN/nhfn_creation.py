import pandas as pd
import sys
import subprocess


def run_overlay(f1, f2, cols, prefix, output):
    """Runs the lrsops tool using subprocess for better error handling."""
    cmd = f'lrsops overlay -b {f1} -s {f2} -c "{cols}" --prefix "{prefix}" -o {output}'
    try:
        # check=True forces the script to throw an error if lrsops fails
        subprocess.run(cmd, shell=True, check=True)
    except subprocess.CalledProcessError as e:
        print(f"\n[CRITICAL ERROR]: lrsops overlay failed -> {e}")
        sys.exit(1)

dict = {'Rural':2,'Urban':3}
df_nhfn_primary = pd.read_csv(r'N:\GI_SECTION\HPMS\Submissions\2025_submission\NHFN\72_NHFN_primary_routes.csv',sep = '|')
df_nhfn_wv_urban_rural = pd.read_excel(r'N:\GI_SECTION\HPMS\Submissions\2025_submission\NHFN\Critical_Freight_Mileage_Combined.xlsx')
df_nhfn_wv_urban_rural = df_nhfn_wv_urban_rural[['ROUTEID','FROM_MP','TO_MP','NETWORK STATUS']]
df_nhfn_wv_urban_rural = df_nhfn_wv_urban_rural.rename(columns = {'ROUTEID':'RouteID','FROM_MP':'BeginPoint','TO_MP':'EndPoint','NETWORK STATUS':'ValueNumeric'})
df_nhfn_wv_urban_rural['ValueNumeric'] = df_nhfn_wv_urban_rural['ValueNumeric'].map(dict)
df_nhfn_wv_urban_rural['BeginDate'] = '01/01/2025'
df_nhfn_wv_urban_rural['ValueText'] = ''
df_nhfn_wv_urban_rural['ValueDate'] = ''
df_nhfn_wv_urban_rural['Comment'] = ''
df_nhfn_wv_urban_rural['DataItem'] = 'NHFN'
df_nhfn_wv_urban_rural['StateID'] = 54
df_nhfn_wv_urban_rural = df_nhfn_wv_urban_rural.dropna()
df_nhfn_primary['BeginDate'] ='01/01/2025'
df_nhfn_reordered = df_nhfn_wv_urban_rural[['BeginDate','StateID','RouteID','BeginPoint','EndPoint','ValueNumeric','DataItem','Comment','ValueText','ValueDate']]
print(df_nhfn_reordered.columns)

df_nhfn = pd.concat([df_nhfn_primary,df_nhfn_reordered])
df_nhfn.to_csv(r'72_NHFN.csv',sep = '|',index =False)


run_overlay('72_NHFN.csv',r'C:\Users\e104200\Documents\GitHub\hpms-validation\NFHN\source_data\route_status_061726.csv','49_ROUTE_STATUS','LRS_','./output_folder/temp.csv')

fil = pd.read_csv(r'./output_folder/temp.csv')

fil['supp_code'] = fil['RouteID'].str.slice(9,11)
fil['sign_system'] = fil['RouteID'].str.slice(2,3)

fil_check = fil[(~fil['supp_code'].isin(['24','25','26','27','28','51','99'])) & (fil['LRS_49_ROUTE_STATUS'] == 5) & (fil['ValueNumeric'].notna())]
fil_check['StateID'] = 54
fil_check['ValueNumeric'] = fil_check['ValueNumeric'].astype('int')
# print(fil_check['sign_system'].unique())
# print(fil_check['supp_code'].unique())
# print(fil_check['RouteID'].nunique())

fil_final = fil_check[['RouteID','BMP','EMP','BeginDate','StateID','ValueNumeric','DataItem','Comment','ValueText','ValueDate']]
print(fil_final)
fil_final.to_csv(r'./output_folder/DataItem_72_NHFN.csv',index = False,sep = '|')
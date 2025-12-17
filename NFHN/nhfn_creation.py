import pandas as pd


dict = {'Rural':2,'Urban':3}
df_nhfn_primary = pd.read_csv(r'N:\GI_SECTION\HPMS\Submissions\2025_submission\NHFN\72_NHFN_primary_routes.csv',sep = '|')
df_nhfn_wv_urban_rural = pd.read_excel(r'N:\GI_SECTION\HPMS\Submissions\2025_submission\NHFN\Critical_Freight_Mileage_Combined.xlsx')
df_nhfn_wv_urban_rural = df_nhfn_wv_urban_rural[['ROUTEID','FROM_MP','TO_MP','NETWORK STATUS']]
df_nhfn_wv_urban_rural = df_nhfn_wv_urban_rural.rename(columns = {'ROUTEID':'RouteID','FROM_MP':'BeginPoint','TO_MP':'EndPoint','NETWORK STATUS':'ValueNumeric'})
df_nhfn_wv_urban_rural['ValueNumeric'] = df_nhfn_wv_urban_rural['ValueNumeric'].map(dict)
df_nhfn_wv_urban_rural['BeginDate'] = '01/01/2024'
df_nhfn_wv_urban_rural['ValueText'] = ''
df_nhfn_wv_urban_rural['ValueDate'] = ''
df_nhfn_wv_urban_rural['Comment'] = ''
df_nhfn_wv_urban_rural['DataItem'] = 'NHFN'
df_nhfn_wv_urban_rural['StateID'] = 54
df_nhfn_wv_urban_rural = df_nhfn_wv_urban_rural.dropna()
df_nhfn_primary['BeginDate'] ='01/01/2024'
df_nhfn_reordered = df_nhfn_wv_urban_rural[['BeginDate','StateID','RouteID','BeginPoint','EndPoint','ValueNumeric','DataItem','Comment','ValueText','ValueDate']]
print(df_nhfn_reordered.columns)

df_nhfn = pd.concat([df_nhfn_primary,df_nhfn_reordered])

df_nhfn.to_csv(r'72_NHFN.csv',sep = '|',index =False)
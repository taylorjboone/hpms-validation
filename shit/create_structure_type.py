import pandas as pd
df = pd.read_excel("aw_conflated_bars_stripped_mike_filtered.xlsx")

df = df[df['IsDupBars']==False]
# df = df[df['RouteID'].str[2]=='1']
df = df[df['BARS'].str[2]=='A']
df_hpms = df[{"RouteID", "BMP", "EMP", "BARS"}]
df_hpms = df_hpms.rename(columns = {'BMP':'BeginPoint','EMP':'EndPoint','BARS':'ValueText'})
df_hpms['BeginDate'] = '01/01/2022'
df_hpms['ValueNumeric'] = 1
df_hpms['DataItem'] = 'STRUCTURE_TYPE'
df_hpms['StateID'] = 54
df_hpms['Comments'] = ''
df_hpms['ValueDate'] ='' 
df_rounded = df_hpms.round({"BeginPoint": 3, "EndPoint": 3})
df_rounded = df_rounded.drop_duplicates(['ValueText'])

df_rounded.to_csv("dataitem4_structure_type_round.csv", sep="|", index=False)
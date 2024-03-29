import pandas as pd

df = pd.read_csv('district/compare_surfaceType.csv')
df['SURF_TYPE'].fillna('',inplace=True)
df['pav_surf_type'] = df['SURF_TYPE'].map(pave_surf_dict_24)

df['70_SURFACE_TYPE'].fillna(0, inplace=True)
df['lrs_surf_type'] = df['70_SURFACE_TYPE'].map(lrs_surf_dict)


df['diff'] = np.where(df['lrs_surf_type'] != df['pav_surf_type'], 'Yes', 'No') 

print(df)

print(len(df[(df['diff'] == 'No') & (df['pav_surf_type'] != 0)]), 'same')
print(len(df[(df['diff'] == 'Yes') & (df['pav_surf_type'] != 0)]), 'diff')

print(len(df[(df['diff'] == 'Yes') & (df['SURF_TYPE'] == 'OTH')]), 'OTH to 11')

print(df[(df['diff'] == 'Yes') & (df['pav_surf_type'] != 0)])
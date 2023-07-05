import pandas as pd
df = pd.read_csv('all_submission_data.csv')

df_full_spatial_test = df.iloc[:1000]
columnNamesAdd = []


for i in range(1,100):
    if i < 10:
        columnNamesAdd.append("SJF0" + str(i) + "_Control")
    else:
        columnNamesAdd.append("SJF" + str(i) + "_Control")

df_full_spatial_test[columnNamesAdd] = ""

columnNames = df_full_spatial_test.columns.values

for i, col in enumerate(columnNames[list(columnNames).index('SJF01_Control'):]):
    for j in range(i*10, i*10+10):
        if j < (i*10 + i*10+10) / 2:
            df_full_spatial_test.at[j, col] = "Pass"
        else:
            df_full_spatial_test.at[j, col] = "Fail"

# print(df_full_spatial_test[['SJF01_Control', 'SJF02_Control']].iloc[:20])

# for col in columnNames[list(columnNames).index('SJF01_Control'):]:
#     tmp_df = df_full_spatial_test[df_full_spatial_test[col]=='Pass']


passDF = df_full_spatial_test.iloc[:,[list(df_full_spatial_test.columns.values).index('SJF01_Control')]:].copy()
passDF = passDF[[passDF.columns.values] == "Pass"]

print(passDF)


# df_full_spatial_test.to_csv('full_spatial_test.csv',index=False)




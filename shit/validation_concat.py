import pandas as pd
from os import listdir
from os.path import isfile, join
import os

mypath = 'modified_june_submission_data'

onlyfiles = [os.path.join(mypath,f) for f in listdir(mypath) if isfile(join(mypath, f)) if f.endswith('csv')]

filter_list = ['modified_june_submission_data\\DataItem1_F_System (2)_test.csv', 'modified_june_submission_data\\HPMS_2021_WV_DataItem64_NHS_test.csv', 'modified_june_submission_data\\DataItem65-STRAHNET_Type_test.csv', 'modified_june_submission_data\\DataItem66-Truck_test.csv', 'modified_june_submission_data\\2021 Travel Time Metrics Final_test.csv', 'modified_june_submission_data\\DataItem_CountySummary_test.csv', 'modified_june_submission_data\\Estimate2021 (1)_test.csv', 'modified_june_submission_data\\Metadata_test.csv', 'modified_june_submission_data\\DataItem64-NHS_test.csv', 'modified_june_submission_data\\Non_Federal_Aid_Summaries_test.csv', 'modified_june_submission_data\\Statewide_Summary_test.csv', 'modified_june_submission_data\\Urban_Summary_test.csv', 'modified_june_submission_data\\VMT_Summaries_test.csv']

road_events = pd.DataFrame()

for file in onlyfiles:
    if not file in filter_list:
        temp_df = pd.read_csv(file, sep='|')
        if 'BeginDate.1' in temp_df.columns.tolist():
            print(file)
        road_events = pd.concat([road_events, temp_df], ignore_index=True)

road_events.to_csv('temp-validations.csv', sep='|', index=False)
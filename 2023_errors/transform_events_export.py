import pandas as pd
import os

cwd = os.getcwd()
events_fp = os.path.join(cwd, 'WV_EventsJune7.csv')
designations_fp = os.path.join(cwd, 'HPMS-RoadDesignation.csv')

designations = pd.read_csv(designations_fp)
events = pd.read_csv(events_fp)

designations = designations.pivot_table(values='ValueNumeric', index=['RouteId', 'BeginPoint', 'EndPoint'], columns='DataItem', aggfunc='first').dropna(how='all').reset_index().rename(columns={'RouteId':'ROUTEID', 'BeginPoint':'BMP', 'EndPoint':'EMP'})
events = events.pivot_table(values='ValueNumeric', index=['RouteId', 'BeginPoint', 'EndPoint'], columns='DataItem', aggfunc='first').dropna(how='all').reset_index().rename(columns={'RouteId':'ROUTEID', 'BeginPoint':'BMP', 'EndPoint':'EMP'})

designations.to_csv('designations_tmp.csv', index=False)
events.to_csv('events_tmp.csv', index=False)

os.system('lrsops overlay -b events_tmp.csv -s designations_tmp.csv -c "F_SYSTEM" -o out.csv')#,NHS,NN,STRAHNET_TYPE


master = pd.read_csv('out.csv')
master[master['FACILITY_TYPE'].astype('string').isin(['1','2']) & master['F_SYSTEM'].astype('string').isin(['1','2','3','4','5']) | (master['F_SYSTEM'].astype('string').str == '6') | (master['NHS'])]
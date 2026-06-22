import pandas as pd

# lat_long_file = pd.read_excel(r'./input/test_bridge_con.xlsx')

# temp= lat_long_file[['BridgeNumber','Latitude','Longitude']]

# coord_dict = temp.set_index('BridgeNumber').T.to_dict('list')


# base = pd.read_excel(r'./input/FullSubmission Excel Conversion 2026 HPMS Submission_unmodified.xlsx',sheet_name='2026-3-13 JSON (7236)')

# print(base)

# base['Latitude'] = base.BarsID.map(lambda x :coord_dict[x][0])
# base['Longitude'] = base.BarsID.map(lambda x : coord_dict[x][1])

# print(base[['Latitude','Longitude']])


zac_layer = pd.read_excel(r'./input/zac_layer_bridge_lrs.xlsx')
zac_layer['BARSid'] = zac_layer['BARSid'].str[-6:]
print(zac_layer['BARSid'])
ztemp = zac_layer[['BARSid','MP']]

bridge_dict = ztemp.to_dict
ztemp_rid = zac_layer[['BARSid','RouteID']]
ztemp_rid['BARSid'] = ztemp_rid['BARSid'].str[-6:]
ztemp_rid_dict = ztemp_rid.to_dict()
bridge_dict = ztemp.to_dict()

print(ztemp_rid_dict)
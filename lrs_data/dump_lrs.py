import os
import json
import pandas as pd
import datetime
import shutil

today = datetime.date.today().strftime('%m-%d-%y')

layers_dict = {
    "2":["AADT","FUTURE_AADT"],
    "3":["ACCESS_CONTROL"],
    "4":["ALT_ROUTE_NAME"],
    "5":["AT_GRADE_OTHER"],
    "6":["AVG_LANE_WIDTH_FT"],
    "12":["COUNTY"],
    "14":["CRACKING_PERCENT"],
    "16":["CURVE_CLASS"],
    "17":["DES_TRUCK_ROUTE"],
    "18":["DISTRICT"],
    "20":["FACILITY"],
    "21":["FAULTING"],
    "25":["GRADE_CLASS"],
    "29":["HPMS_SAMPLE_NO", "FIELD_ESTABLISH_DATE"],
    "30":["IRI_VALUE"],
    "32":["RH_TO_DATE","RH_FROM_DATE","FIELD_ESTABLISH_DATE"],
    "34":["HPMS_MEDIAN_BARRIER_TYPE","MEDIAN_WIDTH_FT"],
    "35":["NAT_FUNCTIONAL_CLASS"],
    "36":["NHS"],
    "37":["NUMBER_SIGNALS"],
    "39":["OWNERSHIP"],
    "41":["PCT_GREEN_TIME"],
    "42":["PCT_PASS_SIGHT"],
    "43":["PEAK_LANES","COUNTER_PEAK_LANES"],
    "44":["PEAK_PARKING"],
    "45":["PSR"],
    "52":["RUTTING"],
    "56":["SHOULDER_TYPE_RT"],
    "57":["SHOULDER_WIDTH_LFT_FT"],
    "58":["SHOULDER_WIDTH_RT_FT"],
    "59":["SIGNAL_TYPE"],
    "63":["SPEED_LIMIT_MPH"],
    "66":["STOP_SIGNS"],
    "70":["SURFACE_TYPE"],
    "71":["TERRAIN_TYPE"],
    "74":["NUM_THROUGH_LANES"],
    "75":["TOLL_CHARGED"],
    "76":["TOLL_TYPE"],
    "77":["AADT_SINGLE","AADT_COMBINATION","PCT_PEAK_SINGLE","PCT_PEAK_COMBINATION","K_FACTOR","DIR_FACTOR"],
    "80":["TURN_LANES_LFT"],
    "81":["TURN_LANES_RT"],
    "83":["URBAN_CODE"],
    "84":["WIDENING_OBSTACLE"],
    "85":["WIDENING_POTENTIAL"],
    "97":["ORIG_SURVEY_DIRECTION"],
    "115":["STRAHNET"]
    }

if not os.path.exists('temp'):
    os.mkdir('temp')
json_object = json.dumps(layers_dict)
with open("temp/carry.json", 'w') as outfile:
    outfile.write(json_object)

layers = ','.join(layers_dict.keys())

command = f'lrsops dumpsplit --outputdir temp -l {layers} --carry_json temp/carry.json -o temp/out.csv'
os.system(command)

df = pd.read_csv(r'temp/out.csv')

cols = [i for i in df.columns.tolist() if 'OBJECTID' in i]
df.drop(columns=cols, inplace=True)
df.to_csv(f'lrs_dump_{today}.csv', index=False)

shutil.rmtree('temp')
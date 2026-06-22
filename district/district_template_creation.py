import pandas as pd
import os
from os import listdir
from os.path import isfile, join


sign_system_dict = {'1':'I','2':'US','3':'WV','4':'CO','6':'SPFR','7':'FA','8':'HA'}

county_name_dict = {
  "01": "Barbour",
  "02": "Berkeley",
  "03": "Boone",
  "04": "Braxton",
  "05": "Brooke",
  "06": "Cabell",
  "07": "Calhoun",
  "08": "Clay",
  "09": "Doddridge",
  "10": "Fayette",
  "11": "Gilmer",
  "12": "Grant",
  "13": "Greenbrier",
  "14": "Hampshire",
  "15": "Hancock",
  "16": "Hardy",
  "17": "Harrison",
  "18": "Jackson",
  "19": "Jefferson",
  "20": "Kanawha",
  "21": "Lewis",
  "22": "Lincoln",
  "23": "Logan",
  "24": "McDowell",
  "25": "Marion",
  "26": "Marshall",
  "27": "Mason",
  "28": "Mercer",
  "29": "Mineral",
  "30": "Mingo",
  "31": "Monongalia",
  "32": "Monroe",
  "33": "Morgan",
  "34": "Nicholas",
  "35": "Ohio",
  "36": "Pendleton",
  "37": "Pleasants",
  "38": "Pocahontas",
  "39": "Preston",
  "40": "Putnam",
  "41": "Raleigh",
  "42": "Randolph",
  "43": "Ritchie",
  "44": "Roane",
  "45": "Summers",
  "46": "Taylor",
  "47": "Tucker",
  "48": "Tyler",
  "49": "Upshur",
  "50": "Wayne",
  "51": "Webster",
  "52": "Wetzel",
  "53": "Wirt",
  "54": "Wood",
  "55": "Wyoming"
}


county_dict = {'01': 7, '02': 5, '03': 1, '04': 7, '05': 6, '06': 2, '07': 3, '08': 1, '09': 4, 
               '10': 9, '11': 7, '12': 5, '13': 9, '14': 5, '15': 6, '16': 5, '17': 4, '18': 3, '19': 5, 
               '20': 1, '21': 7, '22': 2, '23': 2, '24': 10, '25': 4, '26': 6, '27': 1, '28': 10, '29': 5, 
               '30': 2, '31': 4, '32': 9, '33': 5, '34': 9, '35': 6, '36': 8, '37': 3, '38': 8, '39': 4, 
               '40': 1, '41': 10, '42': 8, '43': 3, '44': 3, '45': 9, '46': 4, '47': 8, '48': 6, '49': 7, 
               '50': 2, '51': 7, '52': 6, '53': 3, '54': 3, '55': 10}

pave_dict ={
    1.1 :'Unimproved',
    1.2:'Soil Surface Road - Dirt',
    1.3 :'Gravel or Stone',
    2.1 :'Asphaltic Concrete (virgin)',
    2.2 :'Bituminous (chip seal)',
    3:'JPCP Jointed Plain Concrete Pavement',
    4:'JRCP 2013 Jointed Reinforced Concrete Pavement',
    5:'CRCP Continuously Reinforced Concrete Pavement',
    6:'Asphalt-Concrete (AC) Overlay over Existing AC Pavement',
    7:'AC Overlay over Existing Joined Concrete Pavement',
    8:'AC (Bituminous Overlay over Existing CRCP)',
    9:"Unbonded Jointed Concrete Overlay on PCC Pavement",
    10:'Bonded PCC Overlay on PCC Pavement',
    11:'Other',
    99:'Primitive'
}

def overlay(f1, f2, cols,prefix,output):
    os.system(f'lrsops overlay -b {f1} -s {f2}  -c "{cols}" --prefix "{prefix}" -o {output}')


# uses the overlay of hpms_samples and surface type in the RIL_LRS enviroment on the https://gisdev.transportation.wv.gov/overlay/ website
df_overlay = pd.read_csv(r'./source_data/district_SurfaceType.csv') #provided by Margaret Smith for the 2026 submission year
df_overlay = df_overlay[df_overlay['Sample_ID'].notna()]
df_overlay['LRS_SURFACE_TYPE'] = df_overlay['LRS_SURFACE_TYPE'].map(lambda x: pave_dict.get(x))

df_districts = df_overlay.copy()

df_districts['county'] = df_districts['RouteID'].str.slice(0,2)

df_districts['districts'] = df_districts.RouteID.str[:2].map(lambda x:county_dict.get(x,''))

df_districts['county'] = df_districts['county'].map(lambda x :county_name_dict.get(x,''))

df_districts['route_number'] = df_districts['RouteID'].str.slice(3,7)
df_districts['sub_route'] = df_districts['RouteID'].str.slice(7,9)
df_districts['sign_system'] = df_districts['RouteID'].str.slice(2,3)
df_districts['Route #'] = df_districts['sign_system'].map(lambda x: sign_system_dict.get(x)) + ' '+ df_districts['route_number'] + '/'+ df_districts['sub_route']

df_districts['BeginDate'] = '01/01/2025'
df_districts['Base Thickness'] = ''
df_districts['Base Type'] = ''
df_districts['Year Last Improvement'] = ''
df_districts['Year Last Construction'] = ''
df_districts['Thickness Flexible'] = ''
df_districts['Thickness Rigid'] = ''
df_districts['Last Overlay Thickness'] = ''
df_districts = df_districts.rename(columns={'LRS_SURFACE_TYPE':'Surface_Type','Sample_ID':'SampleId'})
df_final = df_districts.loc[:,['BeginDate','RouteID','SampleId','BMP','EMP','county','districts','Route #','Surface_Type','Base Thickness','Base Type','Year Last Improvement','Year Last Construction','Thickness Flexible','Thickness Rigid','Last Overlay Thickness']]
#sends one combined template as a csv
df_final.to_csv('temp_final.csv',index=False)

temp = pd.DataFrame(columns={'RouteID':'','BMP':'','EMP':''})
a = 1
temp.to_csv('./district_input/base_thick/combined/temp.csv')
mypath_base_thick = f'./district_input/base_thick'
onlyfiles_base_thick = [os.path.join(mypath_base_thick,f) for f in listdir(mypath_base_thick) if isfile(join(mypath_base_thick, f))]
for file in onlyfiles_base_thick:
    overlay('./district_input/base_thick/combined/temp.csv', file, 'ValueNumeric',f'base_thick_{a}_', './district_input/base_thick/combined/temp.csv')
    a += 1

temp.to_csv('./district_input/base_type/combined/temp.csv')
mypath_base_type = f'./district_input/base_type'
onlyfiles_base_type = [os.path.join(mypath_base_type,f) for f in listdir(mypath_base_type) if isfile(join(mypath_base_type, f))]
for file in onlyfiles_base_type:
    overlay('./district_input/base_type/combined/temp.csv', file, 'ValueNumeric',f'base_type_{a}_', './district_input/base_type/combined/temp.csv')
    a += 1

temp.to_csv('./district_input/last_overlay_thickness/combined/temp.csv')
mypath_last_overlay_thickness = f'./district_input/last_overlay_thickness'
onlyfiles_last_overlay_thickness = [os.path.join(mypath_last_overlay_thickness,f) for f in listdir(mypath_last_overlay_thickness) if isfile(join(mypath_last_overlay_thickness, f))]
for file in onlyfiles_last_overlay_thickness:
    overlay('./district_input/last_overlay_thickness/combined/temp.csv', file, 'ValueNumeric',f'last_overlay_thickness_{a}_', './district_input/last_overlay_thickness/combined/temp.csv')
    a += 1

temp.to_csv('./district_input/thickness_flexible/combined/temp.csv')
mypath_thickness_flexible = f'./district_input/thickness_flexible'
onlyfiles_thickness_flexible = [os.path.join(mypath_thickness_flexible,f) for f in listdir(mypath_thickness_flexible) if isfile(join(mypath_thickness_flexible, f))]
for file in onlyfiles_thickness_flexible:
    overlay('./district_input/thickness_flexible/combined/temp.csv', file, 'ValueNumeric',f'thickness_flexible_{a}_', './district_input/thickness_flexible/combined/temp.csv')
    a += 1

temp.to_csv('./district_input/thickness_rigid/combined/temp.csv')
mypath_thickness_rigid = f'./district_input/thickness_rigid'
onlyfiles_thickness_rigid = [os.path.join(mypath_thickness_rigid,f) for f in listdir(mypath_thickness_rigid) if isfile(join(mypath_thickness_rigid, f))]
for file in onlyfiles_thickness_rigid:
    overlay('./district_input/thickness_rigid/combined/temp.csv', file, 'ValueNumeric',f'thickness_rigid_{a}_', './district_input/thickness_rigid/combined/temp.csv')
    a += 1

temp.to_csv('./district_input/year_last_constructed/combined/temp.csv')
mypath_year_last_con = f'./district_input/year_last_constructed'
onlyfiles_year_last_con = [os.path.join(mypath_year_last_con,f) for f in listdir(mypath_year_last_con) if isfile(join(mypath_year_last_con, f))]
for file in onlyfiles_year_last_con:
    overlay('./district_input/year_last_constructed/combined/temp.csv', file, 'ValueDate',f'year_last_constructed_{a}_', './district_input/year_last_constructed/combined/temp.csv')
    a += 1

temp.to_csv('./district_input/year_last_improve/combined/temp.csv')
mypath_year_last_improve = f'./district_input/year_last_improve'
onlyfiles_year_last_improve = [os.path.join(mypath_year_last_improve,f) for f in listdir(mypath_year_last_improve) if isfile(join(mypath_year_last_improve, f))]
for file in onlyfiles_base_thick:
    overlay('./district_input/year_last_improve/combined/temp.csv', file, 'ValueDate',f'year_last_improve_{a}_', './district_input/year_last_improve/combined/temp.csv')
    a += 1




overlay('temp_final.csv','./district_input/base_thick/combined/temp.csv','base_thick_1_ValueNumeric,base_thick_2_ValueNumeric,base_thick_3_ValueNumeric','bthick_','temp_final.csv')
overlay('temp_final.csv','./district_input/base_type/combined/temp.csv','base_type_4_ValueNumeric,base_type_5_ValueNumeric,base_type_6_ValueNumeric','btype_','temp_final.csv')
overlay('temp_final.csv','./district_input/last_overlay_thickness/combined/temp.csv','last_overlay_thickness_7_ValueNumeric,last_overlay_thickness_8_ValueNumeric,last_overlay_thickness_9_ValueNumeric','lot_','temp_final.csv')
overlay('temp_final.csv','./district_input/thickness_flexible/combined/temp.csv','thickness_flexible_10_ValueNumeric,thickness_flexible_11_ValueNumeric,thickness_flexible_12_ValueNumeric','tflex_','temp_final.csv')
overlay('temp_final.csv','./district_input/thickness_rigid/combined/temp.csv','thickness_rigid_13_ValueNumeric,thickness_rigid_14_ValueNumeric,thickness_rigid_15_ValueNumeric','trigid_','temp_final.csv')
overlay('temp_final.csv','./district_input/year_last_constructed/combined/temp.csv','year_last_constructed_16_ValueDate,year_last_constructed_17_ValueDate,year_last_constructed_18_ValueDate','ylc_','temp_final.csv')
overlay('temp_final.csv','./district_input/year_last_improve/combined/temp.csv','year_last_improve_19_ValueDate,year_last_improve_20_ValueDate,year_last_improve_21_ValueDate','ylc_','temp_final.csv')
overlay('temp_final.csv','./source_data/maint_orgs.csv','32_SERVICE_ORG','MAINT_','temp_final.csv')


df_combined = pd.read_csv('temp_final.csv')

df_combined_fil = df_combined[df_combined['Surface_Type'].notna()]
df_combined_fil['Base Thickness'] = df_combined_fil['bthick_base_thick_1_ValueNumeric'].combine_first(df_combined_fil['bthick_base_thick_2_ValueNumeric']).combine_first(df_combined_fil['bthick_base_thick_3_ValueNumeric'])
df_combined_fil['Base Type'] = df_combined_fil['btype_base_type_4_ValueNumeric'].combine_first(df_combined_fil['btype_base_type_5_ValueNumeric']).combine_first(df_combined_fil['btype_base_type_6_ValueNumeric'])
df_combined_fil['Year Last Improvement'] = df_combined_fil['ylc_year_last_improve_19_ValueDate'].combine_first(df_combined_fil['ylc_year_last_improve_20_ValueDate']).combine_first(df_combined_fil['ylc_year_last_improve_21_ValueDate'])
df_combined_fil['Year Last Construction'] = df_combined_fil['ylc_year_last_constructed_16_ValueDate'].combine_first(df_combined_fil['ylc_year_last_constructed_17_ValueDate']).combine_first(df_combined_fil['ylc_year_last_constructed_18_ValueDate'])
df_combined_fil['Thickness Flexible'] = df_combined_fil['tflex_thickness_flexible_10_ValueNumeric'].combine_first(df_combined_fil['tflex_thickness_flexible_11_ValueNumeric']).combine_first(df_combined_fil['tflex_thickness_flexible_12_ValueNumeric'])
df_combined_fil['Thickness Rigid'] = df_combined_fil['trigid_thickness_rigid_13_ValueNumeric'].combine_first(df_combined_fil['trigid_thickness_rigid_14_ValueNumeric']).combine_first(df_combined_fil['trigid_thickness_rigid_15_ValueNumeric'])
df_combined_fil['Last Overlay Thickness'] = df_combined_fil['lot_last_overlay_thickness_7_ValueNumeric'].combine_first(df_combined_fil['lot_last_overlay_thickness_8_ValueNumeric']).combine_first(df_combined_fil['lot_last_overlay_thickness_9_ValueNumeric'])

df_comp = df_combined_fil[['RouteID','BMP','EMP','BeginDate','SampleId','county','districts','MAINT_32_SERVICE_ORG','Route #','Surface_Type','Base Thickness','Base Type','Year Last Improvement','Year Last Construction','Thickness Flexible','Thickness Rigid','Last Overlay Thickness']]

#separates the combined template into individual districts before sending to excel
for a in df_comp['districts'].unique():
    a = int(a)
    x= df_comp[df_comp['districts']==a]
    x.to_excel('./output/district_{}_template_test.xlsx'.format(a),index=False)

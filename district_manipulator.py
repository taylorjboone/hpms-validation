from fileinput import filename
from matplotlib.pyplot import axis
import pandas as pd
import os 
from os import listdir
from os.path import isfile, join
import numpy as np

def mapme(x):
    try:
        tmp = int(float(x))
    except:
        tmp=x
    return tmp

def mapme_float(x):
    try:
        tmp = float(x)
    except:
        x
    return tmp


mypath_sample = r'C:\Users\e104200\Downloads\HPMS_samples_v9.csv'
mypath = 'C:\\Users\\e104200\\Documents\\PythonTest\\24_district_data'
onlyfiles = [os.path.join(mypath,f) for f in listdir(mypath) if isfile(join(mypath, f))]
df = pd.DataFrame(columns=['YearRecord','StateCode','District','SectionLength','SampleID','Comments','MBISurfaceType','SurfaceType','RouteID','BeginPoint','EndPoint','YearLastImprovement','YearLastConstructed','LastOverlayThickness','ThicknessRigid','ThicknessFlexible','BaseType','BaseThickness']
)
pd.read_csv(mypath_sample)
df = []
for a in onlyfiles:
    # print('File',a)
    df2= pd.read_excel(a)
    df.append(df2)
    # print(df2.columns)
    
    
df = pd.concat(df,ignore_index=True)

print(len(df),"SHIT")

# df.to_csv('C:\\Users\\e104200\\Documents\\PythonTest\\master_data.csv',sep='|',index=False)
tmp_df = pd.DataFrame(columns = ['RouteID','BeginDate','StateID','BMP','EMP','ValueNumeric','ValueDate','ValueText','Comments'])
column_list = ['Year Last Improvement','Year Last Constructed','Last Overlay Thickness','Thickness Rigid','Thickness Flex','Base Type','Base Thickness']
for i in column_list:
    print('Pavement type ------->',i)
    di_df = df[['RouteID','BMP','EMP',i,'Comments']]

    #start of modifying district data creation
    di_df = di_df.dropna(subset=['BMP','EMP',i])
    di_df = di_df[di_df[i] != '*']
    if i=='Base Type':
        tmp_base = di_df.copy(deep=True)
        #base type manipulation
        # print('initial base type',tmp_base[i].unique())
        # tmp_base[i] = tmp_base[i].astype(str).map(lambda x: x.strip('.0'))
        # print('base type values',tmp_base[i].unique())
        tmp_base[i] = tmp_base[i].replace(['Asphalt','Concrete','Base II','Base 2',' ','Unknown','Superpave TY 25','Superpave'],[3,6,2,2,25,26,27,28])
        # print('after repalcing',tmp_base[i].unique())
        tmp_base[i] = tmp_base[i].astype(int)

        # print('after turning into int ',tmp_base[i].unique())
        tmp_base.rename(columns={i:'ValueNumeric'},inplace=True)
        tmp_base['ValueDate'] = ''
        tmp_base['StateID'] = '54'
        tmp_base['BeginDate'] = '01/01/2023'
        tmp_base['ValueText'] = ''
        tmp_base['DataItem'] = 'BASE_TYPE'
        tmp_base = tmp_base[tmp_base['ValueNumeric']< 25]
        # print('base type',tmp_base)
        tmp_base.to_csv(f'{i}.csv',sep = '|',index=False)
    
    elif i=='Base Thickness':
        tmp_basethick = di_df.copy(deep=True)
        #Base thickness manipulation
        # print('initial base thickness',tmp_basethick[i].unique())
        tmp_basethick[i] = tmp_basethick[i].loc[(tmp_basethick[i] !='Unknown') & (tmp_basethick[i]!=0)]
        tmp_basethick[i] = tmp_basethick[i].astype(str).map(lambda x: x.rstrip(' "'))
        # print('After stripping left and right',tmp_basethick[i].unique())
        tmp_basethick = tmp_basethick.dropna(subset=[i])
        # print('after dropping na',tmp_basethick[i].unique())
        tmp_basethick.rename(columns={i:'ValueNumeric'},inplace = True)
        # print('columns of basethick',tmp_basethick.columns)
        tmp_basethick['ValueDate'] = ''
        tmp_basethick['StateID'] = '54'
        tmp_basethick['BeginDate'] = '01/01/2023'
        tmp_basethick['ValueText'] = ''
        tmp_basethick['DataItem'] = 'BASE_THICKNESS'
        # tmp_basethick = tmp_basethick.dropna(subset=['ValueNumeric'])
        tmp_basethick = tmp_basethick[tmp_basethick['ValueNumeric']!='nan']
        tmp_basethick = tmp_basethick[tmp_basethick['ValueNumeric']!='> 4']
        tmp_basethick =tmp_basethick[tmp_basethick['ValueNumeric']!='']
        # print('after droppping na,second time',tmp_basethick['ValueNumeric'].unique())
        tmp_basethick['ValueNumeric'] = tmp_basethick['ValueNumeric'].map(mapme)

        # print('After turning into int',tmp_basethick['ValueNumeric'].unique())
        tmp_basethick = tmp_basethick[tmp_basethick['ValueNumeric']!=0]
        # print('basethick df',tmp_basethick)
        tmp_basethick.to_csv(f'{i}.csv',sep='|',index=False)
    
    elif i=='Thickness Flex':
        tmp_thickflex = di_df.copy(deep=True)
        #thickness flexible manipulation
        # print(tmp_thickflex[i].unique())
        # tmp_thickflex[i] = tmp_thickflex[i].astype(str).map(lambda x: x.rstrip('"'))
        tmp_thickflex[i] = tmp_thickflex[i].astype(str).map(lambda x: x.replace('"', ''))
        # print('after strip',tmp_thickflex[i].unique())
        tmp_thickflex[i] = tmp_thickflex[i].replace(['',' '],np.nan)
        tmp_thickflex = tmp_thickflex.dropna(subset=[i])
        tmp_thickflex = tmp_thickflex[tmp_thickflex[i].astype('string') != '0']
        
        tmp_thickflex.rename(columns={i:'ValueNumeric'},inplace=True)
        tmp_thickflex['ValueDate'] = ''
        tmp_thickflex['StateID'] = '54'
        tmp_thickflex['BeginDate'] = '01/01/2023'
        tmp_thickflex['ValueText'] = ''
        tmp_thickflex['DataItem'] = 'THICKNESS_FLEXIBLE'
        # print('Thickness flexible',tmp_thickflex)
        # tmp_thickflex[i] = tmp_thickflex['ValueNumeric'].astype(float)
        tmp_thickflex['ValueNumeric'] = tmp_thickflex['ValueNumeric'].map(mapme)
        tmp_thickflex = tmp_thickflex.drop_duplicates(subset=['RouteID','BMP','EMP'])
        tmp_thickflex = tmp_thickflex[tmp_thickflex['ValueNumeric']!=0]
        # print('after float change?',tmp_thickflex['ValueNumeric'].unique())
        # tmp_thickflex[i] = tmp_thickflex['ValueNumeric'].astype(int)
        tmp_thickflex.to_csv(f'{i}.csv',sep='|',index=False)
    
    elif i=='Thickness Rigid':
        tmp_thickrig = di_df.copy(deep=True)
        tmp_thickrig[i] = tmp_thickrig[i].loc[(tmp_thickrig[i]!=0)]
        #thickness Rigid manipulation
        print('initial thick rigi',tmp_thickrig[i].unique())
        tmp_thickrig[i] = tmp_thickrig[i].astype(str).map(mapme)
        print('after mapping thick rig',tmp_thickrig[i].unique())
        tmp_thickrig[i] = tmp_thickrig[i].replace(' ',np.nan)
        print('after replacing thick rigi',tmp_thickrig[i].unique())
        tmp_thickrig = tmp_thickrig.dropna(subset=[i])
        tmp_thickrig.rename(columns={i:'ValueNumeric'},inplace = True)
        tmp_thickrig['ValueDate'] = ''
        tmp_thickrig['StateID'] = '54'
        tmp_thickrig['BeginDate'] = '01/01/2023'
        tmp_thickrig['ValueText'] = ''
        tmp_thickrig['DataItem'] = 'THICKNESS_RIGID'
        print('after remanaming',tmp_thickrig['ValueNumeric'].unique())
        tmp_thickrig = tmp_thickrig.dropna(subset=['ValueNumeric'])
        # tmp_thickrig['ValueNumeric'] = tmp_thickrig['ValueNumeric'].loc[(tmp_thickrig['ValueNumeric']!='nan')]
        tmp_thickrig = tmp_thickrig[tmp_thickrig['ValueNumeric'] != 'nan']
        print('After filtering final',tmp_thickrig['ValueNumeric'].unique())
        tmp_thickrig.to_csv(f'{i}.csv',sep = '|',index = False)
    
    elif i=='Last Overlay Thickness':
        tmp_lastthick = di_df.copy(deep=True)
        # print(tmp_lastthick[i],'Unmutated')
        #Last Overlay Thickness manipulation
        # print('lastOverlayThick',tmp_lastthick[i])
        tmp_lastthick[i] = tmp_lastthick[i].loc[(tmp_lastthick[i] !='Unknown')  & (tmp_lastthick[i]!='Micro') & (tmp_lastthick[i]!='nan') & (tmp_lastthick[i]!=' nan ')]
        # print('after filter',tmp_lastthick[i].unique())
        tmp_lastthick[i] = tmp_lastthick[i].astype(str).map(lambda x : x.rstrip('"'))
        # print('after rstrip',tmp_lastthick[i].unique())
        tmp_lastthick[i] = tmp_lastthick[i].replace(' ',np.nan)
        # print('first inplace',tmp_lastthick[i].unique())
        tmp_lastthick.dropna(subset = [i])
        # print('after dropping na',tmp_lastthick[i].unique())
        tmp_lastthick.rename(columns={i:'ValueNumeric'},inplace = True)
        # print('after rename',tmp_lastthick)
        tmp_lastthick['ValueDate'] = ''
        tmp_lastthick['StateID'] = '54'
        tmp_lastthick['BeginDate'] = '01/01/2023'
        tmp_lastthick['ValueText'] = ''
        tmp_lastthick['DataItem'] = 'LAST_OVERLAY_THICKNESS'
        tmp_lastthick = tmp_lastthick.dropna(subset=['ValueNumeric'])
        # print('lastOverlayThick',tmp_lastthick)
        tmp_lastthick['ValueNumeric'] = tmp_lastthick['ValueNumeric'].map(mapme_float)
        tmp_lastthick = tmp_lastthick[tmp_lastthick['ValueNumeric']>=0.5]
        tmp_lastthick = tmp_lastthick.drop_duplicates(['RouteID','BMP','EMP'])
        # print('second time',tmp_lastthick['ValueNumeric'].value_counts())
        tmp_lastthick.to_csv(f'{i}.csv',sep='|',index=False)
    
    elif i=='Year Last Constructed':
        tmp_yearcon = di_df.copy(deep=True)
    #Year Last Constructed manipulation
        # print('initial upload',tmp_yearcon[i].unique())
        tmp_yearcon[i] = tmp_yearcon[i].loc[(tmp_yearcon[i] !='Unknown') & (tmp_yearcon[i]!='nan')]
        # print('filter applied',tmp_yearcon[i].unique())
        tmp_yearcon[i] = tmp_yearcon[i].astype(str).map(lambda x : x.split('-')[0])
        # print('first split',tmp_yearcon[i].unique())
        tmp_yearcon[i] = tmp_yearcon[i].astype(str).map(lambda x : x.split('.')[0])
        # print('after second split',tmp_yearcon[i].unique())
        tmp_yearcon[i] = tmp_yearcon[i].replace('2',np.nan)
        # print('after replacing',tmp_yearcon[i].unique())
        # tmp_yearcon = tmp_yearcon.dropna(subset = [i])
        tmp_yearcon.rename(columns={i:'ValueDate'},inplace=True)
        tmp_yearcon['ValueDate'] = tmp_yearcon['ValueDate'].map(lambda x: '2022' if x=='2023' else x)
        for a in tmp_yearcon['ValueDate']:
            if a=='2023':
                print('lambda failed',a)
        tmp_yearcon['ValueNumeric'] = ''
        tmp_yearcon['StateID'] = '54'
        tmp_yearcon['BeginDate'] = '01/01/2023'
        tmp_yearcon['ValueText'] = ''
        tmp_yearcon['DataItem'] = 'YEAR_LAST_CONSTRUCTION'
        
        # print('year condition',tmp_yearcon)
        tmp_yearcon = tmp_yearcon.dropna(subset=['ValueDate'])
        tmp_yearcon = tmp_yearcon[tmp_yearcon['ValueDate']!='nan']
        tmp_yearcon = tmp_yearcon.drop_duplicates(['RouteID','BMP','EMP'])
        # print('after drop na \n',tmp_yearcon['ValueDate'].value_counts())
        tmp_yearcon.to_csv(f'{i}.csv',sep ='|',index=False)

    elif i=='Year Last Improvement':
        tmp_yearimp = di_df.copy(deep=True)
        #Year Last Improvement manipulation
        # print('after initial upload',tmp_yearimp[i].unique())
        tmp_yearimp[i] = tmp_yearimp[i].astype('string').map(lambda x : x.split('-')[0])
        # print('after first split',tmp_yearimp[i].unique())
        tmp_yearimp[i] = tmp_yearimp[i].astype('string').map(lambda x : x.split('.')[0])
        # print('after second split',tmp_yearimp[i].unique())
        tmp_yearimp[i] = tmp_yearimp[i].replace(['Turnpike','District 10','Unknown','No info'],np.nan)
        # print('after replacing',tmp_yearimp[i].unique())
        tmp_yearimp = tmp_yearimp.dropna(subset = [i])
        # print('after first drop na',tmp_yearimp[i].unique())
        tmp_yearimp.rename(columns={i:'ValueDate'},inplace=True)
        # print('after rename',tmp_yearimp['ValueDate'].unique())
        tmp_yearimp['ValueDate'] = tmp_yearimp['ValueDate'].map(lambda x: '2023' if x=='2024' else x)
        for a in tmp_yearimp['ValueDate']:
            if a=='2024':
                print('lambda failed',a)
        tmp_yearimp['ValueNumeric'] = ''
        tmp_yearimp['StateID'] = '54'
        tmp_yearimp['BeginDate'] = '01/01/2023'
        tmp_yearimp['ValueText'] = ''
        tmp_yearimp['DataItem'] = 'YEAR_LAST_IMPROVEMENT'
        tmp_yearimp = tmp_yearimp.dropna()
        tmp_yearimp = tmp_yearimp.drop_duplicates(['RouteID','BMP','EMP'])
        print('final drop',tmp_yearimp['ValueDate'].unique())
        # print('year improvement',tmp_yearimp)
        tmp_yearimp.to_csv(f'{i}.csv', sep='|',index=False)
    else:
        print('It done messed up',i)


    
    # di_df['StateCode'] = '54'
    # di_df['BeginDate'] = '01/01/2022'
    # di_df['ValueText'] = ''

    
    # di_df.to_csv(f'{i}.csv',sep = '|',index=False)

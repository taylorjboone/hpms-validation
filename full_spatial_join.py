import pandas as pd

input_file = r'G:\.shortcut-targets-by-id\129JU9Dw85cayEM9V_oKEkrAogePup1hS\hpms_website_project\WV_DOT_HPMS_2022_June_Submission\ril_7_6_2022.xlsx'

def full_spatial_join(data):
    df = pd.read_excel(data, usecols=['RouteID', 'BMP', 'EMP', '65_STATE_FUNCTIONAL_CLASS', '20_FACILITY', 'Label', '39_OWNERSHIP', '83_URBAN_CODE', '36_NHS', '115_STRAHNET', '17_DES_TRUCK_ROUTE', '74_NUM_THROUGH_LANES', '97_ORIG_SURVEY_DIRECTION', '30_IRI_VALUE', '45_PSR'])
    df['DIR_THROUGH_LANES'] = df['74_NUM_THROUGH_LANES'].loc[df['97_ORIG_SURVEY_DIRECTION'].astype('string') == '0']

    df['sjf01'] = True
    df['sjf01'].loc[(df['20_FACILITY'].isin([1, 2, 3, 4, 5, 6])) & (df['65_STATE_FUNCTIONAL_CLASS'].isna())] = False

    df['sjf02'] = True
    df['sjf02'].loc[((df['20_FACILITY'].isin([1,2,4])) & (df['65_STATE_FUNCTIONAL_CLASS'].isin([1,2,3,4,5,6,7])) & (df['83_URBAN_CODE'].isna())) 
                  | ((df['20_FACILITY'].astype('string') == '6') & (df['DIR_THROUGH_LANES'] > 0) & (df['65_STATE_FUNCTIONAL_CLASS'].astype('string') == '1') & ((df['30_IRI_VALUE'].notna()) | (df['45_PSR'].notna())) & (df['83_URBAN_CODE'].isna()))] = False

    return df


df = full_spatial_join(input_file)
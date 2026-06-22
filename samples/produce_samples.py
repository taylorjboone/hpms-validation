import pandas as pd
from datetime import date

today = date.today()


date_hpms = f'01/01/{date.today().year -1}'

def format_samples(row, data_item, value_num='', value_text=''):
    return {'Year_Record': date_hpms,
            'State_Code': 54,
            'Route_ID': str(row['RouteID']), 
            'Begin_Point': round(row['BMP'],4), 
            'End_Point': round(row['EMP'],4), 
            'Data_Item': data_item,
            'Section_Length': round(abs(row['EMP'] - row['BMP']),4),
            'Value_Numeric': value_num,
            'Value_Text' : value_text,
            'Value_Date': '',
            'Comments' : ''
            }


def convert_and_export_v9(df_result, file_name, dataitemv9):
    v9_folder = 'samples/'        

    df_result.rename(columns={'Year_Record': 'BeginDate','State_Code': 'StateID', 'Route_ID':'RouteID', 'Begin_Point': 'BeginPoint', 
                            'End_Point':'EndPoint', 'Data_Item':'DataItem', 'Value_Numeric':'ValueNumeric', 'Value_Text': 'ValueText', 
                            'Value_Date':'ValueDate'},inplace=True)
    df_result = df_result.drop('Section_Length', axis=1)
    df_result['BeginDate'] = date_hpms
    df_result['DataItem'] = dataitemv9
    df_result.to_csv(v9_folder+file_name+".csv", sep='|', index=False)




def main():
    df = pd.read_csv('samples/samples_april11.csv')
    df1 = pd.read_excel('district/district_template.xlsx', dtype={'Sample_ID':str})
    samples = df1.Sample_ID.unique()
    df[df['29_HPMS_SAMPLE_NO'].isin(samples)]

    result = []
    for index, row in df.iterrows():
        result.append(format_samples(row, data_item='Samples', value_num=row['29_HPMS_SAMPLE_NO']))
    df_result = pd.DataFrame(result)
    df_result.to_csv("samples/HPMS_Samples.csv", sep='|', index=False)
    convert_and_export_v9(df_result,"HPMS_samples_v9", "SAMPLES")


main()
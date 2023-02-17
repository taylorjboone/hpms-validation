import os
from os import listdir
from os.path import isfile, join
import pandas as pd 

 
def split_combine(mypath):
    df = pd.DataFrame(columns=['Year_Record','State_Code','RouteID','BMP','EMP','Data_Item','Section_Length','Value_Numeric','Value_Text','Value_Date','Comments'])
    df.to_csv('C:\\PythonTest\\Voltron\\district_chrystal_report_website\\hpms-validation\\base_file.csv')
    onlyfiles = [os.path.join(mypath,f) for f in listdir(mypath) if isfile(join(mypath, f))]
    for split_file in onlyfiles:
        # print(split_file)
        prefix = split_file.replace("C:\\PythonTest\\Voltron\\district_chrystal_report_website\\hpms-validation\\test\\DataItem","").split(".")[0]
        prefix2 = prefix.replace("C:\\PythonTest\\Voltron\\district_chrystal_report_website\\hpms-validation\\test\\","")
        print(prefix2)
        command = f'lrsops split -b C:\\PythonTest\\Voltron\\district_chrystal_report_website\\hpms-validation\\base_file.csv -s {split_file} -c "Data_Item,Value_Numeric" --prefix "{prefix2}" -o C:\PythonTest\Voltron\\base_file.csv' 
        print(command)            
        os.system(command)


split_combine(mypath='C:\PythonTest\Voltron\hpms_data_items_2021\\test')
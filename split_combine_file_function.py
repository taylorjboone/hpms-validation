import os
from os import listdir
from os.path import isfile, join 

 
def split_combine(mypath):
    onlyfiles = [os.path.join(mypath,f) for f in listdir(mypath) if isfile(join(mypath, f))]
    for split_file in onlyfiles:
        # print(split_file)
        prefix = split_file.replace("C:\PythonTest\Voltron\hpms_data_items_2021\\test\\DataItem","").split(".")[0]
        prefix2 = prefix.replace("C:\PythonTest\Voltron\hpms_data_items_2021\\test\\","")
        print(prefix2)
        command = f'lrsops split -b C:\PythonTest\Voltron\\base_file.csv -s {split_file} -c "Data_Item,Value_Numeric" --prefix "{prefix2}" -o C:\PythonTest\Voltron\\base_file.csv' 
        print(command)            
        os.system(command)


split_combine(mypath='C:\PythonTest\Voltron\hpms_data_items_2021\\test')
import pandas as pd
import os
from full_spatial_join_functions import full_spatial_functions
from pm2_validations import pm2_validations
from domain_validation import domain_validations
from cross_validation_copy_71923 import Cross_Validation



class Validate_HPMS():
    def __init__(self,filename='',domain=True,full=True,pm2=True,cross=True):
        if filename == '':
            os.system('python combine_submission_data.py')
            df = pd.read_csv('all_submission_data.csv', dtype={'URBAN_CODE':str, 'AADT_VALUE_DATE':str})
        else:
            df = pd.read_csv(filename, dtype={'URBAN_CODE':str, 'AADT_VALUE_DATE':str})
        if domain ==True:
            print('Running Domain Validations')
            a = domain_validations(df)
            a.run()
            a.create_output()
        if full==True:
            print('Running Full Spatial Join Validations')
            b = full_spatial_functions(df)
            b.run()
            b.create_output()
        if pm2 ==True:
            print('Running PM2 Validations')
            c = pm2_validations(df)
            c.run()
            c.create_output()
        if cross == True:
            print('Running Cross Validations')
            d = Cross_Validation(df)
            d.run(inventory=True,traffic=True)
            d.create_output()













# 
# valid = Validate_HPMS(df,create_data=True,domain=True,full=True,pm2=True,cross=True)
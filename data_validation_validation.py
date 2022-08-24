from distutils.log import error
import os
from matplotlib import scale
import pandas as pd
from wvdot_utils import add_routeid_df, add_geom_validation_df
from os import listdir
from os.path import isfile, join
import numpy as np


urban_code_list = [99999, 99998, 15481, 6139,
                   21745, 36190, 40753, 59275, 67672, 93592, 94726]
facility_type_list = [1, 2, 4, 5, 6, 7]
functional_class_list = [1, 2, 3, 4, 5, 6, 7]
ownership_list = [1, 2, 3, 4, 11, 12, 21, 25, 26, 27, 31,
                  32, 40, 50, 60, 62, 63, 64, 66, 67, 68, 69, 70, 72]
signal_list=[1,2,3,4,5]
median_type_list=[1,2,3,4,5,6,7]
shoulder_type_list=[1,2,3,4,5,6]
peak_parking_list=[1,2,3]
widening_potential_list=[1,2,3,4]
curve_list=['A','B','C','D','E','F']
terrain_types_list=[1,2,3]
grades_list=['A','B','C','D','E','F']
surface_type_list=[1,2,3,4,5,6,7,8,9,10,11]
base_type_list=[1,2,3,4,5,6,7,8]
county_id_list=[1,3,5,7,9,11,13,15,17,19,21,23,25,27,29,31,33,35,37,39,41,43,45,47,49,51,53,55,57,59,61,63,65,67,69,71,73,75,77,79,91,93,95,97,99,101,103,105,107,109,111]
strahnet_type_list=[1,2]
nn_list=[1,2]
main_ops_list=[1,2,3,4,11,12,21,25,26,27]
nhfn_list=[1,2,3]
is_restricted_list=[1]
access_control_list=[1,2,3]


mypath = 'hpms_data_items'



total_errors = pd.DataFrame()


def add_error_df(df, error_message):
    global total_errors
    if len(df) > 0:
        df['Error'] = error_message
        total_errors = pd.concat([total_errors, df], ignore_index=True)
        print('Wrote Errors df with message: %s and size: %s' % (error_message, len(df)))
    else:
        print("No errors found for error message | %s" % error_message)


def read_hpms_csv(x): return pd.read_csv(mypath + "/" + x, sep='|')


def get_invalid_geom_name(x):
    geom_fn = str.split(x, '.')[0]
    return geom_fn + '_geom_check.xlsx'


def output_geom_file(geom_check, fn):
    geom_fn = get_invalid_geom_name(fn)
    geom_check[geom_check.IsValid == False].to_excel(geom_fn, index=False)
    print('Output file made! :)')


def read_hpms_csv(x): return pd.read_csv(mypath + "/" + x, sep='|')


def check_fsystem_valid(x, check_geom):
    data = read_hpms_csv(x)

    if check_geom:
        geom_check = add_geom_validation_df(
            data, routeid_field='Route_ID', bmp_field='Begin_Point', emp_field='End_Point')
        add_error_df(geom_check[geom_check.IsValid ==
                     False], 'fsystem geometry check invalid!')

    # print('F_System',geom_check[geom_check.IsValid==False])

    # output_geom_file(geom_check,x)
    tmpdf = data[~data['Value_Numeric'].isin(functional_class_list)]
    add_error_df(tmpdf, "Check value numeric for fsystem valid")
    tmpdf2 = data[data['Section_Length'] == 0]
    add_error_df(
        tmpdf2, "Check value numeric for fsystem section length is zero")

   


def check_urban_code(x, check_geom):
    data = read_hpms_csv(x)
    if check_geom:
        geom_check = add_geom_validation_df(
            data, routeid_field='Route_ID', bmp_field='Begin_Point', emp_field='End_Point')
        add_error_df(geom_check[geom_check.IsValid ==
                     False], 'fsystem geometry check invalid!')

    # output_geom_file(x)
    # print('Urban_Code',geom_check[geom_check.IsValid==False])

    # if the geometry is invalid pass that invalid geometry df add error df

    tmpdf = data[(data['Value_Numeric'].isna()) | (
        ~data['Value_Numeric'].isin(urban_code_list))]
    add_error_df(tmpdf, "Urban Code is not in accepted range")
    tmpdf2 = data[data['Section_Length'] == 0]
    add_error_df(tmpdf2, "Urban Code section length is equal to zero")

    


def check_facility_type(x, check_geom):
    data = read_hpms_csv(x)
    if check_geom:
        geom_check = add_geom_validation_df(
            data, routeid_field='Route_ID', bmp_field='Begin_Point', emp_field='End_Point')
        add_error_df(geom_check[geom_check.IsValid ==
                     False], 'fsystem geometry check invalid!')

    tmpdf = data[(data['Value_Numeric'].isna()) | (
        ~data['Value_Numeric'].isin(facility_type_list))]
    add_error_df(
        tmpdf, "Facility type error, not a valid facility type in value_numeric")
    tmpdf2 = data[data['Section_Length'] == 0]
    add_error_df(tmpdf2, "facility type section length is equal to zero")

    

def check_ownership(x, check_geom):
    data = read_hpms_csv(x)
    if check_geom:
        geom_check = add_geom_validation_df(
            data, routeid_field='Route_ID', bmp_field='Begin_Point', emp_field='End_Point')
        add_error_df(geom_check[geom_check.IsValid ==
                     False], 'Ownership geometry bad, please fix')

    tmpdf = data[(data['Value_Numeric'] == '') | (
        ~data['Value_Numeric'].isin(ownership_list))]
    add_error_df(
        tmpdf, "Ownership list error, not a valid ownership list in value_numeric")
    tmpdf2 = data[data['Section_Length'] == 0]
    add_error_df(tmpdf2, "Ownership section length is zero")

   


def check_through_lanes(x, check_geom):
    data = read_hpms_csv(x)
    if check_geom:
        geom_check = add_geom_validation_df(
            data, routeid_field='Route_ID', bmp_field='Begin_Point', emp_field='End_Point')
        add_error_df(geom_check[geom_check.IsValid ==
                     False], 'fsystem geometry check invalid!')

    tmpdf = data[data.Value_Numeric.isna()]
    add_error_df(tmpdf, "Through lanes value numeric is nan")
    tmpdf2 = data[data['Section_Length'] == 0]
    add_error_df(tmpdf2, "Through laness section length is zero")
   


def nhs_new_check(x,check_geom):
    data = read_hpms_csv(x)
    if check_geom:
        geom_check = add_geom_validation_df(
            data, routeid_field='Route_ID', bmp_field='Begin_Point', emp_field='End_Point')
        add_error_df(geom_check[geom_check.IsValid ==
                     False], 'NHS geometry check invalid!')
    tmpdf = data[data.Value_Numeric.isna()]
    add_error_df(tmpdf, "NHS value numeric is nan")
    tmpdf2 = data[data['Section_Length'] == 0]
    add_error_df(tmpdf2, "NHS section length is zero")
   


def check_dir_through_lanes(x,check_geom):
    data = read_hpms_csv(x)
    if check_geom:
        geom_check = add_geom_validation_df(
            data, routeid_field='Route_ID', bmp_field='Begin_Point', emp_field='End_Point')
        add_error_df(geom_check[geom_check.IsValid ==
                     False], 'Direct through lanes geometry check invalid!')
    tmpdf = data[data.Value_Numeric.isna()]
    add_error_df(tmpdf, "Direct through lanes value numeric is nan")
    tmpdf2 = data[data['Section_Length'] == 0]
    add_error_df(tmpdf2, "Direct Through lanes section length is zero")


def check_aadt(x,check_geom):
    data = read_hpms_csv(x)
    if check_geom:
        geom_check = add_geom_validation_df(
            data, routeid_field='Route_ID', bmp_field='Begin_Point', emp_field='End_Point')
        add_error_df(geom_check[geom_check.IsValid ==
                     False], 'AADT geometry check invalid!')
    tmpdf = data[data.Value_Numeric.isna()]
    add_error_df(tmpdf, "AADT value numeric is nan")
    tmpdf2 = data[data['Section_Length'] == 0]
    add_error_df(tmpdf2, "AADT section length is zero")


def check_aadt_single_unit(x,check_geom):
    data = read_hpms_csv(x)
    if check_geom:
        geom_check = add_geom_validation_df(
            data, routeid_field='Route_ID', bmp_field='Begin_Point', emp_field='End_Point')
        add_error_df(geom_check[geom_check.IsValid ==
                     False], 'AADT Single Unit geometry check invalid!')
    tmpdf = data[data.Value_Numeric.isna()]
    add_error_df(tmpdf, "AADT Single Unit value numeric is nan")
    tmpdf2 = data[data['Section_Length'] == 0]
    add_error_df(tmpdf2, "AADT Single Unit section length is zero")


def check_Pct_Peak_Single(x,check_geom):
    data = read_hpms_csv(x)
    if check_geom:
        geom_check = add_geom_validation_df(
            data, routeid_field='Route_ID', bmp_field='Begin_Point', emp_field='End_Point')
        add_error_df(geom_check[geom_check.IsValid ==
                     False], 'PCT Peak Single geometry check invalid!')
    tmpdf = data[data.Value_Numeric.isna()]
    add_error_df(tmpdf, "PCT Peak Single value numeric is nan")
    tmpdf2 = data[data['Section_Length'] == 0]
    add_error_df(tmpdf2, "PCT Peak Single section length is zero")


def check_AADT_Combination(x,check_geom):
    data = read_hpms_csv(x)
    if check_geom:
        geom_check = add_geom_validation_df(
            data, routeid_field='Route_ID', bmp_field='Begin_Point', emp_field='End_Point')
        add_error_df(geom_check[geom_check.IsValid ==
                     False], 'AADT Combination geometry check invalid!')
    tmpdf = data[data.Value_Numeric.isna()]
    add_error_df(tmpdf, "AADT Combination value numeric is nan")
    tmpdf2 = data[data['Section_Length'] == 0]
    add_error_df(tmpdf2, "AADT Combination section length is zero")


def check_pct_peak_combination(x,check_geom):
    data = read_hpms_csv(x)
    if check_geom:
        geom_check = add_geom_validation_df(
            data, routeid_field='Route_ID', bmp_field='Begin_Point', emp_field='End_Point')
        add_error_df(geom_check[geom_check.IsValid ==
                     False], 'PCT Peak Combination geometry check invalid!')
    tmpdf = data[data.Value_Numeric.isna()]
    add_error_df(tmpdf, "PCT Peak Combination value numeric is nan")
    tmpdf2 = data[data['Section_Length'] == 0]
    add_error_df(tmpdf2, "PCT Peak Combination section length is zero")


def check_k_factor(x,check_geom):
    data = read_hpms_csv(x)
    if check_geom:
        geom_check = add_geom_validation_df(
            data, routeid_field='Route_ID', bmp_field='Begin_Point', emp_field='End_Point')
        add_error_df(geom_check[geom_check.IsValid ==
                     False], 'K Factor geometry check invalid!')
    tmpdf = data[data.Value_Numeric.isna()]
    add_error_df(tmpdf, "K Factor value numeric is nan")
    tmpdf2 = data[data['Section_Length'] == 0]
    add_error_df(tmpdf2, "K Factor section length is zero")

def dir_factor(x,check_geom):
    data = read_hpms_csv(x)
    if check_geom:
        geom_check = add_geom_validation_df(
            data, routeid_field='Route_ID', bmp_field='Begin_Point', emp_field='End_Point')
        add_error_df(geom_check[geom_check.IsValid ==
                     False], 'dir factor geometry check invalid!')
    tmpdf = data[data.Value_Numeric.isna()]
    add_error_df(tmpdf, "dir factor value numeric is nan")
    tmpdf2 = data[data['Section_Length'] == 0]
    add_error_df(tmpdf2, "dir factor section length is zero")

def signal_type(x,check_geom):
    data = read_hpms_csv(x)
    if check_geom:
        geom_check = add_geom_validation_df(
            data, routeid_field='Route_ID', bmp_field='Begin_Point', emp_field='End_Point')
        add_error_df(geom_check[geom_check.IsValid ==
                     False], 'K Factor geometry check invalid!')
    tmpdf = data[(data['Value_Numeric'].isna()) | (
        ~data['Value_Numeric'].isin(signal_list))]
    add_error_df(tmpdf, "Urban Code is not in accepted range")
    tmpdf2 = data[data['Section_Length'] == 0]
    add_error_df(tmpdf2, "K Factor section length is zero")

def pct_green_time(x,check_geom):
    data = read_hpms_csv(x)
    if check_geom:
        geom_check = add_geom_validation_df(
            data, routeid_field='Route_ID', bmp_field='Begin_Point', emp_field='End_Point')
        add_error_df(geom_check[geom_check.IsValid ==
                     False], 'Peak green time geometry check invalid!')
    tmpdf = data[data.Value_Numeric.isna()]
    add_error_df(tmpdf, "Peak green time numeric is nan")
    tmpdf2 = data[data['Section_Length'] == 0]
    add_error_df(tmpdf2, "Peak green time length is zero")

def lane_width(x,check_geom):
    data = read_hpms_csv(x)
    if check_geom:
        geom_check = add_geom_validation_df(
            data, routeid_field='Route_ID', bmp_field='Begin_Point', emp_field='End_Point')
        add_error_df(geom_check[geom_check.IsValid ==
                     False], 'Lane width geometry check invalid!')
    tmpdf = data[data.Value_Numeric.isna()]
    add_error_df(tmpdf, "Lane width numeric is nan")
    tmpdf2 = data[data['Section_Length'] == 0]
    add_error_df(tmpdf2, "Lane width section length is zero")

def median_type(x,check_geom):
    data = read_hpms_csv(x)
    if check_geom:
        geom_check = add_geom_validation_df(
            data, routeid_field='Route_ID', bmp_field='Begin_Point', emp_field='End_Point')
        add_error_df(geom_check[geom_check.IsValid ==
                     False], 'Median type geometry check invalid!')
        tmpdf = data[(data['Value_Numeric'].isna()) | (
        ~data['Value_Numeric'].isin(median_type_list))]
    add_error_df(tmpdf, "Median type is not in accepted range")
    tmpdf2 = data[data['Section_Length'] == 0]
    add_error_df(tmpdf2, "Median type section length is zero")

def median_width(x,check_geom):
    data = read_hpms_csv(x)
    if check_geom:
        geom_check = add_geom_validation_df(
            data, routeid_field='Route_ID', bmp_field='Begin_Point', emp_field='End_Point')
        add_error_df(geom_check[geom_check.IsValid ==
                     False], 'Median width geometry check invalid!')
    tmpdf = data[data.Value_Numeric.isna()]
    add_error_df(tmpdf, "Median width numeric is nan")
    tmpdf2 = data[data['Section_Length'] == 0]
    add_error_df(tmpdf2, "Median width section length is zero")

def shoulder_type(x,check_geom):
    data = read_hpms_csv(x)
    if check_geom:
        geom_check = add_geom_validation_df(
            data, routeid_field='Route_ID', bmp_field='Begin_Point', emp_field='End_Point')
        add_error_df(geom_check[geom_check.IsValid ==
                     False], 'Shoulder type geometry check invalid!')
    tmpdf = data[(data['Value_Numeric'].isna()) | (
    ~data['Value_Numeric'].isin(shoulder_type_list))]
    add_error_df(tmpdf, "Shoulder type is not in accepted range")
    tmpdf2 = data[data['Section_Length'] == 0]
    add_error_df(tmpdf2, "Shoulder type section length is zero")

def shoulder_width_r(x,check_geom):
    data = read_hpms_csv(x)
    if check_geom:
        geom_check = add_geom_validation_df(
            data, routeid_field='Route_ID', bmp_field='Begin_Point', emp_field='End_Point')
        add_error_df(geom_check[geom_check.IsValid ==
                     False], 'Shoulder width R geometry check invalid!')
    tmpdf = data[data.Value_Numeric.isna()]
    add_error_df(tmpdf, "Shoulder width R value numeric is nan")
    tmpdf2 = data[data['Section_Length'] == 0]
    add_error_df(tmpdf2, "Shoulder width R section length is zero")

def shoulder_width_l(x,check_geom):
    data = read_hpms_csv(x)
    if check_geom:
        geom_check = add_geom_validation_df(
            data, routeid_field='Route_ID', bmp_field='Begin_Point', emp_field='End_Point')
        add_error_df(geom_check[geom_check.IsValid ==
                     False], 'Shoulder width L geometry check invalid!')
    tmpdf = data[data.Value_Numeric.isna()]
    add_error_df(tmpdf, "Shoulder width L value numeric is nan")
    tmpdf2 = data[data['Section_Length'] == 0]
    add_error_df(tmpdf2, "Shoulder width L section length is zero")

def peak_parking(x,check_geom):
    data = read_hpms_csv(x)
    if check_geom:
        geom_check = add_geom_validation_df(
            data, routeid_field='Route_ID', bmp_field='Begin_Point', emp_field='End_Point')
        add_error_df(geom_check[geom_check.IsValid ==
                     False], 'Peak parking geometry check invalid!')
    tmpdf = data[(data['Value_Numeric'].isna()) | (
        ~data['Value_Numeric'].isin(peak_parking_list))]
    add_error_df(tmpdf, "Peak parking is not in accepted range")
    tmpdf2 = data[data['Section_Length'] == 0]
    add_error_df(tmpdf2, "Peak parking section length is zero")

def widening_potential(x,check_geom):
    data = read_hpms_csv(x)
    if check_geom:
        geom_check = add_geom_validation_df(
            data, routeid_field='Route_ID', bmp_field='Begin_Point', emp_field='End_Point')
        add_error_df(geom_check[geom_check.IsValid ==
                     False], 'Widening potential geometry check invalid!')
    tmpdf = data[(data['Value_Numeric'].isna()) | (
        ~data['Value_Numeric'].isin(widening_potential_list))]
    add_error_df(tmpdf, "Widening potential is not in accepted range")
    tmpdf2 = data[data['Section_Length'] == 0]
    add_error_df(tmpdf2, "widening potential section length is zero")

def curves(x,check_geom):
    data = read_hpms_csv(x)
    if check_geom:
        geom_check = add_geom_validation_df(
            data, routeid_field='Route_ID', bmp_field='Begin_Point', emp_field='End_Point')
        add_error_df(geom_check[geom_check.IsValid ==
                     False], 'curves geometry check invalid!')
    tmpdf = data[(data['Value_Numeric'].isna()) | (
        ~data['Value_Numeric'].isin(curve_list))]
    add_error_df(tmpdf, "Curves is not in accepted range")
    tmpdf2 = data[data['Section_Length'] == 0]
    add_error_df(tmpdf2, "Curves section length is zero")

def terrain_types(x,check_geom):
    data = read_hpms_csv(x)
    if check_geom:
        geom_check = add_geom_validation_df(
            data, routeid_field='Route_ID', bmp_field='Begin_Point', emp_field='End_Point')
        add_error_df(geom_check[geom_check.IsValid ==
                     False], 'Terrain Type geometry check invalid!')
    tmpdf = data[(data['Value_Numeric'].isna()) | (
        ~data['Value_Numeric'].isin(curve_list))]
    add_error_df(tmpdf, "Terrain Type is not in accepted range")
    tmpdf2 = data[data['Section_Length'] == 0]
    add_error_df(tmpdf2, "Terrain section length is zero")

def grades(x,check_geom):
    data = read_hpms_csv(x)
    if check_geom:
        geom_check = add_geom_validation_df(
            data, routeid_field='Route_ID', bmp_field='Begin_Point', emp_field='End_Point')
        add_error_df(geom_check[geom_check.IsValid ==
                     False], 'grades geometry check invalid!')
    tmpdf = data[(data['Value_Numeric'].isna()) | (
    ~data['Value_Numeric'].isin(grades_list))]
    tmpdf2 = data[data['Section_Length'] == 0]
    add_error_df(tmpdf2, "grades section length is zero")

def pct_pass_sight(x,check_geom):
    data = read_hpms_csv(x)
    if check_geom:
        geom_check = add_geom_validation_df(
            data, routeid_field='Route_ID', bmp_field='Begin_Point', emp_field='End_Point')
        add_error_df(geom_check[geom_check.IsValid ==
                     False], 'PCT Pass Sight geometry check invalid!')
    tmpdf = data[data.Value_Numeric.isna()]
    add_error_df(tmpdf, "PCT Pass Sight value numeric is nan")
    tmpdf2 = data[data['Section_Length'] == 0]
    add_error_df(tmpdf2, "PCT Pass Sight section length is zero")

def iri(x,check_geom):
    data = read_hpms_csv(x)
    if check_geom:
        geom_check = add_geom_validation_df(
            data, routeid_field='Route_ID', bmp_field='Begin_Point', emp_field='End_Point')
        add_error_df(geom_check[geom_check.IsValid ==
                     False], 'IRI geometry check invalid!')
    tmpdf = data[data.Value_Numeric.isna()]
    add_error_df(tmpdf, "IRI value numeric is nan")
    tmpdf2 = data[data['Section_Length'] == 0]
    add_error_df(tmpdf2, "IRI section length is zero")

def surface_type(x,check_geom):
    data = read_hpms_csv(x)
    if check_geom:
        geom_check = add_geom_validation_df(
            data, routeid_field='Route_ID', bmp_field='Begin_Point', emp_field='End_Point')
        add_error_df(geom_check[geom_check.IsValid ==
                     False], 'K Factor geometry check invalid!')
    tmpdf = data[(data['Value_Numeric'].isna()) | (
        ~data['Value_Numeric'].isin(surface_type_list))]
    tmpdf2 = data[data['Section_Length'] == 0]
    add_error_df(tmpdf2, "K Factor section length is zero")

def rutting(x,check_geom):
    data = read_hpms_csv(x)
    if check_geom:
        geom_check = add_geom_validation_df(
            data, routeid_field='Route_ID', bmp_field='Begin_Point', emp_field='End_Point')
        add_error_df(geom_check[geom_check.IsValid ==
                     False], 'K Factor geometry check invalid!')
    tmpdf = data[(data['Value_Numeric'].isna()) | (
    ~data['Value_Numeric'].isin(grades_list))]
    tmpdf2 = data[data['Section_Length'] == 0]
    add_error_df(tmpdf2, "K Factor section length is zero")

def faulting(x,check_geom):
    data = read_hpms_csv(x)
    if check_geom:
        geom_check = add_geom_validation_df(
            data, routeid_field='Route_ID', bmp_field='Begin_Point', emp_field='End_Point')
        add_error_df(geom_check[geom_check.IsValid ==
                     False], 'Faulting geometry check invalid!')
    tmpdf = data[data.Value_Numeric.isna()]
    add_error_df(tmpdf, "Faulting value numeric is nan")
    tmpdf2 = data[data['Section_Length'] == 0]
    add_error_df(tmpdf2, "Faulting section length is zero")

def cracking_percent(x,check_geom):
    data = read_hpms_csv(x)
    if check_geom:
        geom_check = add_geom_validation_df(
            data, routeid_field='Route_ID', bmp_field='Begin_Point', emp_field='End_Point')
        add_error_df(geom_check[geom_check.IsValid ==
                     False], 'Cracking Percent geometry check invalid!')
    tmpdf = data[data.Value_Numeric.isna()]
    add_error_df(tmpdf, "Cracking Percent value numeric is nan")
    tmpdf2 = data[data['Section_Length'] == 0]
    add_error_df(tmpdf2, "Cracking Percent section length is zero")

def year_last_improvement(x,check_geom):
    data = read_hpms_csv(x)
    if check_geom:
        geom_check = add_geom_validation_df(
            data, routeid_field='Route_ID', bmp_field='Begin_Point', emp_field='End_Point')
        add_error_df(geom_check[geom_check.IsValid ==
                     False], 'K Factor geometry check invalid!')
    tmpdf = data[data.Value_Numeric.isna()]
    add_error_df(tmpdf, "Year Last Improvement value numeric is nan")
    tmpdf2 = data[data['Section_Length'] == 0]
    add_error_df(tmpdf2, "Year Last Improvement section length is zero")

def year_last_construction(x,check_geom):
    data = read_hpms_csv(x)
    if check_geom:
        geom_check = add_geom_validation_df(
            data, routeid_field='Route_ID', bmp_field='Begin_Point', emp_field='End_Point')
        add_error_df(geom_check[geom_check.IsValid ==
                     False], 'Year Last Construction geometry check invalid!')
    tmpdf = data[data.Value_Numeric.isna()]
    add_error_df(tmpdf, "Year Last Construction value numeric is nan")
    tmpdf2 = data[data['Section_Length'] == 0]
    add_error_df(tmpdf2, "Year Last Construction section length is zero")

def last_overlay_thickness(x,check_geom):
    data = read_hpms_csv(x)
    if check_geom:
        geom_check = add_geom_validation_df(
            data, routeid_field='Route_ID', bmp_field='Begin_Point', emp_field='End_Point')
        add_error_df(geom_check[geom_check.IsValid ==
                     False], 'Last Overlay Thickness geometry check invalid!')
    tmpdf = data[data.Value_Numeric.isna()]
    add_error_df(tmpdf, "Last Overlay Thickness value numeric is nan")
    tmpdf2 = data[data['Section_Length'] == 0]
    add_error_df(tmpdf2, "Last Overlay thickness section length is zero")

def thickness_rigid(x,check_geom):
    data = read_hpms_csv(x)
    if check_geom:
        geom_check = add_geom_validation_df(
            data, routeid_field='Route_ID', bmp_field='Begin_Point', emp_field='End_Point')
        add_error_df(geom_check[geom_check.IsValid ==
                     False], 'thickness rigid geometry check invalid!')
    tmpdf = data[data.Value_Numeric.isna()]
    add_error_df(tmpdf, "Thickness Rigid value numeric is nan")
    tmpdf2 = data[data['Section_Length'] == 0]
    add_error_df(tmpdf2, "Thickness Rigid section length is zero")

def thickness_flexible(x,check_geom):
    data = read_hpms_csv(x)
    if check_geom:
        geom_check = add_geom_validation_df(
            data, routeid_field='Route_ID', bmp_field='Begin_Point', emp_field='End_Point')
        add_error_df(geom_check[geom_check.IsValid ==
                     False], 'thickness flexible geometry check invalid!')
    tmpdf = data[data.Value_Numeric.isna()]
    add_error_df(tmpdf, "Thickness Flexible value numeric is nan")
    tmpdf2 = data[data['Section_Length'] == 0]
    add_error_df(tmpdf2, "Thickness Flexible section length is zero")

def base_type(x,check_geom):
    data = read_hpms_csv(x)
    if check_geom:
        geom_check = add_geom_validation_df(
            data, routeid_field='Route_ID', bmp_field='Begin_Point', emp_field='End_Point')
        add_error_df(geom_check[geom_check.IsValid ==
                     False], 'K Factor geometry check invalid!')
    tmpdf = data[(data['Value_Numeric'].isna()) | (
    ~data['Value_Numeric'].isin(base_type_list))]
    tmpdf2 = data[data['Section_Length'] == 0]
    add_error_df(tmpdf2, "K Factor section length is zero")

def base_thickness(x,check_geom):
    data = read_hpms_csv(x)
    if check_geom:
        geom_check = add_geom_validation_df(
            data, routeid_field='Route_ID', bmp_field='Begin_Point', emp_field='End_Point')
        add_error_df(geom_check[geom_check.IsValid ==
                     False], 'Base thickness geometry check invalid!')
    tmpdf = data[data.Value_Numeric.isna()]
    add_error_df(tmpdf, "Base thickness value numeric is nan")
    tmpdf2 = data[data['Section_Length'] == 0]
    add_error_df(tmpdf2, "Base thickness section length is zero")

def county_id(x,check_geom):
    data = read_hpms_csv(x)
    if check_geom:
        geom_check = add_geom_validation_df(
            data, routeid_field='Route_ID', bmp_field='Begin_Point', emp_field='End_Point')
        add_error_df(geom_check[geom_check.IsValid ==
                     False], 'County ID geometry check invalid!')
    tmpdf = data[(data['Value_Numeric'].isna()) | (
    ~data['Value_Numeric'].isin(county_id_list))]
    tmpdf2 = data[data['Section_Length'] == 0]
    add_error_df(tmpdf2, "County ID section length is zero")

def strahnet_type(x,check_geom):
    data = read_hpms_csv(x)
    if check_geom:
        geom_check = add_geom_validation_df(
            data, routeid_field='Route_ID', bmp_field='Begin_Point', emp_field='End_Point')
        add_error_df(geom_check[geom_check.IsValid ==
                     False], 'STRAHNET geometry check invalid!')
    tmpdf = data[(data['Value_Numeric'].isna()) | (
    ~data['Value_Numeric'].isin(strahnet_type_list))]
    tmpdf2 = data[data['Section_Length'] == 0]
    add_error_df(tmpdf2, "STRAHNET section length is zero")

def nn(x,check_geom):
    data = read_hpms_csv(x)
    if check_geom:
        geom_check = add_geom_validation_df(
            data, routeid_field='Route_ID', bmp_field='Begin_Point', emp_field='End_Point')
        add_error_df(geom_check[geom_check.IsValid ==
                     False], 'NN geometry check invalid!')
    tmpdf = data[(data['Value_Numeric'].isna()) | (
    ~data['Value_Numeric'].isin(nn_list))]
    tmpdf2 = data[data['Section_Length'] == 0]
    add_error_df(tmpdf2, "NN section length is zero")

def maintenance_operations(x,check_geom):
    data = read_hpms_csv(x)
    if check_geom:
        geom_check = add_geom_validation_df(
            data, routeid_field='Route_ID', bmp_field='Begin_Point', emp_field='End_Point')
        add_error_df(geom_check[geom_check.IsValid ==
                     False], 'Maintence operations geometry check invalid!')
    tmpdf = data[(data['Value_Numeric'].isna()) | (
    ~data['Value_Numeric'].isin(main_ops_list))]
    tmpdf2 = data[data['Section_Length'] == 0]
    add_error_df(tmpdf2, "Main operations section length is zero")

def dir_through_lanes(x,check_geom):
    data = read_hpms_csv(x)
    if check_geom:
        geom_check = add_geom_validation_df(
            data, routeid_field='Route_ID', bmp_field='Begin_Point', emp_field='End_Point')
        add_error_df(geom_check[geom_check.IsValid ==
                     False], 'Direct Through Lanes geometry check invalid!')
    tmpdf = data[data.Value_Numeric.isna()]
    add_error_df(tmpdf, "DIrect Through Lanes value numeric is nan")
    tmpdf2 = data[data['Section_Length'] == 0]
    add_error_df(tmpdf2, "Direct Through Lanes section length is zero")

def travel_time_code(x,check_geom):
    data = read_hpms_csv(x)
    if check_geom:
        geom_check = add_geom_validation_df(
            data, routeid_field='Route_ID', bmp_field='Begin_Point', emp_field='End_Point')
        add_error_df(geom_check[geom_check.IsValid ==
                     False], 'Travel Time Code geometry check invalid!')
    tmpdf = data[data.Value_Numeric.isna()]
    add_error_df(tmpdf, "Travel Time Code value numeric is nan")
    tmpdf2 = data[data['Section_Length'] == 0]
    add_error_df(tmpdf2, "Travel Time Code section length is zero")

def nhfn(x,check_geom):
    data = read_hpms_csv(x)
    if check_geom:
        geom_check = add_geom_validation_df(
            data, routeid_field='Route_ID', bmp_field='Begin_Point', emp_field='End_Point')
        add_error_df(geom_check[geom_check.IsValid ==
                     False], 'NHFN geometry check invalid!')
    tmpdf = data[(data['Value_Numeric'].isna()) | (
    ~data['Value_Numeric'].isin(nhfn_list))]
    tmpdf2 = data[data['Section_Length'] == 0]
    add_error_df(tmpdf2, "NHFN section length is zero")

def is_restricted(x,check_geom):
    data = read_hpms_csv(x)
    if check_geom:
        geom_check = add_geom_validation_df(
            data, routeid_field='Route_ID', bmp_field='Begin_Point', emp_field='End_Point')
        add_error_df(geom_check[geom_check.IsValid ==
                     False], 'K Factor geometry check invalid!')
    tmpdf = data[(data['Value_Numeric'].isna()) | (
    ~data['Value_Numeric'].isin(is_restricted_list))]
    tmpdf2 = data[data['Section_Length'] == 0]
    add_error_df(tmpdf2, "K Factor section length is zero")

def access_control(x,check_geom):
    data = read_hpms_csv(x)
    if check_geom:
        geom_check = add_geom_validation_df(
            data, routeid_field='Route_ID', bmp_field='Begin_Point', emp_field='End_Point')
        add_error_df(geom_check[geom_check.IsValid ==
                     False], 'Access Control geometry check invalid!')
    tmpdf = data[(data['Value_Numeric'].isna()) | (
    ~data['Value_Numeric'].isin(access_control_list))]
    tmpdf2 = data[data['Section_Length'] == 0]
    add_error_df(tmpdf2, "Access Control section length is zero")






def fail_func(): print('shit failed')


onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))]

# the key is the filename the value is the function
validation_dict = {
    'DataItem1_F_System.csv': [check_fsystem_valid,'F_System'],
    'DataItem2_Urban_Code.csv': [check_urban_code,'Urban_Code'],
    'DataItem6_Ownership.csv': [check_ownership,'Ownership'],
    'DataItem3_Facility_Type.csv': [check_facility_type,'Facility_Type'],
    'DataItem7_Through_Lanes.csv': [check_through_lanes,'Through_Lanes'],
    'DataItem64_NHS.csv':[nhs_new_check,'NHS'],
    'DataItem70_Dir_Through_Lanes.csv':[check_dir_through_lanes,'Dir_Through_Lanes'],
    'DataItem21-AADT.csv':[check_aadt,'AADT'],
    'DataItem22-AADT_Single_Unit.csv':[check_aadt_single_unit,'AADT_Single_Unit'],
    'DataItem23-Pct_Peak_Single.csv':[check_Pct_Peak_Single,'Pct_Peak_Single'],
    'DataItem24-AADT_Combination.csv':[check_AADT_Combination,'AADT_Combination'],
    'DataItem25-Pct_Peak_Combination.csv':[check_pct_peak_combination,'Pct_Peak_Combination'],
    'HPMS_2021_SAMPLES_LANE_WIDTH.csv':[lane_width,'Lane_Width']
}

def check_columns(fn,col):
    df = read_hpms_csv(fn)
    add_error_df(df[df.Data_Item!=col],'%s field not in Data_Item Column' % col)

def check_all(check_geom=True):
    for fn in onlyfiles:
        # print(fn, "processing file")
        vals = validation_dict.get(fn, False)
        if vals != False:
            myfunc,col = vals 
            check_columns(fn,col)

            myfunc(fn, check_geom=check_geom)


check_all(check_geom=True)



total_errors[['Error'] + ['Year_Record',
                          'State_Code',
                          'Route_ID',
                          'Begin_Point',
                          'End_Point',
                          'Data_Item',
                          'Section_Length',
                          'Value_Numeric',
                          'Value_Text',
                          'Value_Date',
                          'Comments']].to_excel('errors.xlsx', index=False)

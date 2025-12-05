import pandas as pd


df_curves = pd.read_csv('./raw_curves_grades/curves_out.csv')

df_grades = pd.read_csv('./raw_curves_grades/grades_out.csv')

for curve in df_curves.columns:
    if curve == 'A':
        df_curve = df_curves[['RouteID','BMP','EMP',curve]]
        df_curve['DataItem'] = f'Curves_{curve}'
        df_curve['BeginDate'] = '01/01/2024'
        df_curve['ValueText'] = ''
        df_curve['ValueDate'] = ''
        df_curve['StateID'] = '54'
        df_curve['Comments'] = ''
        df_curve.rename(columns = {'A':'ValueNumeric'},inplace = True)
        df_curve = df_curve[df_curve['ValueNumeric'] != 0]
        df_curve.to_csv(fr'./curves_grades_out/43_Curves_{curve}.csv',sep = '|',index = False)
    elif curve == 'B':
        df_curve = df_curves[['RouteID','BMP','EMP',curve]]
        df_curve['DataItem'] = f'Curves_{curve}'
        df_curve['BeginDate'] = '01/01/2024'
        df_curve['ValueText'] = ''
        df_curve['ValueDate'] = ''
        df_curve['StateID'] = '54'
        df_curve['Comments'] = ''
        df_curve.rename(columns = {'B':'ValueNumeric'},inplace = True)
        df_curve = df_curve[df_curve['ValueNumeric'] != 0]
        df_curve.to_csv(fr'./curves_grades_out/43_Curves_{curve}.csv',sep = '|',index = False)
    elif curve == 'C':
        df_curve = df_curves[['RouteID','BMP','EMP',curve]]
        df_curve['DataItem'] = f'Curves_{curve}'
        df_curve['BeginDate'] = '01/01/2024'
        df_curve['ValueText'] = ''
        df_curve['ValueDate'] = ''
        df_curve['StateID'] = '54'
        df_curve['Comments'] = ''
        df_curve.rename(columns = {'C':'ValueNumeric'},inplace = True)
        df_curve = df_curve[df_curve['ValueNumeric'] != 0]
        df_curve.to_csv(fr'./curves_grades_out/43_Curves_{curve}.csv',sep = '|',index = False)
    elif curve == 'D':
        df_curve = df_curves[['RouteID','BMP','EMP',curve]]
        df_curve['DataItem'] = f'Curves_{curve}'
        df_curve['BeginDate'] = '01/01/2024'
        df_curve['ValueText'] = ''
        df_curve['ValueDate'] = ''
        df_curve['StateID'] = '54'
        df_curve['Comments'] = ''
        df_curve.rename(columns = {'D':'ValueNumeric'},inplace = True)
        df_curve = df_curve[df_curve['ValueNumeric'] != 0]
        df_curve.to_csv(fr'./curves_grades_out/43_Curves_{curve}.csv',sep = '|',index = False)
    elif curve == 'E':
        df_curve = df_curves[['RouteID','BMP','EMP',curve]]
        df_curve['DataItem'] = f'Curves_{curve}'
        df_curve['BeginDate'] = '01/01/2024'
        df_curve['ValueText'] = ''
        df_curve['ValueDate'] = ''
        df_curve['StateID'] = '54'
        df_curve['Comments'] = ''
        df_curve.rename(columns = {'E':'ValueNumeric'},inplace = True)
        df_curve = df_curve[df_curve['ValueNumeric'] != 0]
        df_curve.to_csv(fr'./curves_grades_out/43_Curves_{curve}.csv',sep = '|',index = False)
    elif curve == 'F':
        df_curve = df_curves[['RouteID','BMP','EMP',curve]]
        df_curve['DataItem'] = f'Curves_{curve}'
        df_curve['BeginDate'] = '01/01/2024'
        df_curve['ValueText'] = ''
        df_curve['ValueDate'] = ''
        df_curve['StateID'] = '54'
        df_curve['Comments'] = ''
        df_curve.rename(columns = {'F':'ValueNumeric'},inplace = True)
        df_curve = df_curve[df_curve['ValueNumeric'] != 0]
        df_curve.to_csv(fr'./curves_grades_out/43_Curves_{curve}.csv',sep = '|',index = False)


for grade in df_grades.columns:
    if grade == 'A':
        df_grade = df_grades[['RouteID','BMP','EMP',grade]]
        df_grade['DataItem'] = f'Grades_{grade}'
        df_grade['BeginDate'] = '01/01/2024'
        df_grade['ValueText'] = ''
        df_grade['ValueDate'] = ''
        df_grade['StateID'] = '54'
        df_grade['Comments'] = ''
        df_grade.rename(columns = {'A':'ValueNumeric'},inplace = True)
        df_grade = df_grade[df_grade['ValueNumeric'] != 0]
        df_grade.to_csv(fr'./curves_grades_out/45_Grades_{grade}.csv',sep = '|',index = False)
    elif grade == 'B':
        df_grade = df_grades[['RouteID','BMP','EMP',grade]]
        df_grade['DataItem'] = f'Grades_{grade}'
        df_grade['BeginDate'] = '01/01/2024'
        df_grade['ValueText'] = ''
        df_grade['ValueDate'] = ''
        df_grade['StateID'] = '54'
        df_grade['Comments'] = ''
        df_grade.rename(columns = {'B':'ValueNumeric'},inplace = True)
        df_grade = df_grade[df_grade['ValueNumeric'] != 0]
        df_grade.to_csv(fr'./curves_grades_out/45_Grades_{grade}.csv',sep = '|',index = False)
    elif grade == 'C':
        df_grade = df_grades[['RouteID','BMP','EMP',grade]]
        df_grade['DataItem'] = f'Grades_{grade}'
        df_grade['BeginDate'] = '01/01/2024'
        df_grade['ValueText'] = ''
        df_grade['ValueDate'] = ''
        df_grade['StateID'] = '54'
        df_grade['Comments'] = ''
        df_grade.rename(columns = {'C':'ValueNumeric'},inplace = True)
        df_grade = df_grade[df_grade['ValueNumeric'] != 0]
        df_grade.to_csv(fr'./curves_grades_out/45_Grades_{grade}.csv',sep = '|',index = False)
    elif grade == 'D':
        df_grade = df_grades[['RouteID','BMP','EMP',grade]]
        df_grade['DataItem'] = f'Grades_{grade}'
        df_grade['BeginDate'] = '01/01/2024'
        df_grade['ValueText'] = ''
        df_grade['ValueDate'] = ''
        df_grade['StateID'] = '54'
        df_grade['Comments'] = ''
        df_grade.rename(columns = {'D':'ValueNumeric'},inplace = True)
        df_grade = df_grade[df_grade['ValueNumeric'] != 0]
        df_grade.to_csv(fr'./curves_grades_out/45_Grades_{grade}.csv',sep = '|',index = False)
    elif grade == 'E':
        df_grade = df_grades[['RouteID','BMP','EMP',grade]]
        df_grade['DataItem'] = f'Grades_{grade}'
        df_grade['BeginDate'] = '01/01/2024'
        df_grade['ValueText'] = ''
        df_grade['ValueDate'] = ''
        df_grade['StateID'] = '54'
        df_grade['Comments'] = ''
        df_grade.rename(columns = {'E':'ValueNumeric'},inplace = True)
        df_grade = df_grade[df_grade['ValueNumeric'] != 0]
        df_grade.to_csv(fr'./curves_grades_out/45_Grades_{grade}.csv',sep = '|',index = False)
    elif grade == 'F':
        df_grade = df_grades[['RouteID','BMP','EMP',grade]]
        df_grade['DataItem'] = f'Grades_{grade}'
        df_grade['BeginDate'] = '01/01/2024'
        df_grade['ValueText'] = ''
        df_grade['ValueDate'] = ''
        df_grade['StateID'] = '54'
        df_grade['Comments'] = ''
        df_grade.rename(columns = {'F':'ValueNumeric'},inplace = True)
        df_grade = df_grade[df_grade['ValueNumeric'] != 0]
        df_grade.to_csv(fr'./curves_grades_out/45_Grades_{grade}.csv',sep = '|',index = False)
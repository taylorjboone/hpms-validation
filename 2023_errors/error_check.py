import geopandas as gpd
import pandas as pd
import os

cwd = os.getcwd()
fp = os.path.join(cwd, 'Non-Conformances_2023.gdb')

fgdb = gpd.read_file(fp)

validation_rules = sorted(fgdb['ValidationRule'].unique().tolist())

relevant_data_items = {
    'SJ-F-01': ['RouteId', 'BeginPoint', 'EndPoint', 'FsystemVn', 'FacilityTypeVn'],
    'SJ-F-02': ['RouteId', 'BeginPoint', 'EndPoint', 'UrbanIdVn', 'FsystemVn', 'FacilityTypeVn', 'DirThroughLanesVn', 'IriVn'],
    'SJ-F-03': ['RouteId', 'BeginPoint', 'EndPoint', 'FsystemVn', 'FacilityTypeVn'],
    'SJ-F-04': ['RouteId', 'BeginPoint', 'EndPoint', 'StructureTypeVn'],
    'SJ-F-05': ['RouteId', 'BeginPoint', 'EndPoint', 'AccessControlVn', 'FsystemVn', 'FacilityTypeVn'],
    'SJ-F-06': ['RouteId', 'BeginPoint', 'EndPoint', 'OwnershipVn', 'FsystemVn', 'FacilityTypeVn'],
    'SJ-F-07': ['RouteId', 'BeginPoint', 'EndPoint', 'ThroughLanesVn', 'FsystemVn', 'FacilityTypeVn'],
    'SJ-F-08': ['RouteId', 'BeginPoint', 'EndPoint', 'ManagedLanesTypeVn', 'ManagedLanesVn'],
    'SJ-F-09': ['RouteId', 'BeginPoint', 'EndPoint', 'ManagedLanesVn', 'ManagedLanesTypeVn'],
    'SJ-F-10': ['RouteId', 'BeginPoint', 'EndPoint', 'PeakLanesVn', 'SampleId'],
    'SJ-F-11': ['RouteId', 'BeginPoint', 'EndPoint', 'CounterPeakLanesVn', 'FacilityTypeVn', 'UrbanIdVn', 'ThroughLanesVn', 'SampleId'],
    'SJ-F-12': ['RouteId', 'BeginPoint', 'EndPoint', 'TurnLanesRVn', 'UrbanIdVn', 'AccessControlVn', 'SampleId'],
    'SJ-F-13': ['RouteId', 'BeginPoint', 'EndPoint', 'TurnLanesLVn', 'UrbanIdVn', 'AccessControlVn', 'SampleId'],
    'SJ-F-14': ['RouteId', 'BeginPoint', 'EndPoint', 'SpeedLimitVn', 'SampleId', 'NhsVn', 'SampleId'],
    'SJ-F-15': ['RouteId', 'BeginPoint', 'EndPoint', 'TollIdVn'],
    'SJ-F-16': ['RouteId', 'BeginPoint', 'EndPoint', 'RouteNumber', 'FsystemVn', 'NhsVn', 'FacilityTypeVn', 'RouteSigning', 'DirThroughLanesVn', 'IriVn'],
    'SJ-F-17': ['RouteId', 'BeginPoint', 'EndPoint', 'RouteSigning', 'FsystemVn', 'FacilityTypeVn'],
    'SJ-F-18': ['RouteId', 'BeginPoint', 'EndPoint', 'RouteQualifier', 'FsystemVn', 'FacilityTypeVn'],
    'SJ-F-19': ['RouteId', 'BeginPoint', 'EndPoint', 'RouteName', 'FsystemVn', 'FacilityTypeVn'],
    'SJ-F-20': ['RouteId', 'BeginPoint', 'EndPoint', 'AadtVn', 'FsystemVn', 'FacilityTypeVn', 'UrbanIdVn', 'NhsVn'],
    'SJ-F-21': ['RouteId', 'BeginPoint', 'EndPoint', 'AadtsingleUnitVn', 'FsystemVn', 'FacilityTypeVn', 'SampleId'],
    'SJ-F-22': ['RouteId', 'BeginPoint', 'EndPoint', 'PctdhsingleVn', 'SampleId'],
    'SJ-F-23': ['RouteId', 'BeginPoint', 'EndPoint', 'AadtcombinationVn', 'FsystemVn', 'FacilityTypeVn', 'SampleId'],
    'SJ-F-24': ['RouteId', 'BeginPoint', 'EndPoint', 'PctDhcombinationVn', 'SampleId'],
    'SJ-F-25': ['RouteId', 'BeginPoint', 'EndPoint', 'KfactorVn', 'SampleId'],
    'SJ-F-26': ['RouteId', 'BeginPoint', 'EndPoint', 'DirFactorVn', 'SampleId'],
    'SJ-F-27': ['RouteId', 'BeginPoint', 'EndPoint', 'FutureAadtVn', 'SampleId'],
    'SJ-F-28': ['RouteId', 'BeginPoint', 'EndPoint', 'SignalTypeVn', 'SampleId', 'UrbanIdVn', 'NumberSignalsVn'],
    'SJ-F-29': ['RouteId', 'BeginPoint', 'EndPoint', 'PctGreenTimeVn', 'SampleId', 'UrbanIdVn', 'NumberSignalsVn'],
    'SJ-F-30': ['RouteId', 'BeginPoint', 'EndPoint', 'NumberSignalsVn', 'SignalTypeVn', 'SampleId'],
    'SJ-F-31': ['RouteId', 'BeginPoint', 'EndPoint', 'StopSignsVn', 'SampleId'],
    'SJ-F-32': ['RouteId', 'BeginPoint', 'EndPoint', 'AtGradeOtherVn','SampleId'],
    'SJ-F-33': ['RouteId', 'BeginPoint', 'EndPoint', 'LaneWidthVn', 'SampleId'],
    'SJ-F-34': ['RouteId', 'BeginPoint', 'EndPoint', 'MedianTypeVn', 'SampleId'],
    'SJ-F-35': ['RouteId', 'BeginPoint', 'EndPoint', 'MedianWidthVn','MedianTypeVn', 'SampleId'],
    'SJ-F-36': ['RouteId', 'BeginPoint', 'EndPoint', 'ShoulderTypeVn', 'SampleId'],
    'SJ-F-37': ['RouteId', 'BeginPoint', 'EndPoint', 'ShoulderWidthRVn','ShoulderTypeVn', 'SampleId'],
    'SJ-F-38': ['RouteId', 'BeginPoint', 'EndPoint', 'ShoulderWidthLVn','MedianTypeVn','ShoulderTypeVn'],
    'SJ-F-39': ['RouteId', 'BeginPoint', 'EndPoint', 'PeakParkingVn', 'SampleId','UrbanIdVn'],
    'SJ-F-40': ['RouteId', 'BeginPoint', 'EndPoint', 'WideningPotentialVn', 'SampleId'],
    'SJ-F-41': ['RouteId', 'BeginPoint', 'EndPoint', 'CurvesAVn','CurvesBVn','CurvesCVn','CurvesDVn','CurvesEVn','CurvesFVn','FsystemVn','UrbanIdVn','SurfaceTypeVn','SampleId'],
    'SJ-F-42': ['RouteId', 'BeginPoint', 'EndPoint', 'CurvesAVn','CurvesBVn','CurvesCVn','CurvesDVn','CurvesEVn','CurvesFVn','FsystemVn','UrbanIdVn','SurfaceTypeVn','SampleId'],
    'SJ-F-43': ['RouteId', 'BeginPoint', 'EndPoint', 'CurvesAVn','CurvesBVn','CurvesCVn','CurvesDVn','CurvesEVn','CurvesFVn','FsystemVn','UrbanIdVn','SampleId'],
    'SJ-F-44': ['RouteId', 'BeginPoint', 'EndPoint', 'TerrarinTypeVn','FsystemVn','UrbanIdVn','SampleId'],
    'SJ-F-45': ['RouteId', 'BeginPoint', 'EndPoint', 'GradesAVn','GradesBVn','GradesCVn','GradesDVn','GradesEVn','GradesFVn','FsystemVn','UrbanIdVn','SurfaceTypeVn','SampleId'],
    'SJ-F-46': ['RouteId', 'BeginPoint', 'EndPoint', 'GradesAVn','GradesBVn','GradesCVn','GradesDVn','GradesEVn','GradesFVn','FsystemVn','UrbanIdVn','SurfaceTypeVn','SampleId'],
    'SJ-F-47': ['RouteId', 'BeginPoint', 'EndPoint', 'GradesAVn','GradesBVn','GradesCVn','GradesDVn','GradesEVn','GradesFVn','FsystemVn','UrbanIdVn','SampleId'],
    'SJ-F-48': ['RouteId', 'BeginPoint', 'EndPoint', 'PctPassSightVn','UrbanIdVn','SurfaceTypeVn','SampleId','MedianTypeVn','ThroughLanesVn'],
    'SJ-F-49': ['RouteId', 'BeginPoint', 'EndPoint', 'IriVn','SurfaceTypeVn','FacilityTypeVn','FsystemVn','NhsVn','UrbanIdVn','SampleId','DirThroughLanesVn','PsrVn'],
    'SJ-F-50': ['RouteId', 'BeginPoint', 'EndPoint', 'PsrVn','IriVn','SurfaceTypeVn','FacilityTypeVn','FsystemVn','UrbanIdVn','SampleId'],
    'SJ-F-51': ['RouteId', 'BeginPoint', 'EndPoint', 'SurfaceTypeVn','FacilityTypeVn','FsystemVn','NhsVn','DirThroughLanesVn','IriVn','PsrVn','SampleId'],
    'SJ-F-52': ['RouteId', 'BeginPoint', 'EndPoint', 'RuttingVn','SurfaceTypeVn','FacilityTypeVn','FsystemVn','NhsVn','SampleId','DirThroughLanesVn'],
    'SJ-F-53': ['RouteId', 'BeginPoint', 'EndPoint', 'FaultingVn','SurfaceTypeVn','FacilityTypeVn','FsystemVn','NhsVn','SampleId','DirThroughLanesVn','PsrVn','IriVn'],
    'SJ-F-54': ['RouteId', 'BeginPoint', 'EndPoint', 'CrackingPercentVn','FacilityTypeVn','FsystemVn','NhsVn','SampleId','DirThroughLanesVn','PsrVn','IriVn'],
    'SJ-F-55': ['RouteId', 'BeginPoint', 'EndPoint', 'YearLastImprovementVd','SampleId','SurfaceTypeVn','YearLastConstructionVd'],
    'SJ-F-56': ['RouteId', 'BeginPoint', 'EndPoint', 'YearLastConstructionVd','SampleId','SurfaceTypeVn'],
    'SJ-F-57': ['RouteId', 'BeginPoint', 'EndPoint', 'LastOverlayThicknessVn','SampleId','SurfaceTypeVn'],
    'SJ-F-58': ['RouteId', 'BeginPoint', 'EndPoint', 'ThicknessRigidVn','SampleId','SurfaceTypeVn'],
    'SJ-F-59': ['RouteId', 'BeginPoint', 'EndPoint', 'ThicknessFlexibleVn','SampleId','SurfaceTypeVn'],
    'SJ-F-60': ['RouteId', 'BeginPoint', 'EndPoint', 'BaseTypeVn','SampleId','SurfaceTypeVn'],
    'SJ-F-61': ['RouteId', 'BeginPoint', 'EndPoint', 'BaseThicknessVn','SampleId','SurfaceTypeVn'],
    'SJ-F-62': ['RouteId', 'BeginPoint', 'EndPoint', 'SoilTypeVn'],
    'SJ-F-63': ['RouteId', 'BeginPoint', 'EndPoint', 'CountyIdVn','FacilityTypeVn','FsystemVn','NhsVn','UrbanIdVn'],
    'SJ-F-64': ['RouteId', 'BeginPoint', 'EndPoint', 'NhsVn','FacilityTypeVn','FsystemVn'],
    'SJ-F-65': ['RouteId', 'BeginPoint', 'EndPoint', 'StrahnetTypeVn'],
    'SJ-F-66': ['RouteId', 'BeginPoint', 'EndPoint', 'NnVn'],
    'SJ-F-67': ['RouteId', 'BeginPoint', 'EndPoint', 'MaintenanceOperationsVn'],
    'SJ-F-68': ['RouteId', 'BeginPoint', 'EndPoint', 'DirThroughLanesVn'],

}


missing_samples = [
    '020090001052',
    '020090001084',
    '130450100000',
    '132190001416',
    '190290000126',
    '210330001759',
    '221190000540',
    '231190000267',
    '231190000359',
    '253100000967',
    '270350000853',
    '270350001664',
    '270350001785',
    '270350001798',
    '270350001835',
    '270350016410',
    '270350017413',
    '280520000000',
    '28052O000000',
    '311000100000',
    '317050000123',
    '317050000163',
    '360330003321',
    '540770018018',
    '540770018167',
]


counter = 0

for rule in validation_rules:
    if int(rule[-2:]) < 69:
        df = fgdb[fgdb['ValidationRule'] == rule]
        # df = pd.read_csv(cwd + f'\\23_full_spatial_errors-20230605T112949Z-001\\23_full_spatial_errors\\sjf{rule[-2:]}.csv')
        # rule_df = fgdb[fgdb['ValidationRule'] == rule]

        if 'SampleId' in df.columns.tolist():
            df = df[~df['SampleId'].astype('string').isin(missing_samples)]

        counter += len(df)


        print(rule)
        print(df[relevant_data_items[rule]])
        print('\n', '\n')

        df[relevant_data_items[rule]].to_csv(f'{rule}_errors.csv')


print(f'total errors: {counter}')

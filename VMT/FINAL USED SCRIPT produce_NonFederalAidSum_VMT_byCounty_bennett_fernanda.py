from numpy import average
import pandas as pd

county = {
    "Barbour": 1,
    "Berkeley": 2,
    "Boone": 3,
    "Braxton": 4,
    "Brooke": 5,
    "Cabell": 6,
    "Calhoun": 7,
    "Clay": 8,
    "Doddridge": 9,
    "Fayette": 10,
    "Gilmer": 11,
    "Grant": 12,
    "Greenbrier": 13,
    "Hampshire": 14,
    "Hancock": 15,
    "Hardy": 16,
    "Harrison": 17,
    "Jackson": 18,
    "Jefferson": 19,
    "Kanawha": 20,
    "Lewis": 21,
    "lincoln": 22,
    "Logan": 23,
    "McDowell": 24,
    "Marion": 25,
    "Marshall": 26,
    "Mason": 27,
    "Mercer": 28,
    "Mineral": 29,
    "Mingo": 30,
    "Monongalia": 31,
    "Monroe": 32,
    "Morgan": 33,
    "Nicholas": 34,
    "Ohio": 35,
    "Pendleton": 36,
    "Pleasants": 37,
    "Pocahontas": 38,
    "Preston": 39,
    "Putnam": 40,
    "Raleigh": 41,
    "Randolph": 42,
    "Ritchie": 43,
    "Roane": 44,
    "Summers": 45,
    "Taylor": 46,
    "Tucker": 47,
    "Tyler": 48,
    "Upshur": 49,
    "Wayne": 50,
    "Webster": 51,
    "Wetzel": 52,
    "Wirt": 53,
    "Wood": 54,
    "Wyoming": 55,
}


def get_f_system(value):
    if value in [1, 11]:
        return int(1)
    if value in [4, 12]:
        return int(2)
    if value in [2, 14]:
        return int(3)
    if value in [6, 16]:
        return int(4)
    if value in [7, 17]:
        return int(5)
    if value in [8, 18]:
        return int(6)
    if value in [9, 19]:
        return int(7)


result = []

### HPMS v9 ----
def write_result(row):
    return {
        "BeginDate": "12/31/2021",
        "StateID": 54,
        "FSystem": int(row["F_System"]),
        "UrbanID": str(row["Urban_Code"]).zfill(5),
        "VMT": int(row["Local_VMT"]),
        "Comments": "",
    }


def filter_input(df):
    df = df[df["Urban_Code"] != ""]

    # filter out the surface type 1
    df = df[df["SURFACE_TYPE"] != ""]
    df = df[df["SURFACE_TYPE"].astype(float).astype(int) > 1]

    df = df[df["NAT_FUNCTIONAL_CLASS"] != ""]
    df = df[df["NAT_FUNCTIONAL_CLASS"].astype(float).astype(int).isin([8, 9, 19])]
    df["F_System"] = df.apply(lambda x: get_f_system(x["NAT_FUNCTIONAL_CLASS"]), axis=1)
    return df


def calc_VMT_AADT_2021(df):
    df = df[df["AADT_dataitem"] != ""]
    print(df.columns)
    # Calculate the length
    df["Length"] = df["End_Point"] - df["Begin_Point"]
    df["VMT"] = df["Length"] * df["AADT_dataitem"]

    # ### Calculate the Average
    urban_codes = df["Urban_Code"].unique()

    average = []
    for u in urban_codes:
        df_filtered = df[df["Urban_Code"] == u]
        average.append([int(u), round(df_filtered["AADT_dataitem"].mean(), 3)])

    df = df.groupby(["County_Code","Urban_Code", "F_System"]).agg({"Length": "sum", "VMT": "sum"})
    df = df.reset_index()

    # Calculate teh average
    df["AverageVMT"] = df["VMT"] / df["Length"]

    df["StateUrbanizedAreaVMT"] = df["VMT"]
    return df

def calc_average_byUrban_byFsystem(df):
    df = df[df["AADT_dataitem"] != ""]
    print(df.columns)
    # Calculate the length
    df["Length"] = df["End_Point"] - df["Begin_Point"]
    df["VMT"] = df["Length"] * df["AADT_dataitem"]

    # ### Calculate the Average
    urban_codes = df["Urban_Code"].unique()

    average = []
    for u in urban_codes:
        df_filtered = df[df["Urban_Code"] == u]
        average.append([int(u), round(df_filtered["AADT_dataitem"].mean(), 3)])

    df = df.groupby(["Urban_Code", "F_System"]).agg({"Length": "sum", "VMT": "sum"})
    df = df.reset_index()

    # Calculate teh average
    df["AverageVMT"] = df["VMT"] / df["Length"]

    df["StateUrbanizedAreaVMT"] = df["VMT"]
    return df


def federalRoads():
    # Read Fed_Roads_Summary.csv
    # df_fed_roads = pd.read_excel("Fed_Roads_Summary.xlsx", dtype={"Urban_Code": int})
    df_fed_roads = pd.read_csv("Fed_Roads_Summary.csv", dtype={"Urban_Code": int})
    df_fed_roads['County_Code'] = (df_fed_roads['County_Code'].astype(int)  /2).astype(int) + 1
    df_fed_roads = df_fed_roads[df_fed_roads["Urban_Code"] != ""]
    
    df_fed_grouped = (
        df_fed_roads.groupby(["County_Code","Urban_Code", "F_System"])["Section_Length"]
        .sum()
        .reset_index()
        .set_index("Urban_Code")
    )
    return df_fed_grouped


def cityMileage():
    # Read City_NonFedAid_Summary.xlsx
    df_city_nonFedAid = pd.read_excel(
        "City_NonFedAid_Summary.xlsx", dtype={"Urban_Code": int}
    )
    df_city_nonFedAid = df_city_nonFedAid[df_city_nonFedAid["Urban_Code"] != ""]
    df_city_nonFedAid['County_Code'] = df_city_nonFedAid.County_Name.map(lambda x: county.get(x))

    df_city_grouped = (
        df_city_nonFedAid.groupby(["County_Code","Urban_Code", "F_System"])["Section_Length"]
        .sum()
        .reset_index()
        .set_index("Urban_Code")
    )
    print(df_city_grouped[df_city_grouped['County_Code'] == 37])
    return df_city_grouped


# df = pd.read_excel('_joined_Urban_Summaries.xlsx', dtype={"URBAN_CODE":str}).fillna("")
df = pd.read_excel(
    "_NEW_joined_for_Mainframe.xlsx", dtype={"URBAN_CODE": str}
).fillna("")

df.rename(columns={"URBAN_CODE": "Urban_Code"}, inplace=True)
df = df[df["Urban_Code"] != ""]
df["Urban_Code"] = df["Urban_Code"].astype(int)
df = filter_input(df)
df['County_Code'] = df['Route_ID'].str.slice(0,2).astype(int)

df_fed_grouped = federalRoads()
df_city_grouped = cityMileage()

# df_vmt_average = calc_average_byUrban_byFsystem(df)

df_average = calc_VMT_AADT_2021(df)

print(df_average)


df_average = pd.merge(
    df_average,
    df_fed_grouped,
    how="outer",
    left_on=["County_Code","Urban_Code", "F_System"],
    right_on=["County_Code","Urban_Code", "F_System"],
).fillna(0)
df_average = pd.merge(
    df_average,
    df_city_grouped,
    how="outer",
    left_on=["County_Code","Urban_Code", "F_System"],
    right_on=["County_Code","Urban_Code", "F_System"],
).fillna(0)
df_average.rename(
    columns={
        "Section_Length_x": "UrbanizedFedRoadLength",
        "Section_Length_y": "UrbanizedMunicipalRoadLength",
    },
    inplace=True,
)
print(df_average)
df_average['FedUrbanizedAreaVMT'] = round(df_average['UrbanizedFedRoadLength'] * df_average['AverageVMT'],3)
df_average['MunicipalUrbanizedAreaVMT'] = round(df_average['UrbanizedMunicipalRoadLength'] * df_average['AverageVMT'],3)
df_average['Local_VMT'] = round(df_average['StateUrbanizedAreaVMT'] + df_average['FedUrbanizedAreaVMT'] + df_average['MunicipalUrbanizedAreaVMT'],0)

df_average.to_excel('City_Fed_State_Mileage_byCounty_UrbanCode_Fsystem.xlsx', index=False)
print(df_average.Local_VMT.sum())

    # df_average["FedUrbanizedAreaVMT"] = round(
    #     df_average["UrbanizedFedRoadLength"] * df_average["AverageAADT"], 3
    # )
    # df_average["MunicipalUrbanizedAreaVMT"] = round(
    #     df_average["UrbanizedMunicipalRoadLength"] * df_average["AverageAADT"], 3
    # )
    # df_average["Local_VMT"] = round(
    #     df_average["StateUrbanizedAreaVMT"]
    #     + df_average["FedUrbanizedAreaVMT"]
    #     + df_average["MunicipalUrbanizedAreaVMT"],
    #     0,
    # )

    # print(df_average)

    # for index, row in df_average.iterrows():
    #     result.append(write_result(row))


# df_result.to_csv("VMT_byCounty.csv", index=False)

print("Finished!")

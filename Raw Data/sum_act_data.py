import pandas as pd
import numpy as np
pd.set_option('display.max_columns', None)

##act actual
def import_act_actual():
    greenpower_path = 'ACT_Actual_Data_Spring_22/GreenPower Motor Company, 2021 MY vehicles, ACT 3-31-22.csv'
    greenpower_df = pd.read_csv(greenpower_path)
    greenpower_df.insert(1, 'OEM', 'Greenpower')

    lightning_path = 'ACT_Actual_Data_Spring_22/Lightning eMotors ProductionReport.xls'
    lightning_df = pd.read_excel(lightning_path, skiprows = 29)
    lightning_df.rename(columns={"#VIN":"VIN"},inplace=True)
    lightning_df.insert(1, 'OEM', 'Lignting')

    lion_path = 'ACT_Actual_Data_Spring_22/Lion Electric Co Production Report.csv'
    lion_df = pd.read_csv(lion_path, skiprows = 29)
    lion_df.rename(columns={"#VIN":"VIN"},inplace=True)
    lion_df.insert(1, 'OEM', 'Lion')

    navistar_path = 'ACT_Actual_Data_Spring_22/Navistar.csv'
    navistar_df = pd.read_csv(navistar_path)
    navistar_df.rename(columns={"#VIN":"VIN"},inplace=True)
    navistar_df.insert(1, 'OEM', 'Navistar')

    newflyer_path = 'ACT_Actual_Data_Spring_22/New Flyer - MCI MMCH2_COMMON_CR9CACT_.xlsx'
    newflyer_df = pd.read_excel(newflyer_path, skiprows = 29)
    newflyer_df.rename(columns={"#VIN":"VIN"},inplace=True)
    newflyer_df.insert(1, 'OEM', 'New Flyer')

    newflyer2_path = 'ACT_Actual_Data_Spring_22/New Flyer - MCI MNFA2_COMMON_CR9CACT_.xlsx'
    newflyer2_df = pd.read_excel(newflyer2_path, skiprows = 29)
    newflyer2_df.rename(columns={"#VIN":"VIN"},inplace=True)
    newflyer2_df.insert(1, 'OEM', 'New Flyer 2')

    nissan_path = 'ACT_Actual_Data_Spring_22/Nissan.csv'
    nissan_df = pd.read_csv(nissan_path)
    nissan_df.rename(columns={"#VIN":"VIN"},inplace=True)
    nissan_df.insert(1, 'OEM', 'Nissan')

    paccar_path = 'ACT_Actual_Data_Spring_22/PACCAR MPCR2_COMMON_CR9_ACT_.csv'
    paccar_df = pd.read_csv(paccar_path, skiprows = 24)
    paccar_df.rename(columns={"#VIN":"VIN"},inplace=True)
    paccar_df.insert(1, 'OEM', 'Paccar')

    volvo_path = 'ACT_Actual_Data_Spring_22/Volvo MVPT2_ACT_INDIVIDUAL_PR.csv'
    volvo_df = pd.read_csv(volvo_path, skiprows = 29)
    volvo_df.rename(columns={"#VIN":"VIN"},inplace=True)
    volvo_df.insert(1, 'OEM', 'Volvo')

    total_df = pd.concat([greenpower_df, lightning_df, lion_df, navistar_df, newflyer_df, newflyer2_df, nissan_df, paccar_df, volvo_df])
    print(total_df.tail())
    return total_df
##ihs -> only 3 and 4 are MHD: filter only BEV and Fuel Cell: class is above 2b

##hvip vouchers
def import_hvip():
    path = "HVIPVouchers.csv"
    df = pd.read_csv(path, encoding = "unicode_escape")
    print(df.head())

if __name__ == "__main__":
    #hvip_data = import_hvip()
    act_data = import_act_actual()
    act_data.to_csv('all_act_data.csv')

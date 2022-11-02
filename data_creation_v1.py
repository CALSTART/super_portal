import pandas as pd
import numpy as np
pd.set_option('display.max_columns', 500)
pd.options.display.max_rows = 999

vehicle_type_categories = []
#gvwr_ranges = [10,001-14,000, 14001-19500, 19501-26000,26001-33000, '33001 and up']
#1 data source
### Add Segment categories of Refuse, Drayage (if possible), change MD to MD Truck
### Add GVWR to all rows
### Chase on 10/13: revisit segment classification based on GVWR *AND* Segment for all of them
### if gvwr is bigger than a value, then it's heavy duty
### add vehicle class as a column
def hvip():
    data = pd.read_csv('hvip_data.csv', encoding='cp1252', low_memory=False)
    data.rename(columns={"Drivetrain": "Fuel Type", "Air.District":"Air District", "DAC":"DAC?", "FleetAddress1":"Address", "ActualDeliveryDate": "Date Delivered", "lat": "Latitude", "lon": "Longitude", "Amount":"Funding Total", "Pur_Company":"Entity", "GrossVehicleWeight":"GVWR"}, inplace=True)
    data['Source'] = 'HVIP'
    data['VIN'] = data['VIN'].astype(str)
    data = data.astype({'VocationalUseOther': 'string'})
    data['Date Delivered'] = pd.to_datetime(data['Date Delivered'],errors='ignore')
    #Add HFC in Fuel Type Column
    data.loc[data['EngineType'].str.contains('Hydrogen') == True, 'Fuel Type'] = 'HFC'
    #drop all payment status na
    data.dropna(subset=['PaymentDate'], inplace=True)


    #filter out HV from Fuel Type
    data = data[data['Fuel Type'] != 'HV']
    data = data[data['Fuel Type'] != 'EVSE']
    data = data[data['Fuel Type'] != 'ePTO']
    data = data[data['Fuel Type'] != 'Natural Gas']
    #standardize DAC? column
    data['DAC?'] = data['DAC?'].astype(str)
    data.loc[data['DAC?'] == 'True', 'DAC?'] = 'Yes'
    data.loc[data['DAC?'] == 'False', 'DAC?'] = 'No'
    #remove Delivery Date N/A
    data = data[data['Date Delivered'] != 'NA']
    data = data[data['Date Delivered'].notnull()]

    #remove bad VINs
    data = data[data['VIN'] != '0']
    data = data[data['VIN'].notnull()]

    #Add Address
    data['Address'] = data['Address'].astype(str) + " " + data['FleetCity'].astype(str) + ", " + "CA " + data['FleetZipCode'].astype(str)
    #remove county suffix
    data = remove_county_suffix(data)

    #adding in segment information
    gvwr_list = data['GVWR'].to_list()
    segment_list = data['Segment'].to_list()
    updated_segment_list = []
    h_d_truck_list = ['Truck - Tractor', 'Tractor', 'Delivery Truck', 'Truck - Straight', 'Truck - Box', 'Parcel Delivery']
    m_d_segment_list = ['Delivery Truck', 'Truck - Straight', 'Truck - Box', 'Parcel Delivery', 'Truck - Medium Duty', 'MH']
    error_list = ['Truck - Medium Duty', 'MH', 'Step Van', 'Cargo Van', 'Panel/ Step/ Cargo Van']
    m_d_error_list = ['Terminal Tractor', 'HH', 'Tractor', 'Truck - Tractor']
    m_d_step_van_list = ['Motiv Power System EPIC F-59 Battery Electric Step Van, Shuttle Bus, Truck', 'Xos SV Battery Electric Vehicle', 'SEA FORD F-59 EV', 'SEA F59 EV', 'Xos CV Battery Electric Vehicle', 'FCCC MT50e Battery Electric Truck']
    m_d_truck_list = ['GreenPower EV Star CarGo Plus', 'Lightning FE4-129 Battery Electric Truck', 'SEA FORD F-450 EV', 'SEA HINO M5 EV']
    m_d_shuttle_bus = ['Ford T350 Van 2WD']
    m_d_cargo_vans = ['Lightning FT3-86 Battery Electric Vehicle', 'Lightning FT3-43 Battery Electric Bus 22', 'Lightning FT3-43 Battery Electric Truck (Cargo and Bus)']
    for index, row in data.iterrows():
        if (row['GVWR'] == '26,001 - 33,000') or (row['GVWR'] == '33,001 and up'):
            if row['Segment'] in h_d_truck_list:
                data.loc[index, 'Segment'] = 'HD Truck'
            elif (row['Segment'] == 'Utility Truck') or (row['Segment'] == 'Specialty Vehicle'):
                data.loc[index, 'Segment'] = 'Other'
            elif row['Segment'] in error_list:
                data.loc[index, 'Segment'] = 'ERROR'
            elif row['Segment'] == 'Coach':
                data.loc[index, 'Segment'] = 'Shuttle bus'
            else:
                continue
        else:
            if row['Segment'] in m_d_segment_list:
                data.loc[index, 'Segment'] = 'MD Truck'
            elif row['Segment'] in m_d_error_list:
                data.loc[index, 'Segment'] = 'ERROR'
            elif (row['Segment'] == ['Step Van']) or (row['Segment'] == 'Parcel Delivery'):
                data.loc[index, 'Segment'] = 'MD Step Van'
            elif (row['Segment'] == 'Panel/ Step/ Cargo Van') and (row['Description'] in m_d_step_van_list):
                data.loc[index, 'Segment'] = 'MD Step Van'
            elif (row['Segment'] == 'Panel/ Step/ Cargo Van') and (row['Description'] in m_d_truck_list):
                data.loc[index, 'Segment'] = 'MD Truck'
            elif (row['Segment'] == 'Panel/ Step/ Cargo Van') and (row['Description'] in m_d_shuttle_bus):
                data.loc[index, 'Segment'] = 'Shuttle Bus'
            elif (row['Segment'] == 'Panel/ Step/ Cargo Van') and (row['Description'] in m_d_cargo_vans):
                data.loc[index, 'Segment'] = 'Cargo Van'
            elif row['Segment'] == 'Coach':
                data.loc[index, 'Segment'] = 'Shuttle bus'
            else:
                continue
    # string match for drayage and add to segment column
    #x = data[data['VocationalUseOther'].str.contains('Drayage') == True]
    #print('drayage df',x)
    data.loc[data['VocationalUseOther'].str.contains('drayage') == True, 'Segment'] = 'Drayage'
    #print("counts ", data['VocationalUseOther'].str.contains('drayage', case=False).sum())

    #clean segment
    data = standardize_segment(data)
    #remove suffix
    data = remove_ad_suffix(data)
    data= data[['Source', 'VIN','Entity','Date Delivered','Manufacturer','Segment', 'Fuel Type', 'GVWR', 'Funding Total', 'Air District', 'County', 'DAC?', 'Address', 'Latitude', 'Longitude']]
    return data

#2 data source
def vw_settlement():
    data = pd.read_excel('vw_bus.xlsx')
    data.rename(columns={"NewVIN": "VIN", "NewVehicleType":"Segment", "NewFuel":"Fuel Type", "NewEquipmentMake":"Manufacturer", 'Executed': 'Date Delivered', "Equipment County":"County", "DAC":"DAC?", "TransactionAmt":"Funding Total", "Name1":"Entity", "NewGVWR":"GVWR"}, inplace=True)
    data['Source'] = 'VW Settlement'
    data['Air District'] = " "
    #data['Fuel Type'] = data['Fuel Type'].str.title()
    data['Address'] = data['EquipmentAddress'].astype(str) + " " + data['Equipment City'].astype(str) + "," + " CA " + data['Equipment Zip'].astype(str)
    #drop purchase completion date + drop null vins
    data.dropna(subset=['VIN','PurchaseOrCompletionDate'], inplace=True)
    #drop null vins
    #data = data[data['VIN'].notnull()]

    #Standardize DAC Status
    data.loc[data['DAC?'] == 'None', 'DAC?'] = 'No'
    data.loc[data['DAC?'] == 'Low Income', 'DAC?'] = 'No'
    data.loc[data['DAC?'] == 'DAC & Low Income', 'DAC?'] = 'Yes'
    data.loc[data['DAC?'] == 'DAC', 'DAC?'] = 'Yes'
    data['Fuel Type'] = data['Fuel Type'].str.replace('ELECTRIC', 'EV')
    data['Fuel Type'] = data['Fuel Type'].str.replace('Zev', 'EV')

    #get county information
    vw_zips = data['Equipment Zip'].to_list()
    data = get_air_district_county(vw_zips, data)
    #standardize gvwr
    data = standardize_gvwr(data)
    #clean segment
    data = standardize_segment(data)
    #remove suffix
    data = remove_ad_suffix(data)
    data= data[['Source', 'VIN','Entity','Date Delivered','Manufacturer','Segment', 'Fuel Type', 'GVWR', 'Funding Total', 'Air District', 'County', 'DAC?', 'Address', 'Latitude', 'Longitude']]
    return data

#3 data source
def cap_moyer():
    #why are there missing vins
    data = pd.read_csv('cap_moyer_v2.0.csv')
    data.rename(columns={"Type": "Segment", "First Main Engine Fuel":"Engine Type", "Funding Source":"Source", "EquipmentAddress":"Address", "Funding Source":"Source", "Applicant Name":"Entity", "Contract Execution":"Date Delivered", "Grant Amount":"Funding Total", "Make":"Manufacturer"}, inplace=True)

    #filter out HVIP co-funded buses
    data = data[data['HVIP co-funding'] != 'HVIP - Hybrid and Zero-Emission Truck and Bus Voucher Incentive Project']
    #drop all non school buses
    data = data[data['Segment'] == 'SB']
    data.dropna(subset=['VIN'], inplace=True)

    data['County'] = ""
    data['GVWR'] = 'Not Available'
    data['DAC?'] = ""
    data['Address'] = ""
    data['Fuel Type'] = "EV"
    data['Latitude'] = "To be added"
    data['Longitude'] = "To be added"
    #standardize gvwr
    data = standardize_gvwr(data)
    #clean segment
    data = standardize_segment(data)
    #remove suffix
    data = remove_ad_suffix(data)
    data= data[['Source', 'VIN','Entity','Date Delivered','Manufacturer','Segment', 'Fuel Type', 'GVWR', 'Funding Total', 'Air District', 'County', 'DAC?', 'Address', 'Latitude', 'Longitude']]
    return data

#4 data source
def ict():
    data = pd.read_csv('ict_bus_inventory.csv',encoding='cp1252')
    data.rename(columns={"Transit Agency Name": "Entity", "Date In-Service":"Date Delivered", "Make":"Manufacturer"}, inplace=True)
    data['Source'] = 'ICT'
    #dispatch location zip code
    #what is segment?
    data = data[(data['Fuel Type'] == 'Electricity') | (data['Fuel Type'] == 'Hydrogen (Fuel Cell)')]
    data.loc[data['Fuel Type'] == 'Electricity', 'Fuel Type'] = 'Electric'
    #change model year to date
    data['Date Delivered'] = np.where(data['Date Delivered'] == '-', data['Manufacture Year'], data['Date Delivered'])
    data['GVWR'] =''
    data['Funding Total'] = ""
    data['Segment'] = "Transit Bus"
    data['Address'] = ""
    data['DAC?'] = ""
    data['Latitude'] = ""
    data['Longitude'] = ""
    ict_zips = data['Dispatch Location Zip Code'].to_list()
    ict_zips_int = [int(item) for item in ict_zips]
    #print("ICT Zip:", ict_zips[0])
    data = get_air_district_county(ict_zips_int, data)
    data['Air District'] = data['Air District'].str.title()

    #print("Data AD", data['Air District'])
    data= data[['Source', 'VIN','Entity','Date Delivered','Manufacturer','Segment', 'Fuel Type', 'GVWR', 'Funding Total', 'Air District', 'County', 'DAC?', 'Address', 'Latitude', 'Longitude']]
    return data

#5 - data source: rsbpp
def rsbpp_buses():
    #remove non-electric buses
    rsbpp_1 = pd.read_csv('rsbpp_1.csv')
    rsbpp_1.rename(columns={"Applicant": "Entity", "Delivery Date":"Date Delivered", "New Bus Manufacturer":"Manufacturer", "New Bus VIN":"VIN", "New Bus Fuel Type":"Fuel Type", "Allowable Costs $":"Funding Total", "Local Air District":"Air District", "New Bus GVWR":"GVWR"}, inplace=True)
    rsbpp_1 = rsbpp_1[rsbpp_1['VIN'] != '']
    rsbpp_1['Source'] = 'Rural School Bus Pilot Project'
    rsbpp_1['Segment'] = 'School Bus'
    rsbpp_1['Address'] = rsbpp_1['Physical Street'].astype(str) + " "+ rsbpp_1['Physical City'].astype(str) + ", " + "CA " + rsbpp_1['Physical Zip'].astype(str)
    rsbpp_1 = rsbpp_1[rsbpp_1['Fuel Type'] == 'Electric']
    rsbpp_1['County'] = ''
    rsbpp_1['DAC?'] = ''
    rsbpp_1['Latitude'] = ''
    rsbpp_1['Longitude'] = ''
    #rsbpp_1['GVWR'] =''

    rsbpp_1= rsbpp_1[['Source', 'VIN','Entity','Date Delivered','Manufacturer','Segment', 'Fuel Type', 'GVWR', 'Funding Total', 'Air District', 'County', 'DAC?', 'Address', 'Latitude', 'Longitude']]

    rsbpp_2 = pd.read_csv('RSBPP_2.csv')
    rsbpp_2.rename(columns={"Applicant": "Entity", "New Bus Delivery Date":"Date Delivered", "New Bus Manufacturer":"Manufacturer", "New Bus VIN":"VIN", "Technology Type":"Fuel Type", "Total Project Cost":"Funding Total", "Local Air District":"Air District", "New Bus GVWR":"GVWR"}, inplace=True)
    rsbpp_2 = rsbpp_2[rsbpp_2['VIN'] != '']
    rsbpp_2['Fuel Type'] = "ZEV"
    rsbpp_2['Source'] = 'Rural School Bus Pilot Project'
    rsbpp_2['Address'] = rsbpp_2['Bus Storage Street'].astype(str) + " " + rsbpp_2['Bus Storage City'].astype(str) + ", " + "CA " + rsbpp_2['Bus Storage Zip'].astype(str)
    rsbpp_2['Segment'] = 'School Bus'
    rsbpp_2['County'] = ''
    rsbpp_2['Latitude'] = ''
    rsbpp_2['Longitude'] = ''
    #rsbpp_2['GVWR'] =''
    rsbpp_2= rsbpp_2[['Source', 'VIN','Entity','Date Delivered','Manufacturer','Segment', 'Fuel Type', 'GVWR', 'Funding Total', 'Air District', 'County', 'DAC?', 'Address', 'Latitude', 'Longitude']]

    rsbpp_3 = pd.read_csv('rsbpp_3.csv')
    rsbpp_3.rename(columns={"School Fleet Owner": "Entity", "New Bus Delivery Date":"Date Delivered", "New Bus Manufacturer":"Manufacturer", "New Bus VIN":"VIN", "Technology Type":"Fuel Type", "Total Project Funding":"Funding Total", "New Bus GVWR":"GVWR"}, inplace=True)
    rsbpp_3 = rsbpp_3[rsbpp_3['VIN'] != '']
    rsbpp_3['Address'] = rsbpp_3['Physical Street'].astype(str) + " " +  rsbpp_3['Physical City'].astype(str) + ", " + "CA " + rsbpp_3['Physical Zip'].astype(str)
    rsbpp_3['Source'] = 'Rural School Bus Pilot Project'

    rsbpp_3['Fuel Type'] = "ZEV"
    rsbpp_3['Source'] = 'Rural School Bus Pilot Project'
    rsbpp_3['Segment'] = 'School Bus'
    rsbpp_3['County'] = ''
    rsbpp_3['Latitude'] = ''
    rsbpp_3['Longitude'] = ''
    #rsbpp_3['GVWR'] =''

    list_of_zips = rsbpp_3['Bus Storage Zip'].to_list()
    rsbpp_3 = get_air_district_county(list_of_zips, rsbpp_3)

    #rsbpp_3 = remove_ad_suffix(rsbpp_3)
    rsbpp_3= rsbpp_3[['Source', 'VIN','Entity','Date Delivered','Manufacturer','Segment', 'Fuel Type', 'GVWR', 'Funding Total', 'Air District', 'County', 'DAC?', 'Address', 'Latitude', 'Longitude']]
    data = pd.concat([rsbpp_1, rsbpp_2, rsbpp_3])


    #change ELECTRIC to EV
    data['Fuel Type'] = data['Fuel Type'].str.title()
    data['Fuel Type'] = data['Fuel Type'].str.replace('Electric', 'EV')
    data['Fuel Type'] = data['Fuel Type'].str.replace('Zev', 'EV')

    #standardize gvwr
    data = standardize_gvwr(data)
    #clean segment
    data = standardize_segment(data)


    return data

#6 - data source: sacramento
def sacramento():
    sac_df = pd.read_csv('sacramento_bus.csv')
    sac_df.rename(columns={"Recipient School District": "Entity", "Year":"Date Delivered", "School Bus Manufacturer":"Manufacturer", "Grant amount total":"Funding Total", "Address/ Zip Code":"Address"}, inplace=True)
    sac_df['Source'] = 'Sacramento Regional ZE School Bus Deployment Project'
    sac_df['Fuel Type'] = 'EV'
    sac_df['Date Delivered'] = '1/1/' + sac_df['Date Delivered'].apply(str)
    sac_df['Segment'] = 'School Bus'
    #sac_df['Address'] = sac_df['Physical Street'].astype(str) + " "+ rsbpp_1['Physical City'].astype(str) + ", " + "CA " + rsbpp_1['Physical Zip'].astype(str)
    #sac_df = rsbpp_1[rsbpp_1['Fuel Type'] == 'Electric']
    sac_df['Air District'] = 'Sacramento Metropolitan'
    sac_df['County'] = 'Sacramento'
    sac_df['DAC?'] = 'Yes'
    sac_df['Latitude'] = ''
    sac_df['Longitude'] = ''
    for index, row in sac_df.iterrows():
        if (row['GVWR'] >= '14,001 - 16,000#'):
            sac_df.loc[index, 'GVWR'] = '14,001 - 19,500'
        elif (row['GVWR'] == '33,000# +'):
            sac_df.loc[index, 'GVWR'] = '33,001 and up'
    #clean segment
    sac_df = standardize_segment(sac_df)
    sac_df= sac_df[['Source', 'VIN','Entity','Date Delivered','Manufacturer','Segment', 'Fuel Type', 'GVWR', 'Funding Total', 'Air District', 'County', 'DAC?', 'Address', 'Latitude', 'Longitude']]
    return sac_df

#6 - CMIS
def cmis():
    data = pd.read_csv('CMIS.csv')
    data.rename(columns={"School fleet owner": "Entity", "New Bus Manufacturer":"Manufacturer","New Bus CHP Cert Date":"Date Delivered", "Total Project Funding":"Funding Total", "New Bus VIN":"VIN", "Local Air District":"Air District", "New Bus GVWR":"GVWR"}, inplace=True)
    data['Source'] = 'CMIS'
    data['Fuel Type'] = 'ZEV'
    data['Segment'] = 'School Bus'
    data['Address'] = data['Bus Storage Street'].astype(str) + " " + data['Bus Storage City'].astype(str) + ", " + "CA " + data['Bus Storage Zip'].astype(str)
    data['County'] = 'Sacramento'
    data['DAC?'] = 'Yes'
    data['Latitude'] = ''
    data['Longitude'] = ''
    #cmis_df['GVWR'] = ''
    list_of_zips = data['Bus Storage Zip'].to_list()
    data = get_air_district_county(list_of_zips, data)
    #standardize gvwr
    data = standardize_gvwr(data)
    #clean segment
    data = standardize_segment(data)
    #remove suffix
    data = remove_ad_suffix(data)
    data= data[['Source', 'VIN','Entity','Date Delivered','Manufacturer','Segment', 'Fuel Type', 'GVWR', 'Funding Total', 'Air District', 'County', 'DAC?', 'Address', 'Latitude', 'Longitude']]

    return data

# #7 LCT Demo
# def infoshed():
#     vchar = pd.read_csv('infoshed_vchar.csv')
#     basic_data = pd.read_csv('infoshed_basic.csv')
#     joined_data = vchar.join(basic_data, lsuffix='_caller', rsuffix='_other')
#     print(joined_data.head(1))
#     #print("length of df:", len(joined_data))
#     joined_data.rename(columns={"manufacturer":"Manufacturer","model_year":"Date Delivered", "vehicle_type":"Segment", "drivetrain_type":"Fuel Type"}, inplace=True)
#     #entity will come from a join
#     #vin will come from a join
#     #funding Total
#     #zip code
#     #lct_demo = lct_demo[['Source', 'VIN','Entity','Date Delivered','Manufacturer','Segment', 'Fuel Type', 'Funding Total', 'Air District', 'County', 'DAC?', 'Address', 'Latitude', 'Longitude']]
#     #return lct_demo

def prop1b():
    prop1b = pd.read_csv('prop1b.csv')
    prop1b.rename(columns={"Equipment Owner Name": "Entity", "New Engine Make":"Manufacturer","New Engine Model Year":"Date Delivered", "New GVWR":"GVWR"}, inplace=True)
    list_of_zips = prop1b['Zip Code'].to_list()
    prop1b['Source'] = 'Prop 1B'
    prop1b['Fuel Type'] = 'ZEV'
    prop1b['VIN'] = ''
    prop1b['Funding Total'] = ''
    prop1b['Segment'] = 'MD'
    prop1b['Address'] = prop1b['Address'].astype(str) + " " + prop1b['City'].astype(str) + ", " + "CA " + prop1b['Zip Code'].astype(str)
    prop1b['DAC?'] = ''
    prop1b['Latitude'] = ''
    prop1b['Longitude'] = ''
    prop1b['Date Delivered'] = '1/1/' + prop1b['Date Delivered'].apply(str)
    #get air district
    prop1b = get_air_district_county(list_of_zips, prop1b)
    #standardize gvwr
    prop1b = standardize_gvwr(prop1b)
    #clean segment
    prop1b = standardize_segment(prop1b)
    #remove suffix
    prop1b = remove_ad_suffix(prop1b)

    prop1b= prop1b[['Source', 'VIN','Entity','Date Delivered','Manufacturer','Segment', 'Fuel Type', 'GVWR', 'Funding Total', 'Air District', 'County', 'DAC?', 'Address', 'Latitude', 'Longitude']]

    return prop1b

def get_air_district_county(list_of_zips, data):
    #print("first zip", list_of_zips[0])
    file_path = "Active-Zip-District-County-data.xlsx"
    air_df = pd.read_excel(file_path)
    zip_dict = pd.Series(air_df.AirDistrict.values,index=air_df.ZipCode).to_dict()
    #getting air district
    air_dist_list = []
    for i in list_of_zips:
        #print(i)
        #print(type(i))
        #add a check to see if zip exists
        if i != 0:
            air_dist_value = zip_dict.get(i)
            #print("First Air Dist Value", air_dist_value)
            air_dist_list.append(air_dist_value)
        else:
            air_dist_list.append('None')

    data['Air District'] = air_dist_list
    data['Air District'] = data['Air District'].str.title()
    county_dict = pd.Series(air_df.County.values,index=air_df.ZipCode).to_dict()
    county_list = []
    #getting county name
    for j in list_of_zips:
        if j != 0:
            county_value = county_dict.get(j)
            county_list.append(county_value)
        else:
            county_list.append('None')

    data['County'] = county_list
    return data

def remove_ad_suffix(data):
    for index,row in data.iterrows():
        ad_name = row['Air District']
        if ad_name != None:
            words = ad_name.split()
            words.pop()
        words = " ".join(words)
        data.at[index , 'Air District'] = words
        row['Air District'] = words
    return data

def remove_county_suffix(data):
    for index,row in data.iterrows():
        county_name = row['County']
        words = county_name.split()
        words.pop()
        words = " ".join(words)
        data.at[index , 'County'] = words
        row['County'] = words
    return data

def standardize_segment(data):
    all_segments = data['Segment'].to_list()
    #print("All segments: ", all_segments)
    updated_segment_list = []
    dict_of_segments = {"Truck - Medium Duty":"MD Truck", "Step Van":"MD Step Van", "Parcel Delivery":"MD Step Van",
    "Specialty Vehicle":"Other", "Utility Truck":"Other", "Delivery Truck":"MD Truck","Low-floor":"Transit Bus", "Coach Bus":"Coach",
    "Bus - Medium Duty":"Shuttle Bus", "Special Needs":"Shuttle Bus","School Bus C":"School Bus", "School Bus D":"School Bus",
    "School Bus A":"School Bus", "HH":"HD Truck", "MH":"MD Truck", "SB":"School bus", "SW":"Other", "TV":"Transit Bus", "UB":"Transit Bus",
     "MD":"MD Truck"}
    for j in all_segments:
        if j in dict_of_segments:
            segment_value = dict_of_segments.get(j)
            updated_segment_list.append(segment_value)
        else:
            updated_segment_list.append(j)
    data['Segment'] = updated_segment_list
    return data

def standardize_gvwr(data):
    for index, row in data.iterrows():
        if row['GVWR'] == 'Not Available':
            continue
        elif (row['GVWR'] >= 10001) and (row['GVWR'] <= 14000):
            data.loc[index, 'GVWR'] = '14,001 - 19,500'
        elif (row['GVWR'] >= 14001) and (row['GVWR'] <= 19500):
            data.loc[index, 'GVWR'] = '14,001 - 19,500'
        elif (row['GVWR'] >= 19501) and (row['GVWR'] <= 26000):
            data.loc[index, 'GVWR'] = '19,501 - 26,000'
        elif (row['GVWR'] >= 26001) and (row['GVWR'] <= 33000):
            data.loc[index, 'GVWR'] = '26,001 - 33,000'
        elif (row['GVWR'] >= 33001):
            data.loc[index, 'GVWR'] = '33,001 and up'
        else:
            #data.loc[index, 'GVWR'] = ''
            continue
    #print(data['GVWR'])
    #rint('End of GVWR')

    return data

if __name__ == "__main__":
    hvip_data = hvip()
    vw_data = vw_settlement()
    cap_moyer_df = cap_moyer()
    #ict_df = ict()
    rsbpp_df = rsbpp_buses()
    sac_df = sacramento()
    cmis_df = cmis()
    prop1b = prop1b()
    print("Length of HVIP", len(hvip_data))
    print("Length of VW", len(vw_data))
    print("Length of CAP", len(cap_moyer_df))
    #print("Length of ICT", len(ict_df))
    print("Length of RSBPP", len(rsbpp_df))
    print("Length of Sac", len(sac_df))
    print("Length of CMIS", len(cmis_df))
    standardize_gvwr = pd.concat([sac_df, rsbpp_df, prop1b, cmis_df, cap_moyer_df, vw_data, hvip_data], ignore_index = True)
    #re-categorize ZEV to EV
    standardize_gvwr['Fuel Type'] = standardize_gvwr['Fuel Type'].str.replace('ZEV', 'EV')
    standardize_gvwr['Fuel Type'] = standardize_gvwr['Fuel Type'].str.replace('Hfc', 'HFC')
    print("Length of Dataset w/ Duplicates",  len(standardize_gvwr))
    #dropping dups
    final_df = standardize_gvwr.drop_duplicates(subset=['VIN'])
    print("Length of Final Dataset",  len(final_df))

    final_df.to_csv('super_portal_data_v1.42.csv')

    exit()
    # gvwr_num_dfs = pd.concat([vw_data, cap_moyer_df, cmis_df, prop1b, rsbpp_df], ignore_index = True)
    # all_dfs_to_standardize_gvwr = standardize_gvwr(gvwr_num_dfs)
    # data_joined_1 = pd.concat([hvip_data, all_dfs_to_standardize_gvwr], ignore_index = True)
    #
    # ##clean segment
    # data_joined_1_segment_clean = standardize_segment(data_joined_1)
    # ##remove suffix
    # data_joined_ad_clean = remove_ad_suffix(data_joined_1_segment_clean)
    # print(len(data_joined_ad_clean))
    #
    # ##join sacramento and rsbpp here bc it already has cleaned segment and suffix
    # final_df = pd.concat([data_joined_ad_clean, rsbpp_df, sac_df], ignore_index = True)
    # print("Total Length", len(final_df))

    #generate output file
    #final_df.to_csv('super_portal_data_v1.40.csv')

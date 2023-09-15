import pandas as pd
from fuzzywuzzy import fuzz, process
import json
import numpy as np

# get fuzzy or exact match 
def get_ratio(source_df, dest_df, threshold= 82.0):
    
    list1 = source_df['CharacteristicName'].tolist()
    list2 = dest_df['Contaminant'].tolist()
    list1= [x.lower() for x in list1]
    list2= [x.lower() for x in list2]
    # empty lists for storing the matches 
    # later
    mat1 = []
    mat2 = []
    p = []
    
    if threshold ==100:
        for i in list1 : 
            if i.lower() in list2:
                p.append(i)
            else:
                p.append('')    
        source_df['matches'] = p        
        source_df['Contaminant'] = p
        return source_df
    else :   
        for i in list1:

            mat1.append(process.extractOne(
              i.lower(), list2, scorer=fuzz.partial_ratio))
            #mat1.append(process.extract(i, list2, limit=2))
        source_df['matches'] = mat1


        for j in source_df['matches']:
            if j[1] >= threshold:
                p.append(j[0])
            mat2.append(",".join(p))
            p = []
        # storing the resultant matches back to dframe1
        source_df['Contaminant'] = mat2

        return source_df

# get units from config
def get_unit(config, row :str) :

    for k in config.keys() :
        if k in row :
            return k        
    return row

# get unit value from config
def get_unit_val(config,modified_val, row ) :

    if row is not np.nan and modified_val in config.keys() :
        return float(row) * config[modified_val]        
    return row

def modify_unit(config,modified_val ) :

    if  modified_val in config.keys() :
        return 'mg/l'        
    return modified_val

if __name__ == "__main__":

    #load input datasets
    result_df = pd.read_csv('data/input/result.csv')
    print(result_df.columns)

    station_df = pd.read_csv('data/input/station.csv')
    print(station_df.columns)

    water_limit_df = pd.read_csv('data/input/water_limit.csv')
    print(water_limit_df.columns)

    # standardize units
    # read the script configuration file
    with open("data/config/unit_config.json", "r") as fp:
        config = json.load(fp)

    # intermediate data processing

    # standardize units for chemical dataset
    result_df['Modifed_ResultMeasure'] = result_df.apply(lambda x: get_unit(config,str(x["ResultMeasure/MeasureUnitCode"])), axis=1)
    
    result_df.to_csv('data/intermediate/result_1.csv',index=False, header=True)

    result_df['Modifed_ResultMeasureValue'] = result_df.apply(lambda x: get_unit_val(config,str(x['Modifed_ResultMeasure']) ,x["ResultMeasureValue"]), axis=1)
    
    result_df.to_csv('data/intermediate/result_2.csv',index=False, header=True)

    result_df['Modifed_ResultMeasure'] = result_df.apply(lambda x: modify_unit(config,str(x['Modifed_ResultMeasure']) ), axis=1)
    result_df= result_df.drop(['ResultMeasure/MeasureUnitCode','ResultMeasureValue'],axis=1)
    
    result_df.to_csv('data/intermediate/result_3.csv',index=False, header=True)
    
    # merge station dataset with result dataset
    station_df=station_df.drop(['OrganizationIdentifier', 'OrganizationFormalName'],axis=1)

    rs_df=pd.merge(result_df, station_df, how='inner', on=['MonitoringLocationIdentifier', 'MonitoringLocationIdentifier'])    

    print(rs_df.columns)
    rs_df.to_csv('data/intermediate/combined_station_res.csv',index=False, header=True)

    # merge water dataset, station dataset with result dataset with exact match
    water_limit_df['Contaminant'] = water_limit_df.apply(lambda x: x['Contaminant'].lower(), axis=1)

    res_water_df= get_ratio(rs_df, water_limit_df, 100) #exact match
    res_water_df.to_csv('data/intermediate/fuzz_combined.csv',index=False, header=True)

    res_water_df = res_water_df.drop('matches', axis=1)
    combined_df= pd.merge(res_water_df, water_limit_df, how='left', left_on=['Contaminant'],right_on= ['Contaminant'])
    print(combined_df.columns)

    combined_df.to_csv('data/intermediate/combined_water_fuzz.csv',index=False, header=True)
    print(combined_df.head())

    
    combined_df['Modifed_ResultMeasureValue'] = pd.to_numeric(combined_df['Modifed_ResultMeasureValue'], errors='coerce')
    combined_df['MCL(mg/L)'] = pd.to_numeric(combined_df['MCL(mg/L)'], errors='coerce')
    combined_df['MCLG1(mg/L)'] = pd.to_numeric(combined_df['MCLG1(mg/L)'], errors='coerce')
    
    # check for measured contaminants
    combined_df['Flag'] = combined_df.apply(lambda x: True if x['Modifed_ResultMeasureValue'] is not np.nan and ((x['MCLG1(mg/L)'] is not np.nan  and (x['Modifed_ResultMeasureValue']>=  x['MCLG1(mg/L)'])) or ( x['MCL(mg/L)'] is not np.nan and x['Modifed_ResultMeasureValue'] >= x['MCL(mg/L)'])) else False, axis=1)
    
    combined_df.to_csv('data/output/combined_water_fuzz_flag.csv',index=False, header=True)
    



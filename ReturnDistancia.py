import os
import sys
import glob
import numpy as np
from itertools import chain
import pandas as pd
import unique
import timeit
import CalcDistAzim
import ImportDF
import Count

def processArchive(frameSI,frameSI2):
    script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.') 
    frameSI = frameSI[frameSI['LATITUDE_MObileSite_Merged'].astype(bool)]
    frameSI = frameSI[frameSI['LOCATION_Agr_MObileSite_Merged'].astype(bool)]
    frameSI = frameSI[frameSI['MUNICIPIO_MObileSite_Merged'].astype(bool)]

    frameSI = frameSI.drop_duplicates()
    frameSI = frameSI.sort_values(['MUNICIPIO_MObileSite_Merged'], ascending = ([True]))
    frameSI = frameSI.drop_duplicates()

    df1 = frameSI2.copy()
    KeepList = ['MUNICIPIO_ListOfNewLocations']
    locationBase = list(df1.columns)
    DellList = list(set(locationBase)^set(KeepList))
    df1 = df1.drop(DellList,1)
    df1 = df1.drop_duplicates()
    df1 = df1.reset_index(drop=True)
    col_one_list = df1['MUNICIPIO_ListOfNewLocations'].tolist()

    df0 = frameSI2.copy()
    df0.insert(len(df0.columns),'Short2','')
    df0.insert(len(df0.columns),'Short3','')
    
    for i in col_one_list:
        df2 = frameSI.loc[frameSI['MUNICIPIO_MObileSite_Merged'] == i]
        df3 = df0.loc[df0['MUNICIPIO_ListOfNewLocations'] == i]
        for index1,row1 in df3.iterrows():
            maxValue = 100000
            for index2, row2 in df2.iterrows():
                if row2['MUNICIPIO_MObileSite_Merged'] == row1['MUNICIPIO_ListOfNewLocations']:
                    distancia = CalcDistAzim.CalcDist(row2['LATITUDE_MObileSite_Merged'],row2['LONGITUDE_MObileSite_Merged'],row1['LATITUDE_ListOfNewLocations'],row1['LONGITUDE_ListOfNewLocations'])
                    if distancia < maxValue:
                        maxValue = distancia
                        df3.at[index1,'Short2'] = row2['Short2_MObileSite_Merged']
                        df3.at[index1,'Short3'] = row2['Short3_MObileSite_Merged']  
        df3 = Count.count2(df3,'Short2')
        df3 = df3.sort_values(['Short2'], ascending = [True])            
        csv_path = os.path.join(script_dir, 'export/Distancia/'+ i +'_Distancia'+'.csv')
        df3.to_csv(csv_path,index=True,header=True,sep=';')


        filds_C = ['MUNICIPIO_ListOfNewLocations','ID_Temp_ListOfNewLocations','LATITUDE_ListOfNewLocations','LONGITUDE_ListOfNewLocations','Short2','Short3','count']
        pathImport = '/export/Distancia'
        Consolidado_Distancia = ImportDF.ImportDF(filds_C,pathImport)
        Consolidado_Distancia = Consolidado_Distancia.sort_values(['Short2'], ascending = [True])
        csv_path = os.path.join(script_dir, 'export/Consolidado_Distancia/'+'Consolidado'+'.csv')
        Consolidado_Distancia.to_csv(csv_path,index=True,header=True,sep=';')





import os
import sys
import glob
import numpy as np
from itertools import chain
import pandas as pd
from datetime import date
from datetime import datetime
import unique
import ShortName
import Agregar

def processArchive():
    fields = ['NAME','PROVISIONSTATUS','SITE_TYPE','IMPLEMENTATIION_STATUS','LOCATION','MUNICIPIO','ANF','REGIONAL','ANTENA_SYS_CLASS','LATITUDE','LONGITUDE']
    pathImport = '/import/MObileSite'
    pathImportSI = os.getcwd() + pathImport
    filtrolabel = 'REGIONAL'
    filtroValue = 'TSP'
    #print (pathImportSI)
    archiveName = pathImport[8:len(pathImport)]
    #print (archiveName)
    script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
    csv_path = os.path.join(script_dir, 'export/MObileSite/'+archiveName+'.csv')
    #print ('loalding files...\n')
    all_filesSI = glob.glob(pathImportSI + "/*.csv")
    all_filesSI.sort(key=lambda x: os.path.getmtime(x), reverse=True)
    #print (all_filesSI)
    li = []
    lastData = all_filesSI[0][len(all_filesSI[0])-19:len(all_filesSI[0])-11]
    for filename in all_filesSI:
        dataArchive = datetime.fromtimestamp(os.path.getmtime(filename)).strftime('%Y%m%d')
        iter_csv = pd.read_csv(filename, index_col=None, encoding="ANSI",header=0, error_bad_lines=False,dtype=str, sep = ';',iterator=True, chunksize=10000, usecols = fields )
        #df = pd.concat([chunk[(chunk[filtrolabel] == filtroValue)] for chunk in iter_csv]) # WORKS
        df = pd.concat([chunk for chunk in iter_csv])
        df2 = df[fields] # ordering labels   
        li.append(df2)
    frameSI = pd.concat(li, axis=0, ignore_index=True)
    frameSI = frameSI.drop_duplicates()

    for index, row in frameSI.iterrows(): 
        if pd.notna(row["LATITUDE"]):
            frameSI.at[index,"LATITUDE"] = row["LATITUDE"].replace('.',',')
            frameSI.at[index,"LONGITUDE"] = row["LONGITUDE"].replace('.',',')


    for index, row in frameSI.iterrows(): 
        if pd.notna(row["SITE_TYPE"]):
            row["SITE_TYPE"] = row["SITE_TYPE"][len(row["SITE_TYPE"])-2:len(row["SITE_TYPE"])]

    frameSI = frameSI.sort_values(["LOCATION","SITE_TYPE"], ascending = [True,True])
    frameSI = frameSI.reset_index(drop=True)

    #Drop SPX, SPY Cell
    frameSI.insert(len(frameSI.columns),'DropCol','Keep')
    frameSI.loc[(frameSI['NAME'].str[:3].isin(['SPX','SPY'])),['DropCol']] = 'Drop'
    frameSI.loc[(frameSI['NAME'].str[:5].isin(['SLSPX','SLSPY'])),['DropCol']] = 'Drop'
    frameSI = frameSI.loc[frameSI['DropCol'] == 'Keep']

    #Insert ShortName
    frameSI = ShortName.tratarShortNumber(frameSI,'NAME','SITE_TYPE','LOCATION')
    frameSI = frameSI.sort_values(['Short1'], ascending = [True])
    csv_path_Cell = os.path.join(script_dir, 'export/MObileSite_Cell/'+archiveName+'.csv')
    frameSI.to_csv(csv_path_Cell,index=True,header=True,sep=';')

  
    
    #AgregarLocation
    loc1 = frameSI.loc[frameSI['PROVISIONSTATUS'].isin(['In Service','Planned','Implementation','Installed'])]#'Implementation','Installed'
    locationAgr = Agregar.processArchive(loc1,'LOCATION','MObileSite_Station',['MUNICIPIO','Short2','LATITUDE','LONGITUDE'])

    #Short2
    Short2Agr = Agregar.processArchive(frameSI,'Short2','MObileSite_Short2',['Short3'])


    merged_all = pd.merge(locationAgr,Short2Agr, how='left',left_on=['Short2'],right_on=['Short2_Agr'],validate="m:m")
    merged_all = merged_all.sort_values(['MUNICIPIO'], ascending = [True])
    csv_path_Cell = os.path.join(script_dir, 'export/MObileSite_Merged/'+'MObileSite_Merged'+'.csv')
    merged_all.to_csv(csv_path_Cell,index=True,header=True,sep=';')
    return frameSI


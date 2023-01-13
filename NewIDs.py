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

def processArchive():
  script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.') 
  filds_C = ['MUNICIPIO_ListOfNewLocations','ID_Temp_ListOfNewLocations','LATITUDE_ListOfNewLocations','LONGITUDE_ListOfNewLocations','Short2','Short3']
  pathImport = '/export/Consolidado_Distancia'

  Consolidado_Distancia = ImportDF.ImportDF(filds_C,pathImport)

  Consolidado_Distancia.insert(len(Consolidado_Distancia.columns),'Last_Ref',0)
  Consolidado_Distancia.insert(len(Consolidado_Distancia.columns),'New_NAME','')
  newId_List = []
  for index, row in Consolidado_Distancia.iterrows():
    last_ref = str(row['Short3']).split('|')[-1:]
    if last_ref[0] != 'nan':
      if int(last_ref[0]) < 98:
        Consolidado_Distancia.at[index,'Last_Ref'] = int(last_ref[0]) + 1
        #Consolidado_Distancia.loc[Consolidado_Distancia['Short2'] == row['Short2'],['Last_Ref']] = row['Last_Ref'] + 1
        if int(last_ref[0]) + 1 < 10:
          tempName = row['Short2'] + '0' + str(Consolidado_Distancia.at[index,'Last_Ref'])
        else:
          tempName = row['Short2'] + str(Consolidado_Distancia.at[index,'Last_Ref'])
        if tempName not in newId_List:
          newId_List.append(tempName)
          Consolidado_Distancia.at[index, 'New_NAME'] = tempName
        else:
          Consolidado_Distancia.at[index,'Last_Ref'] = int(last_ref[0]) + 2
          if int(last_ref[0]) + 2 < 10:
            tempName = row['Short2'] + '0' + str(Consolidado_Distancia.at[index,'Last_Ref'])
          else:
            tempName = row['Short2'] + str(Consolidado_Distancia.at[index,'Last_Ref'])
          if tempName not in newId_List:
            newId_List.append(tempName)
            Consolidado_Distancia.at[index, 'New_NAME'] = tempName
          else:
            Consolidado_Distancia.at[index,'Last_Ref'] = int(last_ref[0]) + 3
            if int(last_ref[0]) + 3 < 10:
              tempName = row['Short2'] + '0' + str(Consolidado_Distancia.at[index,'Last_Ref'])
            else:
              tempName = row['Short2'] + str(Consolidado_Distancia.at[index,'Last_Ref'])
            if tempName not in newId_List:
              newId_List.append(tempName)
              Consolidado_Distancia.at[index, 'New_NAME'] = tempName
            else:
              Consolidado_Distancia.at[index,'Last_Ref'] = int(last_ref[0]) + 4
              if int(last_ref[0]) + 4 < 10:
                tempName = row['Short2'] + '0' + str(Consolidado_Distancia.at[index,'Last_Ref'])
              else:
                tempName = row['Short2'] + str(Consolidado_Distancia.at[index,'Last_Ref'])
              if tempName not in newId_List:
                newId_List.append(tempName)
                Consolidado_Distancia.at[index, 'New_NAME'] = tempName
              else:
                Consolidado_Distancia.at[index,'Last_Ref'] = int(last_ref[0]) + 5
                if int(last_ref[0]) + 5 < 10:
                  tempName = row['Short2'] + '0' + str(Consolidado_Distancia.at[index,'Last_Ref'])
                else:
                  tempName = row['Short2'] + str(Consolidado_Distancia.at[index,'Last_Ref'])
                if tempName not in newId_List:
                  newId_List.append(tempName)
                  Consolidado_Distancia.at[index, 'New_NAME'] = tempName
                else:
                  Consolidado_Distancia.at[index,'Last_Ref'] = int(last_ref[0]) + 6
                  if int(last_ref[0]) + 6 < 10:
                    tempName = row['Short2'] + '0' + str(Consolidado_Distancia.at[index,'Last_Ref'])
                  else:
                    tempName = row['Short2'] + str(Consolidado_Distancia.at[index,'Last_Ref'])
                  if tempName not in newId_List:
                    newId_List.append(tempName)
                    Consolidado_Distancia.at[index, 'New_NAME'] = tempName
                  else:
                    Consolidado_Distancia.at[index,'Last_Ref'] = int(last_ref[0]) + 7
                    if int(last_ref[0]) + 7 < 10:
                      tempName = row['Short2'] + '0' + str(Consolidado_Distancia.at[index,'Last_Ref'])
                    else:
                      tempName = row['Short2'] + str(Consolidado_Distancia.at[index,'Last_Ref'])
                    if tempName not in newId_List:
                      newId_List.append(tempName)
                      Consolidado_Distancia.at[index, 'New_NAME'] = tempName  




  Consolidado_Distancia = Consolidado_Distancia.drop(['Last_Ref'],1)
  csv_path = os.path.join(script_dir, 'export/NewIDs/'+'NewIDs'+'.csv')
  Consolidado_Distancia.to_csv(csv_path,index=True,header=True,sep=';')



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

  Consolidado_Distancia.insert(len(Consolidado_Distancia.columns),'Disponivel','')
  Consolidado_Distancia.insert(len(Consolidado_Distancia.columns),'Last_Ref',0)
  Consolidado_Distancia.insert(len(Consolidado_Distancia.columns),'New_NAME','')
  Consolidado_Distancia.insert(len(Consolidado_Distancia.columns),'New_NAME2','')
  Consolidado_Distancia.insert(len(Consolidado_Distancia.columns),'Status','OK')


  Consolidado_Distancia = Consolidado_Distancia.sort_values(['Short2'], ascending = [True])
    

  newId_List = []
  for index, row in Consolidado_Distancia.iterrows():
    last_ref = str(row['Short3']).split('|')
    if last_ref[0] != 'nan':
      desired_array = [int(numeric_string) for numeric_string in last_ref]
      disponivel = []
      inicio = desired_array[0]
      fim = desired_array[-1]
      for i in desired_array:
        if i != inicio:
          disponivel.append(inicio)
          inicio = i
        inicio +=1
      
      #Demais
      if len(desired_array)> 1:
        demais = []
        inicio = desired_array[0]
        fim = desired_array[-1]

        for i in range(inicio,fim):
          if i not in disponivel and i not in desired_array:
            disponivel.append(i)
        disponivel.sort(reverse = False)

        print(desired_array)
        print(disponivel)

        d = convert(disponivel)
        Consolidado_Distancia.at[index,'Disponivel'] = d

  for index, row in Consolidado_Distancia.iterrows():
    disponivel = str(row['Disponivel']).split('|')
    print(len(disponivel))
    print(disponivel)
    if len(disponivel) > 0 and disponivel[0] != '':
      Consolidado_Distancia.at[index,'Status'] = 'intermediario disponivel'

    last_ref = str(row['Short3']).split('|')[-1:]
    if last_ref[0] != 'nan':
      if int(last_ref[0]) < 99:
        Consolidado_Distancia.at[index,'Last_Ref'] = int(last_ref[0]) + 1
        #Consolidado_Distancia.loc[Consolidado_Distancia['Short2'] == row['Short2'],['Last_Ref']] = row['Last_Ref'] + 1
        if int(last_ref[0]) + 1 < 10:
          tempName = row['Short2'] + '0' + str(Consolidado_Distancia.at[index,'Last_Ref'])
        else:
          tempName = row['Short2'] + str(Consolidado_Distancia.at[index,'Last_Ref'])
        if tempName not in newId_List:
          newId_List.append(tempName)
          Consolidado_Distancia.at[index, 'New_NAME'] = tempName
          Consolidado_Distancia.at[index, 'New_NAME2'] = '4G-' + tempName
        else:
          Consolidado_Distancia.at[index,'Last_Ref'] = int(last_ref[0]) + 2
          if int(last_ref[0]) + 2 < 10:
            tempName = row['Short2'] + '0' + str(Consolidado_Distancia.at[index,'Last_Ref'])
          else:
            tempName = row['Short2'] + str(Consolidado_Distancia.at[index,'Last_Ref'])
          if tempName not in newId_List:
            newId_List.append(tempName)
            Consolidado_Distancia.at[index, 'New_NAME'] = tempName
            Consolidado_Distancia.at[index, 'New_NAME2'] = '4G-' + tempName
          else:
            Consolidado_Distancia.at[index,'Last_Ref'] = int(last_ref[0]) + 3
            if int(last_ref[0]) + 3 < 10:
              tempName = row['Short2'] + '0' + str(Consolidado_Distancia.at[index,'Last_Ref'])
            else:
              tempName = row['Short2'] + str(Consolidado_Distancia.at[index,'Last_Ref'])
            if tempName not in newId_List:
              newId_List.append(tempName)
              Consolidado_Distancia.at[index, 'New_NAME'] = tempName
              Consolidado_Distancia.at[index, 'New_NAME2'] = '4G-' + tempName
            else:
              Consolidado_Distancia.at[index,'Last_Ref'] = int(last_ref[0]) + 4
              if int(last_ref[0]) + 4 < 10:
                tempName = row['Short2'] + '0' + str(Consolidado_Distancia.at[index,'Last_Ref'])
              else:
                tempName = row['Short2'] + str(Consolidado_Distancia.at[index,'Last_Ref'])
              if tempName not in newId_List:
                newId_List.append(tempName)
                Consolidado_Distancia.at[index, 'New_NAME'] = tempName
                Consolidado_Distancia.at[index, 'New_NAME2'] = '4G-' + tempName
              else:
                Consolidado_Distancia.at[index,'Last_Ref'] = int(last_ref[0]) + 5
                if int(last_ref[0]) + 5 < 10:
                  tempName = row['Short2'] + '0' + str(Consolidado_Distancia.at[index,'Last_Ref'])
                else:
                  tempName = row['Short2'] + str(Consolidado_Distancia.at[index,'Last_Ref'])
                if tempName not in newId_List:
                  newId_List.append(tempName)
                  Consolidado_Distancia.at[index, 'New_NAME'] = tempName
                  Consolidado_Distancia.at[index, 'New_NAME2'] = '4G-' + tempName
                else:
                  Consolidado_Distancia.at[index,'Last_Ref'] = int(last_ref[0]) + 6
                  if int(last_ref[0]) + 6 < 10:
                    tempName = row['Short2'] + '0' + str(Consolidado_Distancia.at[index,'Last_Ref'])
                  else:
                    tempName = row['Short2'] + str(Consolidado_Distancia.at[index,'Last_Ref'])
                  if tempName not in newId_List:
                    newId_List.append(tempName)
                    Consolidado_Distancia.at[index, 'New_NAME'] = tempName
                    Consolidado_Distancia.at[index, 'New_NAME2'] = '4G-' + tempName
                  else:
                    Consolidado_Distancia.at[index,'Last_Ref'] = int(last_ref[0]) + 7
                    if int(last_ref[0]) + 7 < 10:
                      tempName = row['Short2'] + '0' + str(Consolidado_Distancia.at[index,'Last_Ref'])
                    else:
                      tempName = row['Short2'] + str(Consolidado_Distancia.at[index,'Last_Ref'])
                    if tempName not in newId_List:
                      newId_List.append(tempName)
                      Consolidado_Distancia.at[index, 'New_NAME'] = tempName
                      Consolidado_Distancia.at[index, 'New_NAME2'] = '4G-' + tempName
                    else:
                      Consolidado_Distancia.at[index,'Last_Ref'] = int(last_ref[0]) + 8
                      if int(last_ref[0]) + 8 < 10:
                        tempName = row['Short2'] + '0' + str(Consolidado_Distancia.at[index,'Last_Ref'])
                      else:
                        tempName = row['Short2'] + str(Consolidado_Distancia.at[index,'Last_Ref'])
                      if tempName not in newId_List:
                        newId_List.append(tempName)
                        Consolidado_Distancia.at[index, 'New_NAME'] = tempName
                        Consolidado_Distancia.at[index, 'New_NAME2'] = '4G-' + tempName
                      else:
                        Consolidado_Distancia.at[index,'Last_Ref'] = int(last_ref[0]) + 9
                        if int(last_ref[0]) + 9 < 10:
                          tempName = row['Short2'] + '0' + str(Consolidado_Distancia.at[index,'Last_Ref'])
                        else:
                          tempName = row['Short2'] + str(Consolidado_Distancia.at[index,'Last_Ref'])
                        if tempName not in newId_List:
                          newId_List.append(tempName)
                          Consolidado_Distancia.at[index, 'New_NAME'] = tempName
                          Consolidado_Distancia.at[index, 'New_NAME2'] = '4G-' + tempName     
                        else:
                          Consolidado_Distancia.at[index,'Last_Ref'] = int(last_ref[0]) + 10
                          if int(last_ref[0]) + 10 < 10:
                            tempName = row['Short2'] + '0' + str(Consolidado_Distancia.at[index,'Last_Ref'])
                          else:
                            tempName = row['Short2'] + str(Consolidado_Distancia.at[index,'Last_Ref'])
                          if tempName not in newId_List:
                            newId_List.append(tempName)
                            Consolidado_Distancia.at[index, 'New_NAME'] = tempName
                            Consolidado_Distancia.at[index, 'New_NAME2'] = '4G-' + tempName
                          else:
                            Consolidado_Distancia.at[index,'Last_Ref'] = int(last_ref[0]) + 11
                            if int(last_ref[0]) + 11 < 10:
                              tempName = row['Short2'] + '0' + str(Consolidado_Distancia.at[index,'Last_Ref'])
                            else:
                              tempName = row['Short2'] + str(Consolidado_Distancia.at[index,'Last_Ref'])
                            if tempName not in newId_List:
                              newId_List.append(tempName)
                              Consolidado_Distancia.at[index, 'New_NAME'] = tempName
                              Consolidado_Distancia.at[index, 'New_NAME2'] = '4G-' + tempName
                            else:
                              Consolidado_Distancia.at[index,'Last_Ref'] = int(last_ref[0]) + 12
                              if int(last_ref[0]) + 12 < 10:
                                tempName = row['Short2'] + '0' + str(Consolidado_Distancia.at[index,'Last_Ref'])
                              else:
                                tempName = row['Short2'] + str(Consolidado_Distancia.at[index,'Last_Ref'])
                              if tempName not in newId_List:
                                newId_List.append(tempName)
                                Consolidado_Distancia.at[index, 'New_NAME'] = tempName
                                Consolidado_Distancia.at[index, 'New_NAME2'] = '4G-' + tempName

  Consolidado_Distancia.loc[Consolidado_Distancia['New_NAME'] == '',['Status']] = 'Fazer Manual'
  Consolidado_Distancia = Consolidado_Distancia.drop(['Last_Ref'],1)
  csv_path = os.path.join(script_dir, 'export/NewIDs/'+'NewIDs'+'.csv')
  Consolidado_Distancia.to_csv(csv_path,index=True,header=True,sep=';')

def convert(list1):
  lista_1 = ''
  if len(list1) > 0:
    lastOne = list1[-1]
    for i in list1:
      if i != lastOne:
        lista_1 += str(i)+'|'
      else:
        lista_1 += str(i)  
  return lista_1

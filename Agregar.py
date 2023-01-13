import unique
import os
import sys


def processArchive(df,columnAgr,folderName,KeepList):
    script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.')
    locationAgr = df.copy()
    collName = columnAgr + '_Agr'
    KeepList.append(collName)
    locationAgr[collName] = locationAgr[columnAgr]


    locationAgr = locationAgr.dropna(subset=[columnAgr])
    locationAgr = locationAgr.fillna('').groupby([columnAgr], as_index=True).agg('|'.join)
    removefromloop = []
    locationBase_top = list(locationAgr.columns)
    res = list(set(locationBase_top)^set(removefromloop))
    for i in res: 
        for index, row in locationAgr.iterrows():
            locationAgr.at[index, i] = '|'.join(unique.unique_list(locationAgr.at[index, i].split('|'))) 

    locationBase = list(locationAgr.columns)
    DellList = list(set(locationBase)^set(KeepList))
    locationAgr = locationAgr.drop(DellList,1)
    locationAgr = locationAgr.drop_duplicates()
    locationAgr = locationAgr.reset_index(drop=True)


    csv_path_station = os.path.join(script_dir, 'export/' + folderName + '/' + folderName+'.csv')
    locationAgr.to_csv(csv_path_station,index=True,header=True,sep=';')
    return locationAgr
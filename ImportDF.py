import os
import sys
import glob
import pandas as pd



def change_columnsName(df):
    for i in df.columns:
        df.rename(columns={i:i + '_' + df.name},inplace=True)
    return df

def ImportDF(fields, pathImport):
    pathImportSI = os.getcwd() + pathImport
    all_filesSI = glob.glob(pathImportSI + "/*.csv")
    all_filesSI.sort(key=lambda x: os.path.getmtime(x), reverse=True)
    li = []

    for filename in all_filesSI:
        iter_csv = pd.read_csv(filename, index_col=None, encoding="UTF-8",header=0, error_bad_lines=False,dtype=str, sep = ';',decimal=',',iterator=True, chunksize=10000, usecols = fields )
        df = pd.concat([chunk for chunk in iter_csv]) # & |  WORKS
        df2 = df[fields] # ordering labels 
        #df2["dataArchive_Import"] = fileData   
        li.append(df2)
    frameSI = pd.concat(li, axis=0, ignore_index=True)
    frameSI = frameSI.drop_duplicates()

    return frameSI    
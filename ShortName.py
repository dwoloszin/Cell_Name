import pandas as pd
import numpy as np




def tratarShortNumber(df,siteNameCollumn,Tecnologia,Station):
    df_2G = df.loc[df[Tecnologia] == '2G']
    df_3G = df.loc[df[Tecnologia] == '3G']
    df_4G = df.loc[df[Tecnologia] == '4G']


    df_4G = tratarS(df_4G,'NAME','SITE_TYPE','LOCATION')
    df_2G['Short1'] = df_2G['NAME']
    df_all = df_2G.append(df_4G)

    #short 2G
    short2G = df_all.copy()
    #short2G = short2G.loc[short2G['PROVISIONSTATUS'].isin(['In Service','Implementation'])]

    KeepList = ['LOCATION','Short1']
    locationBase = list(short2G.columns)
    DellList = list(set(locationBase)^set(KeepList))
    short2G = short2G.drop(DellList,1)
    short2G = short2G.drop_duplicates()
    short2G = short2G.reset_index(drop=True)
    short2G.rename(columns={'LOCATION':'LOCATION2'},inplace=True)



    df_3G = pd.merge(df_3G,short2G, how='left',left_on=['LOCATION'],right_on=['LOCATION2'],validate="m:m")
    df_3G = df_3G.drop(['LOCATION2'],1)

    df_3G.loc[df_3G['Short1'].isna(),['Short1']] = df_3G['NAME']
    print(df_3G)

    df_all = df_all.append(df_3G)

    df_all = df_all.loc[df_all['Short1'].str[-1:].isin(['1','2','3','4','5','6','7','8','9','0'])]
    df_all = df_all.loc[df_all['Short1'].str[-2:-1].isin(['1','2','3','4','5','6','7','8','9','0'])]
    df_all = df_all.loc[(np.array(list(map(len,df_all['Short1'].astype(str).values))) == 6)]
    df_all = df_all.drop_duplicates()
    df_all = df_all.reset_index(drop=True)
    df_all['Short2'] = df_all['Short1'].str[:-2]
    df_all['Short3'] = df_all['Short1'].str[-2:]

    return df_all




def tratarS(df,siteNameCollumn,Tecnologia,Station):
    df2 = df.copy()
    df2.insert(len(df2.columns),'Short1',df2[siteNameCollumn])
    df2.loc[np.array(list(map(len,df2[siteNameCollumn].astype(str).values))) == 11,['Short1']] = df2[siteNameCollumn].astype(str).str[4:]
    df2.loc[(df2[siteNameCollumn].str[:4] == '18NL') & (np.array(list(map(len,df2[siteNameCollumn].astype(str).values))) == 10),['Short1']] = df2[siteNameCollumn].str[4:]
    df2.loc[(df2[siteNameCollumn].str[:4] == '18NL') & (np.array(list(map(len,df2[siteNameCollumn].astype(str).values))) == 12),['Short1']] = df2[siteNameCollumn].str[4:-2]
    df2.loc[(df2[siteNameCollumn].str[:2] == 'SL') & (np.array(list(map(len,df2[siteNameCollumn].astype(str).values))) == 10),['Short1']] = df2[siteNameCollumn].str[2:-2]
    df2.loc[(df2[siteNameCollumn].str[:1] == '3') & (np.array(list(map(len,df2[siteNameCollumn].astype(str).values))) == 9),['Short1']] = df2[siteNameCollumn].str[3:]
    df2.loc[(df2[siteNameCollumn].str[:3] == '7NL'),['Short1']] = df2[siteNameCollumn].str[3:]
    df2.loc[(df2[siteNameCollumn].str[:2] == 'NL'),['Short1']] = df2[siteNameCollumn].str[2:]
    df2.loc[(df2[siteNameCollumn].str[:2] == 'SL') & (np.array(list(map(len,df2[siteNameCollumn].astype(str).values))) == 8),['Short1']] = df2[siteNameCollumn].str[2:]
    df2.loc[(df2[siteNameCollumn].str[-3:] == '_SP') & (np.array(list(map(len,df2[siteNameCollumn].astype(str).values))) == 8),['Short1']] = df2[siteNameCollumn].str[:-3]
    #df2.loc[(df2[siteNameCollumn].str[:2] == '4S') & (np.array(list(map(len,df2[siteNameCollumn].astype(str).values))) == 8),'Short1'] = df2[siteNameCollumn].str[2:]
    df2.loc[(df2[siteNameCollumn].str[:2] == 'MM') & (np.array(list(map(len,df2[siteNameCollumn].astype(str).values))) == 8),['Short1']] = df2[siteNameCollumn].str[2:]
    #df2.loc[(np.array(list(map(len,df2[siteNameCollumn].values))) == 6),'Short1'] = df2[siteNameCollumn]
    df2.loc[(df2[siteNameCollumn].str[2:3] == '-') & (np.array(list(map(len,df2[siteNameCollumn].astype(str).values))) == 9),['Short1']] = df2[siteNameCollumn].str[3:]
    df2.loc[(df2[siteNameCollumn].str[2:3] == '-') & (np.array(list(map(len,df2[siteNameCollumn].astype(str).values))) == 12),['Short1']] = df2[siteNameCollumn].str[3:-3]


    return df2












def tratarShortNumber2(df,siteNameCollumn,Tecnologia,Station):
    df2 = df.copy()
    df2.insert(len(df2.columns),'Short1',df2[siteNameCollumn])
    df2.loc[np.array(list(map(len,df2[siteNameCollumn].astype(str).values))) == 11,['Short1']] = df2[siteNameCollumn].astype(str).str[4:]
    df2.loc[(df2[siteNameCollumn].str[:4] == '18NL') & (np.array(list(map(len,df2[siteNameCollumn].astype(str).values))) == 10),['Short1']] = df2[siteNameCollumn].str[4:]
    df2.loc[(df2[siteNameCollumn].str[:4] == '18NL') & (np.array(list(map(len,df2[siteNameCollumn].astype(str).values))) == 12),['Short1']] = df2[siteNameCollumn].str[4:-2]
    df2.loc[(df2[siteNameCollumn].str[:2] == 'SL') & (np.array(list(map(len,df2[siteNameCollumn].astype(str).values))) == 10),['Short1']] = df2[siteNameCollumn].str[2:-2]
    df2.loc[(df2[siteNameCollumn].str[:1] == '3') & (np.array(list(map(len,df2[siteNameCollumn].astype(str).values))) == 9),['Short1']] = df2[siteNameCollumn].str[3:]
    df2.loc[(df2[siteNameCollumn].str[:3] == '7NL'),['Short1']] = df2[siteNameCollumn].str[3:]
    df2.loc[(df2[siteNameCollumn].str[:2] == 'NL'),['Short1']] = df2[siteNameCollumn].str[2:]
    df2.loc[(df2[siteNameCollumn].str[:2] == 'SL') & (np.array(list(map(len,df2[siteNameCollumn].astype(str).values))) == 8),['Short1']] = df2[siteNameCollumn].str[2:]
    df2.loc[(df2[siteNameCollumn].str[-3:] == '_SP') & (np.array(list(map(len,df2[siteNameCollumn].astype(str).values))) == 8),['Short1']] = df2[siteNameCollumn].str[:-3]
    #df2.loc[(df2[siteNameCollumn].str[:2] == '4S') & (np.array(list(map(len,df2[siteNameCollumn].astype(str).values))) == 8),'Short1'] = df2[siteNameCollumn].str[2:]
    df2.loc[(df2[siteNameCollumn].str[:2] == 'MM') & (np.array(list(map(len,df2[siteNameCollumn].astype(str).values))) == 8),['Short1']] = df2[siteNameCollumn].str[2:]
    #df2.loc[(np.array(list(map(len,df2[siteNameCollumn].values))) == 6),'Short1'] = df2[siteNameCollumn]
    df2.loc[(df2[siteNameCollumn].str[2:3] == '-')& (np.array(list(map(len,df2[siteNameCollumn].astype(str).values))) == 9),['Short1']] = df2[siteNameCollumn].str[3:]
    #df3 = df2.loc[df2['Station'] == 'SPCAS_0055']
    #print(df3)
    df2.loc[(df2[Tecnologia] == '2G'),['Short1']] = df2[siteNameCollumn]

    #todo incluir shot 3G puro
    df2.loc[(df2[Tecnologia] == '3G'),['Short1']] = df2[siteNameCollumn]   

    return df2















def Short2Gto3G(df,Tecnologia,Station):
    df3 = df.copy()
    df3 = df3.reset_index(drop=True)
    df4G2G = df3.loc[(df3[Tecnologia] == '2G') | (df3[Tecnologia] == '4G')]
    KeepList_df4G2G = [Station,'Short1']
    locationBase_df4G2G = list(df4G2G.columns)
    DellListdf4G2G = list(set(locationBase_df4G2G)^set(KeepList_df4G2G))
    df4G2G = df4G2G.drop(DellListdf4G2G,1)
    df4G2G = df4G2G.drop_duplicates()
    df4G2G = df4G2G.reset_index(drop=True)

    indexI = 0
    for i in df3[Station]:
        indexJ = 0
        for j in df4G2G[Station]:
            if i == j:
                df3.at[indexI,'Short1'] = df4G2G.at[indexJ,'Short1']
            indexJ += 1
        indexI += 1    
    return df3
    
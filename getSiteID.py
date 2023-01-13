import ImportDF
import ReturnDistancia



def processArchive():
    MObileSite_Merged_fields = ['MUNICIPIO','LATITUDE','LONGITUDE','Short2','LOCATION_Agr','Short3']
    MObileSite_Merged_pathImport = '/export/MObileSite_Merged'

    ListOfNewLocations_fields = ['MUNICIPIO','ID_Temp','LATITUDE','LONGITUDE']
    ListOfNewLocations_pathImport = '/import/ListOfNewLocations'

    MObileSite_Merged = ImportDF.ImportDF(MObileSite_Merged_fields,MObileSite_Merged_pathImport)
    MObileSite_Merged.name = 'MObileSite_Merged'
    MObileSite_Merged = ImportDF.change_columnsName(MObileSite_Merged)

    ListOfNewLocations = ImportDF.ImportDF(ListOfNewLocations_fields,ListOfNewLocations_pathImport)
    ListOfNewLocations.name = 'ListOfNewLocations'
    ListOfNewLocations = ImportDF.change_columnsName(ListOfNewLocations)

    print(MObileSite_Merged)
    print(ListOfNewLocations)

    ReturnDistancia.processArchive(MObileSite_Merged,ListOfNewLocations)


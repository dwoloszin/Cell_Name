import timeit
import MObileSite
import getSiteID
import NewIDs
import NewIDs2
import NewIDs3
import NewIDs4
import os
import glob
import sys
inicioTotal = timeit.default_timer()



script_dir = os.path.abspath(os.path.dirname(sys.argv[0]) or '.') 
files = glob.glob(script_dir + '/export/Distancia/*.csv')
for f in files:
    os.remove(f)



print ('\nprocessing MObileSite... ')
inicio = timeit.default_timer()
MobileSite1 = MObileSite.processArchive()
print ('MObileSite done!: ')
fim = timeit.default_timer()
print ('duracao: %f' % ((fim - inicio)/60) + ' min') 

print ('\nprocessing getSiteID... ')
inicio = timeit.default_timer()
getSiteID.processArchive()
print ('getSiteID done!: ')
fim = timeit.default_timer()
print ('duracao: %f' % ((fim - inicio)/60) + ' min')





#processar Consolidado_Distancia
print ('\nprocessing NewIDs... ')
inicio = timeit.default_timer()
NewIDs4.processArchive()
print ('NewIDs done!: ')
fim = timeit.default_timer()
print ('duracao: %f' % ((fim - inicio)/60) + ' min')


fimTotal = timeit.default_timer()
print ('duracao: %f' % ((fimTotal - inicioTotal)/60) + ' min') 

import csv
import datetime as dt
import os
import pandas as pd

path = 'C:/Users/Administrator/Documents/MultiCharts/DATA/ETHBUSD_F/F10'
listFiles = os.listdir(path)
# listFiles.reverse()
pathNewFile = path + "/MERGED.csv"
newFile = open(pathNewFile, 'w', newline='') 
mcWriter = csv.writer(newFile, delimiter=',', quoting=csv.QUOTE_NONE, escapechar='\\', doublequote=False)    
for p in listFiles:
    if ("csv" not in p): continue
    # if ("NEW" in p or "zip" in p): continue
    # if ("2022-09" in p): continue
    pathFile = path + "/" + p
    print(pathFile)    
    with open(pathFile, 'r') as file:
        reader = csv.reader(file)
        count = 0
        for row in reader:        
            mcWriter.writerow(row)            
print("Done")

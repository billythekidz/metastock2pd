import csv
import datetime as dt
import os
import pandas as pd

path = 'C:/Users/Administrator/Documents/MultiCharts/DATA/ETHBUSD'
listFiles = os.listdir(path)
listFiles.reverse()
for p in listFiles:
    if ("NEW" in p or "zip" in p): continue
    # if ("2022-09" in p): continue
    pathFile = path + "/" + p
    pathNewFile = path + "/" + p.replace('.csv','-NEW.csv')
    print(pathFile)    
    newFile = open(pathNewFile, 'w', newline='') 
    mcWriter = csv.writer(newFile, delimiter=',', quoting=csv.QUOTE_NONE, escapechar='\\', doublequote=False)    
    lastestTime = dt.datetime.fromordinal(1)
    with open(pathFile, 'r') as file:
        # reader = pd.read_csv(file)#, skiprows=1)
        reader = csv.reader(file)
        count = 0
        for row in reader:        
        # for index, row in reader.iterrows():                     
            count += 1
            # print(row)
            # price = round(float(row['price']),2)
            # volume = int(float(row['quote_qty']))
            # timestamp = float((row['time'])) / 1000
            if count == 1: continue            
            # print(row)
            # print(".", end ="")
            # tickid = row[0]
            price = round(float(row[1]),2)
            volume = int(float(row[3]))
            timestamp = float((row[4])) / 1000            
            # 
            datetime = dt.datetime.fromtimestamp(timestamp) #
            if count == 2: lastestTime = datetime
            if lastestTime >= datetime:
                # print("before " + str(lastestTime.microsecond))
                # print("last " + str(lastestTime.timestamp()))
                datetime = dt.datetime.fromtimestamp(lastestTime.timestamp() + 0.001)
                # print("after " + str(datetime.microsecond))                
            lastestTime = datetime
            date = datetime.strftime('%m-%d-%Y') 
            time = datetime.strftime('%H:%M:%S.%f')[:-3]                                                                   
            mcRow = [date, time, price, volume] 
            mcWriter.writerow(mcRow)            
            # print(mcRow)     
            #print(fixRow)
            # except:
            #     pass
    print("Done")
    # csv_file = csv.DictReader(file)
    # for row in csv_file:
        # print(dict(row))
import csv
import datetime as dt


yearsback = 2
mcFile = open('VN30F1M_MC_BACKYEAR.csv', 'w', newline='') 
mcWriter = csv.writer(mcFile, delimiter=',', quoting=csv.QUOTE_NONE, escapechar='\\', doublequote=False)    
with open("VN30F1M_MC.csv", 'r') as file:
    reader = csv.reader(file)
    lastTime = dt.datetime.fromordinal(1)    
    for row in reader:
        # try:
        # columns = row.split(',')
        readDate = row[0] #splits the line at the comma and takes the first bit
        readTime = row[1]
        # dateTime = dt.datetime.strptime(readDate + " " + readTime, '%d-%m-%Y %H:%M:%S')
        dateTime = dt.datetime.strptime(readDate + "," + readTime, '%m/%d/%Y,%I:%M:%S %p')    
        dateTime = dateTime.replace(year=dateTime.year-yearsback)    
        acsii_date = dateTime.strftime('%m/%d/%Y')
        # print(acsii_date)
        acsii_time = dateTime.strftime('%I:%M:%S %p')                                                   
        price = float(row[2])
        volume = int(float(row[3]))
        mcRow = [acsii_date, acsii_time, price, volume]
        # for i in range(2, len(row),1):
            # newRow.append(row[i])
        mcWriter.writerow(mcRow)
        
        # dateTime = dt.datetime.strptime(readDate + "," + readTime, '%m/%d/%Y,%I:%M:%S %p')
        if (lastTime > dateTime):            
            print("Error: " + readDate + " " + readTime)        
            break
        lastTime = dateTime
        #print(fixRow)
        # except:
        #     pass
print("Done")
    # csv_file = csv.DictReader(file)
    # for row in csv_file:
        # print(dict(row))
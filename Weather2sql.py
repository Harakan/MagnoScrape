import re
import sys
import sqlite3
import csv
import glob
from fuzzywuzzy import fuzz
from fuzzywuzzy import process
from google_api import givePos

#NOTE ON CSV:
#metadata
#1. Station Name
#2. Provice
#3. Latitude
#4. longitude
#5. Elevation
#6. Climate Identifier
#7. WMO Identifier
#8. TC Identifier

#DATA:
#[date, year, month, day, data quality, max temp, max temp flag, min temp, min temp flag, mean temp, mean temp flag, heat deg days, heat deg days flag, cool deg days, cool deg days flag, total rain (mm), total rain flag, total snow (cm), total snow flag, total precip (mm), total precip flag, snow on ground (cm), snow on ground flag, direction of max gust (10s degree), dir max gust flag, speed of max gust (km/h), speed of max gust flag]
    #OFFSETS:
                #date=line[0]
                #dataQuality=line[4]
                #maxTemp=line[5]
                #minTemp=line[7]
                #meanTemp=line[9]
                #heatDegDays=line[11]
                #coolDegDays=line[13]
                #totalRain=line[15]
                #totalSnow=line[17]
                #totalPrecip=line[19]
                #groundSnow=line[21]
                #gustDir=line[23]
                #gustSpeed=line[25]

#POPULATION:
#"Geographic code","Geographic name","Geographic type","Incompletely enumerated Indian reserves and Indian settlements, 2011","Population, 2011","Population, 2006","2006 adjusted population flag","Incompletely enumerated Indian reserves and Indian settlements, 2006","Population, % change","Total private dwellings, 2011","Private dwellings occupied by usual residents, 2011","Land area in square kilometres, 2011","Population density per square kilometre, 2011","National population rank, 2011","Provincial/territorial population rank, 2011"
    #OFFSETS:
            #geoCode=line[0]
            #geoName=line[1]
            #geoType=line[2]
            #pop2011=line[4]
            #pop2006=line[5]
            #privdwel2011=line[9]
            #privusualdwel2011=line[10]
            #totalLand=line[11]
            #popDensity=line[12]
            #popRank=line[13]

class Weather2sql():
    def __init__(self,serverConnect=False,sqlServer=''):
        print "initializing Weather2sql"
        self.index=1000
        if serverConnect:
            try:
                self.conn = sqlite3.connect(sqlServer)
                self.c=self.conn.cursor()
                self.sqlserver=True
                print "Connected to SQL server successfully"
            except:
                self.conn=False
                self.sqlserver=False
                self.c=False
                print "Failed to connected to SQL server"

        self.provMap={"BRITISH COLUMBIA": "BC",
                "ALBERTA": "AB",
                "SASKATCHEWAN":"SK",
                "MANITOBA":"MB",
                "ONTARIO":"ON",
                "QUEBEC":"QC",
                "NEWFOUNDLAND":"NF",
                "PRINCE EDWARD ISLAND":"PEI",
                "NOVA SCOTIA":"NS",
                "NEW BRUNSWICK":"NB",
                "YUKON TERRITORY":"YK",
                "NORTHWEST TERRITORIES":"NWT",
                "NUNAVUT":"NV"}
        self.popProvMap={"B.C.": "BC",
                "Alta.": "AB",
                "Sask.":"SK",
                "Man.":"MB",
                "Ont.":"ON",
                "Que.":"QC",
                "N.L.":"NF",
                "P.E.I.":"PEI",
                "N.S.":"NS",
                "N.B.":"NB",
                "Y.T.":"YK",
                "N.W.T.":"NWT",
                "Nvt.":"NV"}
        self.lut={"BC" : [],
                "AB" : [],
                "SK" : [],
                "MB" : [],
                "ON" : [],
                "QC" : [],
                "NF" : [],
                "PEI" : [],
                "NS" : [],
                "NB" : [],
                "YK" : [],
                "NWT" : [],
                "NV" : []}

    def makeTables(self):
        if self.sqlserver == True:
            self.conn.execute('''CREATE TABLE stations 
                (station_name varchar(32) NOT NULL,
                province varchar(8),
                wmo_identifier int,
                latitude float,
                longitude float,
                elevation float,
                climate_identifier varchar(8),
                population_2011 int,
                population_2006 int,
                tc_identifier varchar(8),
                geo_code int,
                geo_type varchar(8),
                pop_2006 int,
                pop_2011 int,
                priv_dwel2011 int,
                priv_usualdwel2011 int,
                total_land float,
                pop_rank int,
                idx int NOT NULL UNIQUE,
                PRIMARY KEY(wmo_identifier,idx)
                );''')

            #self.conn.execute('''CREATE TABLE yearly 
                #(wmo identifier int NOT NULL,
                #cold days int, 
                #population, population density, '''

                #weatherData=[date,dataQuality,maxTemp,minTemp,meanTemp,heatDegDays,coolDegDays,totalRain,totalSnow,totalPrecip,groundSnow,gustDir,gustSpeed]
            self.conn.execute('''CREATE TABLE daily
                (weather_date date NOT NULL,
                wmo_identifier int,
                data_quality varchar(8),
                max_temp float,
                min_temp float,
                mean_temp float,
                heatdeg_days float,
                cooldeg_days float,
                total_rain float,
                total_snow float,
                total_precip float,
                ground_snow float,
                gust_dir float,
                gust_speed float,
                idx int NOT NULL,
                PRIMARY KEY(idx,wmo_identifier,weather_date)
                );''')
        else:
            print "Not connected to server, cannot make tables"

    def genDistMap(self):
        #Loop over City
        #   Generate list of closest neibhors
        #       Loop through all the weather data for a temp to be 999
        #           if it is, replace with next closest neibhors temp (recursive?)
        self.createDistTable()
        self.c.execute('SELECT station_name,idx,latitude,longitude FROM stations')
        nameMap=self.c.fetchall()
        for line in nameMap:
            for line2 in nameMap:
                dist=self.genDistance(line[2],line[3],line2[2],line2[3])
                if dist<1.0: #Distance in ~rads (small distance pythagorous but using lat/long, It works for ordering of distances where the delta of the lat or long is less than 180)
                    self.conn.execute('INSERT INTO distmap (idx1,idx2,dist) VALUES (?,?,?)',(line[1],line2[1],dist))
                    print "{0} and {1} give a distance of: {2}".format(line[1],line2[1],dist)
        self.conn.commit()

    def genDistance(self,lat1,lon1,lat2,lon2):
        lat = lat1-lat2
        lon = lon1-lon2
        dist=(lat**2+lon**2)**0.5
        return dist 

    def createDistTable(self):
        self.conn.execute('''CREATE TABLE distmap
        (idx1 int NOT NULL,
        idx2 int NOT NULL,
        dist float NOT NULL);''')

    def fuzzWeather(self):
        self.c.execute('SELECT station_name,idx,latitude,longitude FROM stations')
        nameMap=self.c.fetchall()
        total=len(nameMap)
        count=0
        fixed=0
        for line in nameMap:
            self.c.execute('SELECT weather_date,max_temp,min_temp,mean_temp FROM daily WHERE idx=?',(line[1],))
            weatherData=self.c.fetchall()
            print "Fixing idx {0}".format(line[1])
            print "\t{0}/{1} Finished, Fixed {2} Enteries".format(count,total,fixed)
            count+=1
            fixed+=self.fixMissingTemps(weatherData,line[1]) 
        self.conn.commit()

    def fixMissingTemps(self,weatherData,idx):
        fixed=0
        self.c.execute('SELECT idx2,dist FROM distmap WHERE idx1=? ORDER BY dist',(idx,)) 
        distMap=self.c.fetchall()
        for line in weatherData: #weather_date,max_temp,min_temp,mean_temp
            if line[3] == 999: #key off mean_temp since that neads max and min
                found=False
                for dist in distMap:
                    try:
                        self.c.execute('SELECT max_temp,min_temp,mean_temp FROM daily WHERE idx=? AND weather_date=?',(dist[0],line[0]))
                        replacementData=self.c.fetchall()
                        #print "DEBUG: {0}".format(replacementData)
                        if replacementData[0][2]!=999: #again key off mean_data
                            #print "Found Fix for idx={0}".format(idx)
                            newDat=replacementData[0]
                            self.conn.execute('UPDATE daily SET max_temp=?,min_temp=?,mean_temp=? WHERE idx=? AND weather_date=?',(newDat[0],newDat[1],newDat[2],idx,line[0]))
                            fixed+=1
                            break
                    except:
                        print "No Replacement Data for {0}".format(idx)
        return fixed

    def genLookup(self, nameMaps):
        print self.lut #should be an empty dict of each province
        #relevant data per index, name,idx,lat,long
        for line in nameMaps:
            self.lut[line[2]].append([line[0],line[1],line[3],line[4]])
        #print self.lut["SK"]
            
    def parsePop(self,filename):
        fid=open(filename, 'rb')
        tempReader=csv.reader(fid, delimiter=',', quotechar='"')

        self.c.execute('SELECT station_name,idx,province,latitude,longitude FROM stations')
        nameMap=self.c.fetchall()
        if nameMap==[]:
            raw_input("NO RESPONCE FROM SQL NIGGAH")
        else:
            self.genLookup(nameMap)
    
        count=0
        pauseCount=0
        for line in tempReader:
            geoCode=line[0]
            #geoName=line[1].encode('ascii')
            geoName=line[1]
            geoType=line[2]
            pop2011=line[4]
            pop2006=line[5]
            privdwel2011=line[9]
            privusualdwel2011=line[10]
            totalLand=line[11]
            popDensity=line[12]
            popRank=line[13]
            allPop=[geoCode,geoName,geoType,pop2011,pop2006,privdwel2011,privusualdwel2011,totalLand,popDensity,popRank]
            success=self.sqlPop(allPop)
            count=count+success
            pauseCount=pauseCount+1
            if pauseCount % 2000 == 0:
                print("\n------------------------")
                raw_input("\t PAUSING TO SWITCH IP's")
                
        print
        print "Matched: {0}/{1} {2}%".format(count,pauseCount, count*100.0/pauseCount)
        self.conn.commit()

    def sqlPop(self,allPop):
        geoName=allPop[1]
        provRE=re.compile('\((.*?)\)')
        stationRE=re.compile('(.*?)\(')
        geoProv=provRE.findall(geoName)[-1]
        geoProv=self.popProvMap[geoProv] #fix the name
        geoName=stationRE.findall(geoName)[0]
        queryName="{0}, ({1}), Canada".format(geoName,geoProv)
        try:
            queryLatLong=givePos(queryName)
            print "{0} -> {1}".format(queryName,queryLatLong)
            success=1
        except:
            print "Couldn't find lat/long for {0}".format(queryName)
            success=0
        if success:
            try:
                self.updatePop(allPop,queryLatLong,geoProv)
            except:
                print "Can't update population (stupid accents)"
        return success

    def updatePop(self,allPop,LatLong,geoProv):
        idx=self.getNearIdx(LatLong,geoProv)
        self.c.execute('SELECT station_name,idx,pop_2011,pop_2006,priv_dwel2011,priv_usualdwel2011 FROM stations WHERE idx=?',((idx,)))
        curStored=self.c.fetchall()
        if curStored[0][2]==None:
            self.conn.execute('UPDATE stations SET geo_code=?,geo_type=?,pop_2011=?,pop_2006=?,priv_dwel2011=?,priv_usualdwel2011=?,total_land=?,pop_Rank=? WHERE idx=?',((allPop[0],unicode(allPop[2]),allPop[3],allPop[4],allPop[5],allPop[6],allPop[7],allPop[9],idx)))
        else:
            pop_2011=int(allPop[3])+int(curStored[0][2])
            pop_2006=int(allPop[4])+int(curStored[0][3])
            priv_dwel2011=int(allPop[5])+int(curStored[0][4])
            priv_usualdwel2011=int(allPop[6])+int(curStored[0][5])
            self.conn.execute('UPDATE stations SET pop_2011=?,pop_2006=?,priv_dwel2011=?,priv_usualdwel2011=? WHERE idx=?',((pop_2011,pop_2006,priv_dwel2011,priv_usualdwel2011,idx)))

    def getNearIdx(self,LatLong,geoProv):
        idx=0
        distance=999
        LUT=self.lut[geoProv]
        for entry in LUT: #name,idx,lat,long
            lat = LatLong[0]-entry[2]
            lon = LatLong[1]-entry[3]
            tmpDist=(lat**2+lon**2)**0.5
            if tmpDist < distance:
                idx=entry[1]
                distance=tmpDist
        print ("\tidx: {0}, dist: {1}".format(idx,distance))
        return idx

    #... I'm so sorry about this function, I didn't know how else to do it.
    #if/else each input and check for "good" conditions, else fill it with usable stuff
    def parseCsv(self,filename):
        gotMetadata=False
        self.oldIndex=0
        metaData=[]
        fid=open(filename, 'rb')
        tempReader=csv.reader(fid, delimiter=',', quotechar='"')
        for line in tempReader:
            lineLen=len(line) #parse by line length (in terms of array entries)
            #print lineLen
            if lineLen==2: #2 entry is always "metadata"
                metaData.append(line[1])
            elif lineLen==27: #27 colums in csv given to us
                if "Date/Time" == line[0]:
                    pass #skip the header
                else:
                    date=line[0]
                    dataQuality=line[4]
                    if dataQuality == '\x86':
                        dataQuality=1 #"good"
                    else:
                        dataQuality=0 #"bad?"
                    if line[5]=='':
                        maxTemp=999
                    else:
                        maxTemp=float(line[5])
                    if line[7]=='':
                        minTemp=999
                    else:
                        minTemp=float(line[7])
                    if line[9]=='':
                        meanTemp=999
                    else:
                        meanTemp=float(line[9])
                    if line[11]=='':
                        heatDegDays=999
                    else:
                        heatDegDays=float(line[11])
                    if line[13]=='':
                        coolDegDays=999
                    else:
                        coolDegDays=float(line[13])
                    if line[15] == '':
                        totalRain=0.0
                    else:
                        totalRain=float(line[15])
                    if line[17]== '':
                        totalSnow=0.0
                    else:
                        totalSnow=float(line[17])
                    if line[19]== '':
                        totalPrecip=0.0
                    else:
                        totalPrecip=float(line[19])
                    if line[21]== '':
                        groundSnow=0.0
                    else:
                        groundSnow=float(line[21])
                    if line[23]== '':
                        gustDir=0.0
                    else:
                        gustDir=float(line[23])
                    if line[25] in ['','<31']:
                        gustSpeed=0.0
                    else:
                        gustSpeed=float(line[25])
                    #this sucks, but makes the sql later easier metaData[6] should be the wmo
                    if self.oldIndex==0:
                        index=self.index
                    else:
                        index=self.oldIndex
                    weatherData=[date,metaData[6],dataQuality,maxTemp,minTemp,meanTemp,heatDegDays,coolDegDays,totalRain,totalSnow,totalPrecip,groundSnow,gustDir,gustSpeed,index]
                    self.saveTemp(weatherData)
            elif lineLen==0:
                #if '' in metaData: #It's missing some data in the meta... yikes
                #    print "INCOMPLETE DATA, BREAKING"
                #    break
                #elif gotMetadata==False:
                if gotMetadata==False:
                    self.oldIndex=self.saveMeta(metaData,self.index)
                    gotMetadata=True
        self.index=self.index+1


    def saveTemp(self,weatherData):
        #date=line[0]
        #metadata(manually added here)
        #dataQuality=line[4]
        #maxTemp=line[5]
        #minTemp=line[7]
        #meanTemp=line[9]
        #heatDegDays=line[11]
        #coolDegDays=line[13]
        #totalRain=line[15]
        #totalSnow=line[17]
        #totalPrecip=line[19]
        #groundSnow=line[21]
        #gustDir=line[23]
        #gustSpeed=line[25]
        try:
            self.conn.executemany('INSERT INTO daily (weather_date,wmo_identifier,data_quality,max_temp,min_temp,mean_temp,heatdeg_days,cooldeg_days,total_rain,total_snow,total_precip,ground_snow,gust_dir,gust_speed, idx) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',(weatherData,))
        except:
            print "Insert Failed, likely cause is duplicated csv data"

    def saveMeta(self,metaData,index):
        metaData[1]=self.provMap[metaData[1]] #Shorten province names
        self.c.execute('SELECT station_name,idx FROM stations WHERE station_name=?',(metaData[0],))
        tmp=self.c.fetchall()
        print "SAVING: {0}".format(metaData)

        if tmp == None or tmp == []: #new entry
            outputData=metaData+[index]
            self.conn.executemany('INSERT INTO stations (station_name,province,latitude,longitude,elevation,climate_identifier,wmo_identifier,tc_identifier,idx) VALUES (?,?,?,?,?,?,?,?,?)',(outputData,))
        else: #existing entry
            #print "got: {0}".format(tmp)
            #raw_input("AAAAND IS IT RIGHT!?")
            station=tmp[0][0]
            index=tmp[0][1]
            outputData=metaData+[index]
            if station==metaData[0]: #As long as the names match the previous one
                print "\tFound previous entry, using that index for meta"
                return index #the previous one, as parsed by tmp
            else:
                print "station/wmo mismatch: {0} vs {1}".format([station,wmo],[metaData[0],metaData[6]])
        return self.index #the current (last_index+1) math done in calling function

class Demographics():
    def __init__(self,serverConnect=False,sqlServer=''):
        print "initializing Demographics"
        if serverConnect:
            try:
                self.conn = sqlite3.connect(sqlServer)
                self.c=self.conn.cursor()
                self.sqlserver=True
                print "Connected to SQL server successfully"
            except:
                self.conn=False
                self.sqlserver=False
                self.c=False
                print "Failed to connected to SQL server"
        self.yearsRe=re.compile("\d+")

    def initializeMaps(self):
    #looks like 17358 enteries per region/province
        self.provMap={"Canada": "CAN",
                "Newfoundland and Labrador": "NF",
                "Prince Edward Island":"PEI",
                "Nova Scotia":"NS",
                "New Brunswick":"NB",
                "British Columbia":"BC",
                "Alberta":"AB",
                "Saskatchewan":"SK",
                "Manitoba":"MB",
                "Ontario":"ON",
                "Quebec":"QC",
                "Yukon":"YK",
                "Northwest Territories":"NWT",
                "Nunavut":"NV",
                "Northwest Territories including Nunavut":"NWT"}
        
        self.sexMap={"Both sexes":0,
                "Males":1,
                "Females":2}

    def parseAge(self,agefile='age_demographics_province.csv'):
        print "parsing file {0}".format(agefile)
        self.initializeMaps()
        fid=open(agefile,'r')
        tempReader=csv.reader(fid, delimiter=',', quotechar='"')
        progress=0
        for line in tempReader:
            if line[0]=='Ref_Date':
                print "skipping header data"
                pass #skip the header
                save=False
            else:
                year=line[0]
                region=line[1]
                sex=line[2]
                age=line[3]
                value=line[6]
                save=True
            if save:
                self.dbWrite(year,region,sex,age,value)
            progress+=1
            print "Parsed {0} lines".format(progress)
    
    def makeTables(self):
        self.conn.execute('''CREATE TABLE demographics
            (year int NOT NULL,
            prov varchar(4) NOT NULL,
            sex int NOT NULL,
            age int NOT NULL,
            pop int NOT NULL
            );''')
        
    def dbWrite(self,year,region,sex,age,value):
        yeardb=int(year)
        regiondb=self.provMap[region] #note: initialized dict
        sexdb=self.sexMap[sex]#note: initialized dict
        agedb=self.mapAge(age)#note: function
        try:
            valuedb=float(value)
        except:
            valuedb=-999 #error case, looks like when value is '..' this happens
        #input is :['54'] or ['12','41']
        try:
            agedb=int(agedb[0])
        except:
            agedb=-999
        info=[yeardb,regiondb,sexdb,agedb,valuedb]
        if agedb==-999: #the error case
            #agedb=int(agedb[0])
            print "Badly formatted age {0}".format(info)
        else:
            print "saving {0}".format(info)
            self.conn.execute('INSERT INTO demographics (year,prov,sex,age,pop) VALUES (?,?,?,?,?)',(yeardb,regiondb,sexdb,agedb,valuedb))

    def mapAge(self,inStr):
        #possibilities:
        #   '%d years'
        #   '%d to %d years'
        #   'All ages'
        if inStr=='All ages':
            return [999]
        elif 'years' in inStr:
            results=self.yearsRe.findall(inStr)#grab all chunks of numbers and toss em in a list
            return results #return [digits,digits] or [digits] NOTE: these need to be int cast
        else:
            return [-999]
            #This looks like an error case, usually blank or badly formatted lines
            print "\nERROR..... ? Like maybe\n?"
            
def makeDB():
    Parser=Weather2sql(True,'test.db')
    Parser.makeTables()
    path="/home/Red/Projects/MagnoScrape/temperature/"
    #filename="eng-daily-0101{0}-1231{0} (4).csv".format(year)
    filenames=glob.glob(path+"eng*")
    for filename in filenames:
        print "filename: {0}".format(filename)
        Parser.parseCsv(filename)
    Parser.conn.commit() #Commit all the changes to disk

def populatePop():
    Parser=Weather2sql(True,'test.db')
    filename="population/stripped_pop_map.csv"
    Parser.parsePop(filename)

def distMapper():
    Parser=Weather2sql(True,'test.db')
    Parser.genDistMap()

def fixNines():
    Parser=Weather2sql(True,'test.db')
    Parser.fuzzWeather()

def addDemographics():
    demo=Demographics(True,'test.db')
    demo.makeTables()
    demo.parseAge()
    demo.conn.commit()
    

if __name__=="__main__":
    print "Start of Script"
    addDemographics()
    

from datetime import datetime,date
import sqlite3
from mpl_toolkits.basemap import Basemap, cm
import numpy as np
import matplotlib.pyplot as plt

#Commands to fix your python if stuff doesn't run:
#sudo easy_install -U distribute
#sudo pip install --upgrade matplotlib
    #You may also need to instal the GEOS library, google that. (I compiled from source)
#sudo pip install --upgrade basemap

#if the above don't run, try and install pip and easy_install (through apt-get)
#if you are in windows you may need to google stuff to get this to work

#NOTE: Python has this cool thing I'm trying to use to do line wraps.
#   If you have to change this and see a string ending with a '/'
#   that means it's wrapping to the next line, it just makes the sql nicer to read

class Visualize():
    def __init__(self,sqlServer="test.db"):
        print "Initialized Visualize Class"
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

    def getData(self,d): #date should be in the form of "yyyy-mm-dd"
        print "getting data" 
        #Below doesn't work, the date also needs to be inside a  2nd set of quotes when querying from the sqlitebrowser
        self.c.execute('SELECT idx,max_temp,min_temp,mean_temp,total_snow,ground_snow FROM daily WHERE weather_date=? AND max_temp!=999.0',(d,))
        data=self.c.fetchall()
        print "DEBUG:\n\n {0} \n\n".format(data)
        return data

    def getStation(self,idx):
        self.c.execute('SELECT station_name,province,latitude,longitude,elevation FROM station WHERE idx=?',(idx,))
        result=self.c.fetchall()
        return result #station/province/lat/long/elv

    def displayData(self): #Debug function
        print "Displaying Data"
        year=2012
        month=1
        day=1
        d=date(year,month,day) #she wants the D:/
        self.getData(d)

    def getIdxs(self,year):
        #query the colddays database and get all index's that have at least 30 days of an average temperature less than -10.
        # then add the population up for all of these areas
        self.c.execute('SELECT idx, colddays FROM coldcount WHERE colddays>30 AND year=?',(year,))
        results=self.c.fetchall()
        marketSize=0
        filename="MagnoMarket_{0}.csv".format(year)
        yearData=open(filename,'wb')
        for index in results: 
            idx=int(index[0])
            colddays=int(index[1])
            self.c.execute('SELECT station_name, province, pop_2011, latitude, longitude FROM stations WHERE idx=?',(idx,))
            result=self.c.fetchall()
            station_name=result[0][0]
            province=result[0][1]
            pop=result[0][2]
            lat=result[0][3]
            lon=result[0][4]
            if pop==None:
                pop=0
            print "{0},{1},{2},{3},{4},{5}".format(station_name,province,pop,lat,lon,colddays)
            yearData.write("{0},{1},{2},{3},{4},{5}\n".format(station_name,province,pop,lat,lon,colddays))
        yearData.close()
            

class CountCold():
    def __init__(self,sqlServer="test.db"):
        print "Initialized Visualize Class"
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

    def createTable(self):
        self.conn.execute("CREATE TABLE coldcount (idx int NOT NULL,colddays int,year int);")

    def countDays(self,dateStart,dateEnd,threshold,idx):
        #self.c.execute("SELECT weather_date,mean_temp FROM daily WHERE idx=? AND mean_temp<? AND weather_date BETWEEN '2011-01-01' AND '2011-12-31'",(idx,threshold,dateStart,dateEnd))
        self.c.execute("SELECT weather_date,mean_temp FROM daily WHERE idx=? AND mean_temp<? AND weather_date BETWEEN '2011-01-01' AND '2011-12-31'",(idx,threshold,))
        result=self.c.fetchall()
        return len(result)

    def ColdDays(self,year,dateStart,dateEnd,threshold):
        self.c.execute('SELECT idx FROM stations')
        result=self.c.fetchall()
        print "GOT {0} INDEX'S".format(len(result))
        for idx in result:
            idx=int(idx[0])
            colddays=self.countDays(dateStart,dateEnd,threshold,idx)
            self.conn.execute('INSERT INTO coldcount (idx,colddays,year) VALUES (?,?,?)',(idx,colddays,year))
            print "Counted {0} days for index {1}".format(colddays,idx)
        self.conn.commit() #Commit all the changes to disk

        #SQL SELECT ALL STATIONS INDEXs, PER YEAR (see above).
        #THEN LOOP OVER countDays THEN store in a new table?

class DispMap():
    def __init__(self,sqlServer="test.db"):
        print "Initialized Visualize Class"
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

    def getDayWeather(self,wDay,threshold=None):
        if threshold == None:
            self.c.execute('SELECT daily.idx,daily.max_temp,daily.min_temp,daily.mean_temp,daily.ground_snow,stations.latitude,stations.longitude,stations.pop_2011 FROM daily INNER JOIN stations ON daily.idx=stations.idx WHERE daily.weather_date=?',(wDay,)) 
        else:
            self.c.execute('SELECT daily.idx,daily.max_temp,daily.min_temp,daily.mean_temp,daily.ground_snow,stations.latitude,stations.longitude, stations.pop_2011 FROM daily INNER JOIN stations ON daily.idx=stations.idx WHERE daily.weather_date=? AND daily.mean_temp<? ',(wDay,threshold)) 
        result=self.c.fetchall()
        idx=[]
        maxtemp=[]
        mintemp=[]
        meantemp=[]
        groundSnow=[]
        lat=[]
        lon=[]
        pop=[]
        for line in result:
            #idx.append(line[0])
            #maxtemp.append(line[1])
            #mintemp.append(line[2])
            meantemp.append(line[3])
            #groundSnow.append(line[4])
            lat.append(line[5])
            lon.append(line[6])
            pop.append(line[7])
        return (idx,maxtemp,mintemp,meantemp,groundSnow,lat,lon,pop)

    def baseMap(self,wDay,coldThreshold=None):
        #YYYY-MM-DD
        weatherMap=self.getDayWeather(wDay,coldThreshold)
        #population=sum(weatherMap[7])
        #left lower, left upper, right upper, right lower
        fig = plt.figure(figsize=(16,16))
        latcorners=[45.0,85.0,85.0,45.0]
        loncorners=[-145.0,-145.0,-55.0,-55.0] #-50 to -145 small negative on right
        self.m = Basemap(projection='merc',lon_0=-100,lat_0=50,
                    resolution='i',area_thresh=100.0,
                    llcrnrlon=-150.0, llcrnrlat=40.0,
                    urcrnrlon=-50.0,  urcrnrlat=80.0)
        # draw coastlines, state and country boundaries, edge of map, etc...
        self.m.drawcoastlines()
        self.m.drawstates()
        self.m.drawcountries()
        self.m.fillcontinents(color='coral')
        m=self.m #take pre-rendered map (Didn't work so I just rename the above. Could be a deep/shallow copy problem)
        popSum=self.holySum(weatherMap[7])
        x,y=m(weatherMap[6],weatherMap[5])#lon,lat
        m.plot(x, y, 'bo', markersize=12) #I'd love to change the circle and colour size based on population
        plt.title(' Block Heater usage on {0}, Market Size={1}'.format(wDay,popSum), size=22)
        #plt.show()
        plt.savefig("{0}_market.png".format(wDay),bbox_inches='tight')
        plt.close() #this should fix the warning of the memory leak

    def holySum(self,holyList): #I can't believe I need to do this.... grrrr dam holes.
        count=0
        for pop in holyList:
            if pop != None:
                try:
                    count+=int(pop)
                except: #I hate working with shitty sql data. Something that is just whitespace gets through
                    print "THIS THING, what is it? -> {0}".format(pop)
        return count
        
    def genTimelapse(self,resume=None,threshold=-10.0):
        print "Selecting all dates found in table"
        if resume == None: #optional resume due to segfault (unsure of reason)
            self.c.execute('SELECT DISTINCT weather_date FROM daily')
        else:
            self.c.execute('SELECT DISTINCT weather_date FROM daily WHERE weather_date>?',(resume,))
        dates=self.c.fetchall()
        total=len(dates)
        current=0
        print "Starting to parse by Day"
        print "_"*60
        for day in dates:
            print "Compiling data from date {0}".format(day)
            print "\t {0}/{1} Complete".format(current,total)
            current+=1
            wDay=(day[0])
            self.baseMap(wDay,threshold)

    def popMap(self,wDay,threshold=None):
        if threshold==None:
            print "please give me a threshold to check against"

def showMap():
    disp=DispMap()
    #resume=date(2013,1,1) #debug date for checking and resuming a halted run
    resume=None #for a full run use none
    disp.genTimelapse(resume)
    #disp.baseMap(resume,-10.0)
        
def visu():
    visu=Visualize()
    years=[2010,2011,2012,2013,2014]
    for year in years:
        visu.getIdxs(year)

def formatDate(year,month,day): #literally just a reminder
    return date(year,month,day)

def countAllTheDays():
    Counter=CountCold()
    Counter.createTable()
    threshold=-10.0
    years=[2010,2011,2012,2013,2014]
    for year in years:
        print "PARSING YEAR {0}".format(year)
        start=date(year,01,01)
        end=date(year,12,31)
        Counter.ColdDays(year,start,end,threshold)
        print "FINISHED YEAR {0}".format(year)
    
if __name__=="__main__":
    print "Start of Program"
    #countAllTheDays()
    #visu()
    showMap()

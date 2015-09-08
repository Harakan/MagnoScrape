import wget
import re
from bs4 import BeautifulSoup
from urllib2 import urlopen

#timeframe= [2-> monthy, 3-> Daily] (we want 2)
#Prov=[AB,BC,SK,MB,ON,QC,NB,NT,PE,NU,YT,NS]
#StationID= Count up from 1 until we 404 5 in a row (or we hit 50000 in a province)
#dlyRange=2004-09-02|2015-04-15
#Year=[current year]
#Month=1&Day=01 (always since this seems to make it work)
#wget.download('http://www.google.com/url?q=http%3A%2F%2Fclimate.weather.gc.ca%2FclimateData%2Fbulkdata_e.html%3Fformat%3Dcsv%26stationID%3D42967%26Year%3D2014%26Month%3D1%26Day%3D1%26timeframe%3D2%26submit%3DDownload%2BData&sa=D&sntz=1&usg=AFQjCNEwIlyJ0esbczYOIhDB-mqKb2QiJg')
    #http://climate.weather.gc.ca/climateData/bulkdata_e.html?format={0}&stationID={1}&Year={2}&Month=1&Day=1&timeframe=2&submit=Download+Data

def getTemps(stationID,Year):
    try:
        url="http://climate.weather.gc.ca/climateData/bulkdata_e.html?format=csv&stationID={0}&Year={1}&Month=1&Day=1&timeframe=2&submit=Download+Data".format(stationID,Year)
        filename = wget.download(url)
        print "got File: {0}".format(filename)
    except:
        print "couldn't get url {0}".format(url)

def getAllTemps(stationIDs=0):
    print "Starting Climate Downloader"
    fstation=open("canStation.csv")
    for year in range(2014,2015): #Not actually 2015 though
        if stationIDs==0:
            for station in fstation:
                getTemps(int(station),year)
        else:
            for station in stationIDs:
                getTemps(int(station),year)

def getCanStations():
    print "Parsing out StationIDs"
    output=open("canStation.csv",'w')
    reID=re.compile("value=\"\d+\"")
    reID=re.compile("\d+")

    for pages in range(0,24):
        start=pages*100+1
        website="http://climate.weather.gc.ca/advanceSearch/searchHistoricDataStations_e.html?searchType=stnProv&timeframe=1&lstProvince=&StartYear=1840&EndYear=2015&optLimit=specDate&Year=2000&Month=1&Day=1&selRowPerPage=100&cmdProvSubmit=Search&startRow={0}".format(start)
        try:
            page = urlopen(website).read()
            soup=BeautifulSoup(page, "html.parser") #lxml || html.parser
        except:
            print "couldn't open page num: {0}".format(pages)
        stations=soup.find_all(attrs={"name": "StationID"})
        station=reID.findall(str(stations)) #regex out the station ID then store at the end of the station list
        print "writing to file for {0}".format(pages)
        for stationNum in station:
            output.write("{0}\n".format(int(stationNum)))



if __name__ == "__main__":
    #wget.download("http://climate.weather.gc.ca/climateData/bulkdata_e.html?format=csv&stationID=42967&Year=2014&Month=1&Day=1&timeframe=2&submit=Download+Data")
    #the above works, just testing the automated below:
    
    #NOTE: you need the canStation.csv file to run getAllTemps
    #getCanStations()
    getAllTemps()





#def getCanStations(filename):
#    print "Parsing out StationID from {0}"
#    fid=open(filename,'r')
#    output=open("canStation.csv",'w')
#    for line in fid:
#        if station != []:
#            print "found station: {0}, saving to canStation.csv".format(station[0])
#            output.write("{0}\n".format(station[0]))
#        

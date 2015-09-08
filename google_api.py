import re
import urllib2

# Import Custom libraries
from BeautifulSoup import BeautifulSoup, BeautifulStoneSoup

def render_google_uri(idict):
    '''
    Render the appropriate Google Map Api request uri
    '''
    base_url = "http://maps.googleapis.com/maps/api/geocode/xml?"
    options = [(key,re.sub("\s+", "+", value)) for (key, value) in idict.items()]

    options = map(lambda x: "=".join(x), options)
    options = "&".join(options)
    url = base_url + options
    return url

def get_street_position(*args):
    '''
    Longitude and Latitude from Street Address
    '''
    def is_result(itag):
        if itag.name == "result":
            if itag.type.text == "locality":
                return True
        return False

    ret_list = []
    for address in args:
        google_api_dict = \
        {
            "address" : address,
            "sensor"  : "false",
        }
        request_uri = render_google_uri(google_api_dict)
        request = urllib2.Request(request_uri, None, {})

        try:
            response = urllib2.urlopen(request)
            the_page = response.read()
        except Exception:
            the_page = ""

        if the_page:
            pool = BeautifulStoneSoup(the_page)
            result = pool.find(is_result)

            if result:
                bounds = result.find("bounds")

                if bounds:

                    cur_dict = \
                    {
                        "Address"        : address,
                        "Google Address" : result.formatted_address.text,
                        "Bounds"         : \
                        {
                            "SW" :
                            {
                                "Longitude" : bounds.southwest.lng.text,
                                "Latitude"  : bounds.southwest.lat.text
                            },
                            "NE" :
                            {
                                "Longitude" : bounds.northeast.lng.text,
                                "Latitude"  : bounds.northeast.lat.text
                            }
                        }
                    }
                    ret_list += [cur_dict]

    return ret_list

def givePos(inName):
    inputs=get_street_position(inName)
    lat=float(inputs[0]["Bounds"]["SW"]["Latitude"])+float(inputs[0]["Bounds"]["NE"]["Latitude"])
    lat=lat/2.0
    lon=float(inputs[0]["Bounds"]["SW"]["Longitude"])+float(inputs[0]["Bounds"]["NE"]["Longitude"])
    lon=lon/2.0
    return(lat,lon)

if __name__ == "__main__":
    print get_street_position("Sydney", "Marrakech")
    sk_time = get_street_position("Saskatoon (SK) Canada")
    pos=givePos("Prince Albert (SK) Canada")
    print sk_time
    print pos
    #Google Address
    #Bounds
    #   SW
    #       Latitude
    #       Longitude
    #   NE
    #       "" (above)
    #   Address

#{'Google Address': u'Saskatoon, SK, Canada', 'Bounds': {'SW': {'Latitude': u'52.0695480', 'Longitude': u'-106.7758751'}, 'NE': {'Latitude': u'52.2097610', 'Longitude': u'-106.5175790'}}, 'Address': 'Saskatoon (SK) Canada'}






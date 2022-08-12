import random
import datetime
import urllib.request
import json
#from mySQLconnection import get_real_data

API_KEY = "af41cb6c0847abdfe46f42e8177035b6"
BASE_URL = "https://api.openweathermap.org/data/2.5"
# https://api.openweathermap.org/data/2.5/onecall?lat=48.8534&lon=2.3488&exclude=current,minutely,hourly,alerts&appid=1fa9ff4126d95b8db54f3897a208e91c&units=metric

# This method is used to fetch the accident records for last minute
def get_weather_records():
    latitude = 46.0748
    longitude = 11.1217
    url = BASE_URL + "/weather?lat=" + str(latitude) + "&lon=" + str(longitude) + "&exclude=current,minutely,hourly,alerts&units=metric&appid=" + API_KEY
    #print(url)
    accident_list = urllib.request.urlopen(url).read()
    my_json = accident_list.decode('utf8').replace("'", '"')
    data = json.loads(my_json)
    #print("Daily: ", data["daily"])
    #print("Response: ", my_json)
    #return json.loads(my_json)
    return my_json

# fetch real data from AWS and populate fake data
def fetch_weather_data() -> dict:
    #accident_data = get_real_data()

    #lat = accident_data['y_gps']
    #long = accident_data['x_gps']

    lat = random.randint(0, 100)
    long = random.randint(0, 50)

    level = random.randint(0, 4)

    # ct stores current time
    ct = datetime.datetime.now()
    # print("current time:-", ct)
    n = random.randint(0, 10)
    # Add 1 minutes to datetime object
    final_time = ct + datetime.timedelta(minutes=n)

    timestamp = str(final_time)

    duration = random.randint(0, 10)

    return {
        'duration' : duration,
        'latitude': lat,
        'longitude': long,
        'level': level,
        'time': timestamp
    }

# print(generate_accident())
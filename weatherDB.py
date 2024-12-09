import os
import requests
import sqlite3
from datetime import datetime

API_KEY = os.environ["API_KEY"]

def check_weather(city):

    main_data = {}
    weather_data = []

    lat_lon = get_lat_lon(city)
    
    if lat_lon is not None:

        lat, lon = lat_lon

        response = requests.get(f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API_KEY}&units=metric")
        
        if (response.status_code == 200):
            
            data = response.json()

            main_data = {
                "key": f'{data["id"]}|{data["dt"]}',
                "date": datetime.fromtimestamp(data["dt"]).strftime('%Y-%m-%d'),
                "time": datetime.fromtimestamp(data["dt"]).strftime('%H:%M:%S'),
                "name": data["name"],
                "coord_lon": data["coord"]["lon"],
                "coord_lat": data["coord"]["lat"],
                "base": data["base"],
                "main_temp": data["main"]["temp"],
                "main_feels_like": data["main"]["feels_like"],
                "main_temp_min": data["main"]["temp_min"],
                "main_temp_max": data["main"]["temp_max"],
                "main_pressure": data["main"]["pressure"],
                "main_humidity": data["main"]["humidity"],
                "main_sea_level": data["main"]["sea_level"],
                "main_grnd_level": data["main"]["grnd_level"],
                "visibility": data["visibility"],
                "wind_speed": data["wind"]["speed"],
                "wind_deg": data["wind"]["deg"],
                "clouds_all": data["clouds"]["all"],
                "sys_type": data["sys"]["type"],
                "sys_id": data["sys"]["id"],
                "sys_country": data["sys"]["country"],
                "sys_sunrise": datetime.fromtimestamp(data["sys"]["sunrise"]).strftime('%H:%M:%S'),
                "sys_sunset": datetime.fromtimestamp(data["sys"]["sunset"]).strftime('%H:%M:%S'),
                "timezone": data["timezone"],
                "id": data["id"],
                "cod": data["cod"]
            }
                    
            for item in data['weather']:       
                weather_data.append({   
                    "key": f'{data["id"]}|{ item["id"]}',
                    "foreign_key": f'{data["id"]}|{data["dt"]}',
                    "weather_id": item["id"],
                    "weather_main": item["main"],
                    "weather_description": item["description"],
                    "weather_icon": item["icon"]
                })
        else: 
            print("Error fetching current weather for '{city}")

    return main_data, weather_data

def get_lat_lon (city):

        response = requests.get(f"http://api.openweathermap.org/geo/1.0/direct?q={city}&limit=5&appid={API_KEY}")

        if (response.ok):
            data = response.json()
            latitude = data[0]['lat']
            longitude = data[0]['lon']
            return (latitude,longitude)
        else: 
            print(f"Error fetching coordinates for '{city}'")
            return None

def update_weather_database(main_data, weather_data):

    if (main_data) and (weather_data):
        cur.execute("""
                    INSERT OR REPLACE INTO Main_data (key, date, time, name, coord_lon, coord_lat, base, main_temp, main_feels_like, main_temp_min, main_temp_max, main_pressure, main_humidity, main_sea_level, main_grnd_level, visibility, wind_speed, wind_deg, clouds_all, sys_type, sys_id, sys_country, sys_sunrise, sys_sunset, timezone, id, cod)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,tuple(main_data.values()))
        
        for i, item in enumerate(weather_data):
                cur.execute(""" 
                    INSERT OR REPLACE INTO Weather_data (key, foreign_key, weather_id, weather_main, weather_description, weather_icon)
                    VALUES (?,?,?,?,?,?)
        """,tuple(weather_data[i].values()))


cities = ['Paris', 'London', 'Amsterdam', 'Berlin','Brighton','Bordeaux','Lisbon','Tokyo','Jakarta']

conn = sqlite3.connect("Weather.db")

cur = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS Main_data (    
    key TEXT PRIMARY KEY,
    date DATE,
    time TIME,        
    name TEXT,       
    coord_lon REAL,
    coord_lat REAL,
    base TEXT,
    main_temp REAL,
    main_feels_like REAL,
    main_temp_min REAL,
    main_temp_max REAL,
    main_pressure REAL,
    main_humidity REAL,
    main_sea_level REAL,
    main_grnd_level REAL,
    visibility REAL,
    wind_speed REAL,
    wind_deg REAL,
    clouds_all REAL,
    sys_type INTEGER,
    sys_id INTEGER,
    sys_country TEXT,
    sys_sunrise TIME,
    sys_sunset TIME,
    timezone INTEGER,
    id INTEGER,
    cod INTEGER
)
""")

cur.execute("""
CREATE TABLE IF NOT EXISTS Weather_data (
    key TEXT PRIMARY KEY,
    foreign_key TEXT,        
    weather_id INTEGER,
    weather_main TEXT,
    weather_description TEXT,
    weather_icon TEXT,
    FOREIGN KEY(foreign_key) REFERENCES Main_data(key)  
)
""")

for city in cities:

    main_data, weather_data  = check_weather(city)
    update_weather_database(main_data,weather_data)

conn.commit()
conn.close()
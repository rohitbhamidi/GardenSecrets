import requests
import pandas as pd
import singlestoredb as s2

class WeatherData:
    def __init__(self, db_host, db_user, db_password, db_name):
        self.db_host = db_host
        self.db_user = db_user
        self.db_password = db_password
        self.db_name = db_name

    def _get_ip_info(self):
        response = requests.get('https://ipinfo.io/json')
        data = response.json()
        self.ip_address = data['ip']
        self.latitude, self.longitude = data['loc'].split(',')
    
    def _fetch_weather_data(self):
        base_url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": self.latitude,
            "longitude": self.longitude,
            "current": "temperature_2m,is_day,rain,cloud_cover,wind_speed_10m"
        }
        endpoint = base_url + f'?latitude={params["latitude"]}&longitude={params["longitude"]}&current={params["current"]}'
        response = requests.get(endpoint)
        self.weather_data = response.json()

    def _write_to_db(self):
        # Connect to the database
        connection = s2.connect(
            host=self.db_host,
            user=self.db_user,
            password=self.db_password,
            database=self.db_name
        )
        cursor = connection.cursor()

        # Assuming table 'weather_data' exists with appropriate columns
        weather_info = self.weather_data['current']
        insert_query = """
        INSERT INTO weather_data (ip_address, latitude, longitude, temperature, is_day, rain, cloud_cover, wind_speed)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_query, (
            self.ip_address, 
            self.latitude, 
            self.longitude, 
            weather_info['temperature_2m'], 
            weather_info['is_day'], 
            weather_info['rain'], 
            weather_info['cloud_cover'], 
            weather_info['wind_speed_10m']
        ))
        
        connection.commit()
        cursor.close()
        connection.close()

    def process_weather_data(self):
        self._get_ip_info()
        self._fetch_weather_data()
        # self.write_to_db()
        print("Process completed successfully!")
        return self.weather_data
        
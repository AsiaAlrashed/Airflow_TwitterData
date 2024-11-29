import requests


class WeatherExtractor:

    def __init__(self, API_key, API_url):
        self.API_key = API_key
        self.API_url = API_url

    def fetch_weather_data(self, location):
        params = {"q": location, "appid": self.API_key}
        response = requests.get(self.API_url, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            response.raise_for_status()

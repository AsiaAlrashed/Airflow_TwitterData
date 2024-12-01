from prefect import task, flow
from datetime import datetime, timedelta
from .Weather_Extractor import WeatherExtractor
from .Weather_Transformer import WeatherTransformer
from .Weather_Loader import WeatherLoader
from .Weather_Analyzer import WeatherAnalyzer


class WeatherPipeline:

    def __init__(self, API_url, API_key, db_path, location):
        self.API_url = API_url
        self.API_key = API_key
        self.db_path = db_path
        self.location = location
        self.extractor = WeatherExtractor(self.API_key, self.API_url)
        self.transformer = WeatherTransformer()
        self.loader = WeatherLoader(self.db_path)

    @task
    def extract_task(self):
        data = self.extractor.fetch_weather_data(self.location)
        return data

    @task
    def transform_task(self, data):
        transformed_data = self.transformer.transform_data(data)
        return transformed_data

    @task
    def load_task(self, transformed_data):
        self.loader.insert_weather_data(self.location, transformed_data)

    @task
    def quruer_weather_date_location(self):
        return self.loader.quruer_weather_date_location(self.location)

    @task
    def analyzer_task(data, location):
        analyzer = WeatherAnalyzer(data, location).analyzer()
        return analyzer

    @flow
    def run_pipeline(self):
        data = self.extract_task()
        transformed_data = self.transform_task(data)
        self.load_task(transformed_data)
        quruer = self.quruer_weather_date_location()
        summary = self.analyzer_task(quruer, self.location)
        return summary

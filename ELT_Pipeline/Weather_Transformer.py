import pandas as pd


class WeatherTransformer:

    def __init__(self):
        self.data_frame = pd.DataFrame()

    def transform_data(self, data):
        """
        Process and structure the JSON data
        """
        records = []
        for item in data["list"]:
            # Extract date information
            date_str = item["dt_txt"]
            date_obj = pd.to_datetime(date_str)

            record = {
                "date": date_str,
                "year": date_obj.year,
                "month": date_obj.month,
                "day": date_obj.days_in_month,
                "hour": date_obj.hour,
                "temperature": item["main"]["temp"],
                "humidity": item["main"]["humidity"],
                "weather": item["weather"][0]["description"],
                "wind_speed": item["wind"]["speed"],
            }
            records.append(record)

        self.data_frame = pd.DataFrame(records)
        return self.data_frame

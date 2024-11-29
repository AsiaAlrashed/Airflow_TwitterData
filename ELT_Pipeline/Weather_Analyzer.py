import pandas as pd
import plotly.express as px


class WeatherAnalyzer:

    def __init__(self, data_json, location_name):
        self.data_json = data_json
        self.location_name = location_name

    def analyzer(self):

        df = pd.DataFrame(
            self.data_json,
            columns=[
                "location_id",
                "date",
                "temperature",
                "humidity",
                "weather",
                "wind_speed",
                "year",
                "month",
                "day",
                "hour",
            ],
        )

        avg_temp = df["temperature"].mean()
        avg_humidity = df["humidity"].mean()
        summary = f"Average Temperature: {avg_temp:.2f} °C, Average Humidity: {avg_humidity:.2f}%"

        fig = px.line(
            df,
            x="date",
            y="temperature",
            title=f"Temperature Trends for {self.location_name}",
        )
        fig.write_image("./static/chart.png")

        fig = px.line(
            df,
            x="date",
            y="humidity",
            title=f"humidity Trends for {self.location_name}",
        )
        fig.write_image("./static/chart1.png")

        return summary

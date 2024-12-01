from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from ELT_pipeline.Weather_Pipeline import WeatherPipeline
from ELT_pipeline.Weather_Analyzer import WeatherAnalyzer

# pipeline = WeatherPipeline(
#     "http://api.openweathermap.org/data/2.5/forecast",
#     "6f8511ad484589a6fd579cd02af41194",
#     "weather_data.db",
# )


# Initialize FastAPI app
app = FastAPI()

# Set up static files and templates directory
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="template")


@app.get("/", response_class=HTMLResponse)  # Correct
async def root():
    return "Hello, World!"


@app.get("/location/{location_name}", response_class=HTMLResponse)
async def get_location_data(request: Request, location_name: str):
    """Endpoint to display weather data analysis for a specific location."""
    pipeline = WeatherPipeline(
        "http://api.openweathermap.org/data/2.5/forecast",
        "6f8511ad484589a6fd579cd02af41194",
        "weather_data.db",
        location_name,
    )

    summary = pipeline.run_pipeline()

    return templates.TemplateResponse(
        "chart_page.html",
        {
            "request": request,
            "location": location_name,
            "summary": summary,
        },
    )

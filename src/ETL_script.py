import requests
import os
from dotenv import load_dotenv
import json
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, FloatType, LongType, DoubleType


load_dotenv()
weather_api_key = os.getenv("WEATHER_API_KEY")
postgresql_password = os.getenv("POSTGRESQL_PASSWORD")

def extract(cities):

    url = f"http://api.openweathermap.org/data/2.5/forecast?q={cities}&appid={weather_api_key}&units=metric"

    response = requests.get(url)

    response.raise_for_status()

    return response.json()


def transform(data_to_transform):

    def get_solar_panel_status(temp_f):
        if temp_f >= 90:
            return "Exceptional"
        elif temp_f >= 80:
            return "High"
        elif temp_f >= 70:
            return "Average"
        elif temp_f >= 50:
            return "Budget"
        else:
            return "Sub-par"
        
    def get_wind_direction(deg):
        if deg is None:
            return "Unknown"
        
        deg = deg % 360 
        
        if 337.5 <= deg or deg < 22.5:
            return "North"
        elif 22.5 <= deg < 67.5:
            return "North-East"
        elif 67.5 <= deg < 112.5:
            return "East"
        elif 112.5 <= deg < 157.5:
            return "South-East"
        elif 157.5 <= deg < 202.5:
            return "South"
        elif 202.5 <= deg < 247.5:
            return "South-West"
        elif 247.5 <= deg < 292.5:
            return "West"
        elif 292.5 <= deg < 337.5:
            return "North-West"

    transformed_data = pd.json_normalize(
        data_to_transform,
        record_path=["list"],
        meta=[
            ["city", "id"],
            ["city", "name"],
            ["city", "country"],
            ["city", "population"],
        ],
        errors="ignore"
    )

    transformed_data['weather_main'] = transformed_data['weather'].apply(
        lambda x: x[0]['main'] if isinstance(x, list) else None
    )

    transformed_data['weather_desc'] = transformed_data['weather'].apply(
        lambda x: x[0]['description'] if isinstance(x, list) else None
    )

    transformed_data = transformed_data.rename(columns={
        "dt_txt": "datetime",
        "main.temp": "temperature",
        "main.temp_min": "temp_min",
        "main.temp_max": "temp_max",
        "main.pressure": "pressure",
        "main.sea_level": "sea_level",
        "main.humidity": "humidity",
        "city.name": "city",
        "city.id": "city_id",
        "city.country": "country",
        "city.population": "population",
        "clouds.all" : "cloud_percentage",
        "wind.speed" : "wind_speed",
        "wind.deg" : "wind_degrees",
        "wind.gust" : "gust"
    })
    

    transformed_data["temp_fahrenheit"] = (transformed_data["temperature"] * 1.8) + 32

    transformed_data["status"] = transformed_data["temperature"].apply(
        lambda x: "Very Hot" if x > 35 else ("warm" if x > 25 else "cool")
    )

    transformed_data["cloud_label"] = transformed_data["cloud_percentage"].apply(
        lambda x: "Sunny" if x <= 20 else ("Partly Cloudy" if x <= 60 else "Overcast")
    )

    transformed_data["Solar_Power_Level"] = transformed_data["temp_fahrenheit"].apply(get_solar_panel_status)

    transformed_data["wind_direction"] = transformed_data["wind_degrees"].apply(get_wind_direction) 

    cleaned_data = transformed_data[[
        "city", "country",
        "datetime", "weather_main",
        "temperature",
        "temp_fahrenheit", "status",
        "cloud_percentage", "cloud_label",
        "Solar_Power_Level", "wind_speed",
        "wind_degrees", "wind_direction",
        "weather_desc", "city_id",
        "humidity", "temp_min",
        "temp_max", "pressure"
    ]].copy()

    cleaned_data["datetime"] = pd.to_datetime(cleaned_data["datetime"])
    
    return cleaned_data


def load(loaded_sparkdata):
    
    spark = (
        SparkSession.builder
            .master("local[*]")
            .appName("ELT-Batch-Processing")
            .config("spark.jars", "./postgresql-42.7.9.jar")
            .getOrCreate()
    )

    schema = StructType([
        StructField("city", StringType(), False),
        StructField("country", StringType(), False),
        StructField("datetime", TimestampType(), False),
        StructField("weather_main", StringType(), False),
        StructField("temperature", DoubleType(), False),
        StructField("temp_fahrenheit", FloatType(), False),
        StructField("status", StringType(), True),
        StructField("cloud_percentage", LongType(), True), 
        StructField("cloud_label", StringType(), False),
        StructField("Solar_Power_Level", StringType(), True),
        StructField("wind_speed", FloatType(), False),
        StructField("wind_degrees", LongType(), False),
        StructField("wind_direction", StringType(), False),
        StructField("weather_desc", StringType(), False),
        StructField("city_id", LongType(), False),
        StructField("humidity", LongType(), False),
        StructField("temp_min", FloatType(), False),
        StructField("temp_max", FloatType(), False),
        StructField("pressure", LongType(), False)
    ])

    transformed_to_spark = spark.createDataFrame(loaded_sparkdata, schema=schema)

    db_url = "jdbc:postgresql://localhost:5432/WeatherDB"
    db_properties = {
        "user" : "postgres",
        "password" : postgresql_password,
        "driver" : "org.postgresql.Driver"
    }

    transformed_to_spark.write.jdbc(url=db_url, table="weather_report", mode="append", properties=db_properties)

    print("succesfully loaded to postgresql")

    spark.stop()
    


if __name__ == "__main__":
    print("Starting ELT process...")

    raw_data = extract("Manila")

    cleaned_df = transform(raw_data)

    load(cleaned_df)

    print("Process Complete!")
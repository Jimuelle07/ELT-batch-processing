import requests
import API
import json
import pandas as pd


# if city_name:
#    with open(f"{city_name}_weather_data.json", "w") as f:
#        json.dump(data, f, indent=3)
# else: 
#    print("There is no city name.")

def extract(cities):

        url = f"http://api.openweathermap.org/data/2.5/forecast?q={cities}&appid={API.weather_api_key}&units=metric"
        weather_data_response = requests.get(url)

        try:
            extracted_data = weather_data_response.json()
            
        except json.JSONDecodeError as e:
                print(f"error in data handling {e}")

        return extracted_data


def transform(data_to_transform):

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

    transformed_data['weather_desc'] = transformed_data['weather'].apply(lambda x: x[0]['description'] if isinstance(x, list) else None)

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
        "city.population": "population"
    })

    cleaned_data = transformed_data[[
        "city", "country", "datetime", "temperature", "weather_desc", 
        "city_id", "humidity", "temp_min", "temp_max", "pressure", 
        "sea_level", "population"
    ]]
    
    return cleaned_data









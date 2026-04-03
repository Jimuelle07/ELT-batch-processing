import requests
import API
import json
import pandas as pd


# if city_name:
#    with open(f"{city_name}_weather_data.json", "w") as f:
#        json.dump(data, f, indent=3)
# else: 
#    print("There is no city name.")


## ph_cities = ["Manila,PH", "Calapan,PH", "Batangas,PH", "Cebu City,PH", "Davao,PH"]



def ETL(name_of_the_city):

    def extract(cities):

        ## for city in cities:
        url = f"http://api.openweathermap.org/data/2.5/forecast?q={city}&appid={API.weather_api_key}&units=metric"
        weather_data_response = requests.get(url)

        try:
            extracted_data = weather_data_response.json()
            
            ## print(json.dumps(data, indent=4))

        except json.JSONDecodeError as e:
                print(f"error in data handling {e}")

        return extracted_data

    def transform(data_to_transform):
        transformed_data =  pd.json_normalize(data_to_transform)
        






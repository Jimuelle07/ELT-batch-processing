import requests  
import src/API.py

r = requests.get(f"http://api.openweathermap.org/data/2.5/forecast?id=524901&appid={API.weather_api_key}")

r.json()
print(r)
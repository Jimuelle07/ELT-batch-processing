import requests  
import api_key

r = requests.get(f"http://api.openweathermap.org/data/2.5/forecast?id=524901&appid={api_key}")

r.json()
print(r)
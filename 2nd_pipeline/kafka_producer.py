import requests
from kafka import KafkaProducer
from json import dumps
import json 

producer = KafkaProducer(bootstrap_servers=['localhost:9099'])


url = "https://free-nba.p.rapidapi.com/games"

querystring = {"per_page":"25","page":"0"}

headers = {
    'x-rapidapi-key': "140057265dmsh713908e8cfa938ap13c876jsnf0d7be73661d",
    'x-rapidapi-host': "free-nba.p.rapidapi.com"
    }

response = requests.request("GET", url, headers=headers, params=querystring)

next_step = response.json()
input_json = json.dumps(next_step)
# print(next_step)

producer.send("kafkaNba", input_json.encode('utf-8'))
producer.flush()




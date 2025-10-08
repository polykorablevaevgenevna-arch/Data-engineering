import requests
import pandas as pd

url = 'https://official-joke-api.appspot.com/jokes/programming/ten'

#Запрос к API
response = requests.get(url)
data = response.json()  # data — список словарей

#Создаём DataFrame
df = pd.DataFrame(data)

# Выводим DataFrame
print(df)
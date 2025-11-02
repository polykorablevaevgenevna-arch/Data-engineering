import requests

url = 'https://official-joke-api.appspot.com/jokes/programming/ten'
response = requests.get(url)

if response.status_code == 200:
    data = response.json()
    print(data[0]['setup'])
    print(data[0]['punchline'])
else:
    print("Ошибка:", response.status_code)

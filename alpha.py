import requests

url = 'https://www.alphavantage.co/query?function=DIGITAL_CURRENCY_DAILY&symbol=BTC&market=CNY&apikey=AAGSKPMQ179UJDEY'
r = requests.get(url)
data = r.json()

print(data)

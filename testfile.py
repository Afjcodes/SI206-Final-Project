import sqlite3
import json
import os
import requests
import re

#Test test test

api_key = 'KRTAD97W3Z6VP2H5WD852KJN5'
url = f'https://datausa.io/api/data?drilldowns=State&measures=Population&year=2021'

response = requests.get(url)
data = response.json()
print(data['data'][22])
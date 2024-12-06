import sqlite3
import json
import os
import requests
import re

#URL for requests
url = f'https://datausa.io/api/data?drilldowns=State&measures=Population&year='

def get_data_by_year(year):
    response = requests.get(url+year)
    data = response.json()
    return data['data']

def set_up_database(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path + "/" + db_name)
    cur = conn.cursor()
    return cur, conn

def create_population_table(data,cur,conn):
    cur.execute(
        "CREATE TABLE IF NOT EXISTS Populations (state TEXT, population INT, year INT)"
    )
    count = 0
    state_relevant_data = []
    for state in data:
        statename = state['State']
        year = state['ID Year']
        pop = state['Population']
        state_relevant_data.append((statename,pop,year))
    
    for state in state_relevant_data:
        if count >= 25:
            break
        
        cur.execute("INSERT OR IGNORE INTO Populations (state,population,year) VALUES (?,?,?)", (state[0],state[1],state[2]))
        
        
    conn.commit()
        


    

     

def read_data_from_file(filename):
    full_path = os.path.join(os.path.dirname(__file__), filename)
    f = open(full_path)
    file_data = f.read()
    f.close()
    json_data = json.loads(file_data)
    return json_data



def main():
    data22 = get_data_by_year('2022')
    data21 = get_data_by_year('2021')
    combined_data = data22+data21
    with open('multiyearstatedata', 'w') as f:
        json.dump(combined_data, f, indent =4)  
    state_data = read_data_from_file('multiyearstatedata')
    cur,conn = set_up_database('statepop.db')
    create_population_table(state_data,cur, conn)
    conn.close()



if __name__ == "__main__":
    main()


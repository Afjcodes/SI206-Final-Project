import sqlite3
import json
import os
import requests
import re

#URL for requests
url = f'https://datausa.io/api/data?drilldowns=State&measures=Population&year='

def get_data_by_year(year):
    #year is an integer
    #This function intends to use the requests library to use the url from the API and get data for a specified year, returning the response as a json.
    response = requests.get(url+year)
    data = response.json()
    return data['data']

def set_up_database(db_name):
    #db_name is the name of the database to be created.
    #This function intends to create a database with the specified name as input, returning the cursor and connection objects.
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path + "/" + db_name)
    cur = conn.cursor()
    return cur, conn

def create_population_table(data,cur,conn):
    #data is a JSON object consisting of the data collected from the population API
    #cur is the database cursor
    #conn is the database connection
    cur.execute(
        "CREATE TABLE IF NOT EXISTS Populations (state TEXT PRIMARY KEY, population INT, year INT)"
    )
    count = 0
    state_relevant_data = []
    for state in data:
        statename = state['State']+ '_' +str(state['ID Year'])
        year = state['ID Year']
        pop = state['Population']
        state_relevant_data.append((statename,pop,year))
    
    for state in state_relevant_data:
        if count >= 25:
            break
        
        cur.execute("INSERT OR IGNORE INTO Populations (state,population,year) VALUES (?,?,?)", (state[0],state[1],state[2]))
        row_id = cur.lastrowid
        #print(row_id)
        if row_id != 0:
            count +=1

    conn.commit()
        


    

     

def read_data_from_file(filename):
    #filename is a String with the filename to be read from
    #This function returns a JSON object of data from the file used as input.
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


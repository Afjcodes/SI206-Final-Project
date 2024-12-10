import sqlite3
import json
import os
import requests
import re
import matplotlib.pyplot as plt


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
        
def lowest_pops_graph(cur,conn,year):
    colors = ["#e8d8e0","#aebbdb","#dbc4bc","#ecd8d7","#f4a29d",
              "#cad69e","#cbaedb","#d6bf9f","#b4cbe9","#ece6a2"]
    fig = plt.figure(1,figsize = (15,5))
    ax1 = fig.add_subplot(122)
    cur.execute(f"SELECT Populations.population, Populations.state FROM Populations WHERE year = {year}")
    popcounts = cur.fetchall()
    bottom10 = sorted(popcounts, key= lambda x: int(x[0]), reverse = False)[:10]
    popnums = []
    statenames = []
    for i in bottom10:
        popnums.append(i[0])
        statenames.append(i[1])

    ax1.barh(statenames, popnums,color = colors)
    ax1.ticklabel_format(axis='x',style='plain')
    ax1.set_xlabel("Population")
    ax1.set_ylabel('States')
    ax1.set_title(f'10 States with the Lowest Population in {year}')
    plt.ticklabel_format(axis='x',style = 'plain')
    plt.show()

def create_pie_chart(cur,conn,year):
    colors = ["#e8d8e0","#aebbdb","#dbc4bc","#ecd8d7","#f4a29d",
              "#cad69e","#cbaedb","#d6bf9f","#b4cbe9","#ece6a2"]
    cur.execute(f"SELECT crash_data.crash_counts, crash_data.unique_id FROM crash_data WHERE case_year = {year}")
    crashcounts = cur.fetchall()
    top10 = sorted(crashcounts, key= lambda x: int(x[0]), reverse = True)[:10]
    crashnums = []
    statenames = []
    for i in top10:
        crashnums.append(i[0])
        statenames.append(i[1])
    fig = plt.figure(1,figsize=(10,10))
    ax1 = fig.add_subplot(121)
    ax1.pie(crashnums, labels = statenames, autopct = '%1.1f%%', colors = colors)
    ax1.set_title('Top 10 States with Car Crashes')
    plt.show()

     

def read_data_from_file(filename):
    full_path = os.path.join(os.path.dirname(__file__), filename)
    f = open(full_path)
    file_data = f.read()
    f.close()
    json_data = json.loads(file_data)
    return json_data


def crashes_by_pop(cur, conn):
    cur.execute(f"SELECT crash_data.crash_counts, Populations.population FROM crash_data JOIN Populations ON crash_data.unique_id =  Populations.state WHERE crash_data.state_id = 26")
    results = cur.fetchall()
    percents = []
    for year in results:
        perc = year[0] / year[1]
        percents.append(perc)
    results = f"In comparison of the crashes divided by population from 2021 to 2022 in the state of Michigan, the calculation for 2021 was {percents[0]}, while the calculation for 2022 was {percents[1]}. We saw a slight decrease from 2021 to 2022 in Michigan."
    with open('MichiganAveragesCalculations', 'w') as f:
        f.write(results)


def main():
    data22 = get_data_by_year('2022')
    data21 = get_data_by_year('2021')
    combined_data = data22+data21
    with open('multiyearstatedata', 'w') as f:
        json.dump(combined_data, f, indent =4)  
    state_data = read_data_from_file('multiyearstatedata')
    cur,conn = set_up_database('final.db')
    create_population_table(state_data,cur, conn)
    with open("combined_crash_data.json", "r") as file:
        data = json.load(file)

    # Connect to the database
    

    # Create the states table
    cur.execute("""
    CREATE TABLE IF NOT EXISTS states (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL
    )
    """)

    # Add state information
    states = [
        (1, "Alabama"), (2, "Alaska"), (4, "Arizona"), (5, "Arkansas"),
        (6, "California"), (8, "Colorado"), (9, "Connecticut"),
        (10, "Delaware"), (12, "Florida"), (13, "Georgia"), (15, "Hawaii"),
        (16, "Idaho"), (17, "Illinois"), (18, "Indiana"), (19, "Iowa"),
        (20, "Kansas"), (21, "Kentucky"), (22, "Louisiana"), (23, "Maine"),
        (24, "Maryland"), (25, "Massachusetts"), (26, "Michigan"),
        (27, "Minnesota"), (28, "Mississippi"), (29, "Missouri"),
        (30, "Montana"), (31, "Nebraska"), (32, "Nevada"),
        (33, "New Hampshire"), (34, "New Jersey"), (35, "New Mexico"),
        (36, "New York"), (37, "North Carolina"), (38, "North Dakota"),
        (39, "Ohio"), (40, "Oklahoma"), (41, "Oregon"),
        (42, "Pennsylvania"), (44, "Rhode Island"), (45, "South Carolina"),
        (46, "South Dakota"), (47, "Tennessee"), (48, "Texas"),
        (49, "Utah"), (50, "Vermont"), (51, "Virginia"),
        (53, "Washington"), (54, "West Virginia"), (55, "Wisconsin"),
        (56, "Wyoming")
    ]
    cur.executemany("INSERT OR IGNORE INTO states (id, name) VALUES (?, ?)", states)

    # Create the crash_data table
    cur.execute("""
    CREATE TABLE IF NOT EXISTS crash_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        unique_id TEXT NOT NULL UNIQUE,
        case_year TEXT NOT NULL,
        crash_counts INTEGER NOT NULL,
        fatal_counts INTEGER NOT NULL,
        state_id INTEGER NOT NULL,
        FOREIGN KEY (state_id) REFERENCES states (id)
    )
    """)

    # Add crash data with a hard limit of 25 unique entries per run
    added_rows = 0
    limit = 25

    for entry in data["Results"][0]:
        # Get state name from the states table
        state_name = [state[1] for state in states if state[0] == entry["State"]][0]
        unique_id = f"{state_name}_{entry['CaseYear']}"  # Create unique ID

        # Check if the unique_id already exists in the database
        cur.execute("SELECT COUNT(*) FROM crash_data WHERE unique_id = ?", (unique_id,))
        exists = cur.fetchone()[0]

        if exists == 0:  # If unique_id doesn't exist, insert the row
            cur.execute("""
            INSERT INTO crash_data (unique_id, case_year, crash_counts, fatal_counts, state_id)
            VALUES (?, ?, ?, ?, ?)
            """, (unique_id, entry["CaseYear"], entry["CrashCounts"], entry["TotalFatalCounts"], entry["State"]))
            added_rows += 1

            if added_rows == limit:  # Stop after adding 25 rows
                break
    

    #Calculations
    crashes_by_pop(cur,conn)

    #Visualizations
    create_pie_chart(cur,conn,2021)
    create_pie_chart(cur,conn,2022)
    lowest_pops_graph(cur,conn,2021)





    # Commit changes and close the connection
    conn.commit()
    conn.close()

    print(f"Added {added_rows} unique rows to the database.")



if __name__ == "__main__":
    main()


import sqlite3
import json

# Load the JSON data
with open("combined_crash_data.json", "r") as file:
    data = json.load(file)

# Connect to the database
conn = sqlite3.connect("crash_data.db")
cursor = conn.cursor()

# Create the states table
cursor.execute("""
CREATE TABLE IF NOT EXISTS states (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
)
""")

# Add state information
# This was necessary to be hardcoded since the website uses these id's to refer to the states and there's some weirdness [Numbers being skipped and other territories]
# Having these allowed us to call only the 50 states we wanted and ignore other locations that didn't meet our needs
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
cursor.executemany("INSERT OR IGNORE INTO states (id, name) VALUES (?, ?)", states)
#google and gpt showed executemany allows to put all of the states into the table at once
# Create the crash_data table
cursor.execute("""
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
    unique_id = f"{state_name}_{entry['CaseYear']}"  
    # We had to create a uniqueid for each of the combinations in order to lookup state/year combinations

    # Check if the unique_id already exists in the database
    cursor.execute("SELECT COUNT(*) FROM crash_data WHERE unique_id = ?", (unique_id,))
    exists = cursor.fetchone()[0]

    if exists == 0:  # If unique_id doesn't exist, insert the row
        cursor.execute("""
        INSERT INTO crash_data (unique_id, case_year, crash_counts, fatal_counts, state_id)
        VALUES (?, ?, ?, ?, ?)
        """, (unique_id, entry["CaseYear"], entry["CrashCounts"], entry["TotalFatalCounts"], entry["State"]))
        added_rows += 1

        if added_rows == limit:  # Stop after adding 25 rows
            break

# Commit changes and close the connection
conn.commit()
conn.close()

print(f"Added {added_rows} unique rows to the database.")
import sqlite3
import json
import os
import requests

#start by adding the api key and url
# then use the keyworkds needed to get the specific data i want
# limit the json data to a 100
# create a database and start adding them to the database based on the info limit of 25 each time
VALID_STATE_IDS = [
    1, 2, 4, 5, 6, 8, 9, 10, 12, 13, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 
    27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 44, 45, 46, 47, 
    48, 49, 50, 51, 53, 54, 55, 56
]


def fetch_state_crash_data(state_id, url_template):
    """
    Fetch crash data for a specific state using the provided API URL template.
    """
    url = url_template.replace("state=1", f"state={state_id}")
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching data for State {state_id}: {response.status_code}")
        return None

def merge_crash_data(existing_data, new_data, state_id):
    """
    Merge new crash data into existing data, avoiding overlaps based on state and year.
    """
    if not new_data or "Results" not in new_data or not new_data["Results"][0]:
        print(f"No new data to merge for State {state_id}.")
        return existing_data

    new_results = new_data["Results"][0]  # New state's data
    existing_results = existing_data["Results"][0]  # Combined data

    # Track unique combinations of state and year
    existing_combinations = {(entry["State"], entry["CaseYear"]) for entry in existing_results}

    for new_entry in new_results:
        new_entry["State"] = state_id  # Add state info to each entry
        if (state_id, new_entry["CaseYear"]) not in existing_combinations:
            existing_results.append(new_entry)
        else:
            print(f"Skipping duplicate entry for State {state_id}, Year {new_entry['CaseYear']}")

    # Update the count to reflect the new total entries
    existing_data["Count"] = len(existing_results)
    return existing_data

def main():
    url_template = "https://crashviewer.nhtsa.dot.gov/CrashAPI/analytics/GetInjurySeverityCounts?fromCaseYear=2014&toCaseYear=2015&state=1&format=json"
    
    # Initialize an empty JSON structure
    combined_data = {
        "Count": 0,
        "Message": "Combined Results",
        "Results": [[]],  # Placeholder for the combined results
        "SearchCriteria": "FromYear: 2014 | ToYear: 2015"
    }

    for state_id in VALID_STATE_IDS:
        print(f"Fetching data for State {state_id}...")
        state_data = fetch_state_crash_data(state_id, url_template)
        combined_data = merge_crash_data(combined_data, state_data, state_id)


    # Save the combined data to a JSON file
    with open("combined_crash_data.json", "w") as file:
        json.dump(combined_data, file, indent=2)

    print("Combined data saved to 'combined_crash_data.json'.")

if __name__ == "__main__":
    main()
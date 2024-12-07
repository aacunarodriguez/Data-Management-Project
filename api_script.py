import requests
import mysql.connector
import traceback
import sys


API_KEY = "gJJIq2s8RWtAffWX2BlWE4mZpdxuxBvTf8RSyreQ"
headers = {"Accept": "application/json"}

def get_state_data(state):
    URL = f"https://api.usa.gov/crime/fbi/cde/arrest/state/{state}/property_crime?from=2022&to=2022&API_KEY={API_KEY}"
    response = requests.get(URL, headers=headers)
    return response.json()


def get_api_data():
    print("Running api_script.py")
    print("Extracting API data from https://cde.ucr.cjis.gov/LATEST/webapp/# and sending to DB")
    
    try:
        db = mysql.connector.connect(
            host="127.0.0.1",
            port="3306",
            user="root",
            password="oiecy321",
            database="web_scrapper_api_property_data",
        )
        cursor = db.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS api_table (
                state VARCHAR(255),
                count_burglary INT,
                count_stolen_property INT,
                count_vandalism INT
            )
        """)

        cursor.execute("SELECT DISTINCT state FROM web_scrapper_table")
        states = cursor.fetchall()

        for (state,) in states:
            cursor.execute("SELECT EXISTS(SELECT * FROM api_table WHERE state = %s)", (state,))
            if cursor.fetchone()[0] == 0:
                print(f"Getting crime data for {state} and sending to DB")
                state_api_data = get_state_data(state)
                if 'data' in state_api_data:
                    data = state_api_data['data'][0]
                    sql = "INSERT INTO api_table (state, count_burglary, count_stolen_property, count_vandalism) VALUES (%s, %s, %s, %s)"
                    count_burglary = data.get('Burglary', 0)
                    stolen_property = data.get('Stolen Property: Buying, Receiving, Possessing', 0)
                    vandalism = data.get('Vandalism', 0)

                    val = (state, count_burglary, stolen_property, vandalism)
                    cursor.execute(sql, val)
                    db.commit()
                else:
                    print(f"The API didn't return data as expected. The state {state} will be ignored")
            else:
                print(f"State {state} has already crime data in the DB")

    except Exception as exp:
        print("Error in API Script. Exiting script")
        print(traceback.format_exc())
        print("\n")
        sys.exit()
        
    finally:
        cursor.close()
        db.close()
    
    
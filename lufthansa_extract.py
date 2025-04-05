import requests
import json
import os
import csv
from mysql.connector import Error
import sys
from datetime import datetime, timezone
import logging
from database import get_db_persistent
import time

api_calls = 0
start_time = time.time()

def enforce_rate_limit():
    """Ensures API calls remain under 5 per second and 1000 per hour."""
    global api_calls, start_time

    # Check per-second limit (5 requests per second)
    time.sleep(0.2)  # Ensures at most 5 requests per second

    # Check per-hour limit (1000 requests per hour)
    if api_calls >= 995:
        elapsed_time = time.time() - start_time
        if elapsed_time < 3600:  # If 1000 requests used before 1 hour
            sleep_time = 3600 - elapsed_time
            logging.warning(f"API limit reached. Sleeping for {sleep_time:.2f} seconds...")
            time.sleep(sleep_time)  # Sleep until the hour resets

        # Reset counter and timestamp
        api_calls = 0
        start_time = time.time()

    api_calls += 1  # Increment API call counter


def convert_to_mysql_datetime(iso_datetime):
    """Converts ISO 8601 datetime string to MySQL DATETIME format with timezone conversion."""
    try:
        # Remove 'T' and 'Z' and convert to datetime object
        if iso_datetime.strip() == "":  # Handle empty datetime strings
            return None
        
        iso_datetime = iso_datetime.replace("T", " ").replace("Z", "")
        
        # Parse the datetime string into a datetime object
        dt_obj = datetime.strptime(iso_datetime, "%Y-%m-%d %H:%M")
        
        

        # Return the datetime object in MySQL format
        return dt_obj.strftime("%Y-%m-%d %H:%M")
    
    except ValueError as e:
        logging.error(f"Error parsing datetime: {e} for value {iso_datetime}")
        return None  # Return None if conversion fails

current_utc_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M")
base="https://api.lufthansa.com/v1/" \
""
def get_credentials(OUTPUT_PATH: str) -> dict[str, str]:
    """Get the Lufthansa API credentials from the local file."""
    file_path = os.path.join(OUTPUT_PATH, 'lufthansa_credentials.json')
    with open(file_path, 'r') as f:
        data = json.load(f)
        credentials = data.get('credentials')[0]
        logging.debug(OUTPUT_PATH +'lufthansa_credentials.json')
    return credentials


def get_token(client_id: str, client_secret: str) -> tuple[str,str]:
    """Get the token from the Lufthansa API."""
    enforce_rate_limit()
    # Ensure we don't exceed the rate limit
    endpoint = f"oauth/token"
    url=base+endpoint
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json"
    }
    data = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "client_credentials"
    }
    try:
        response = requests.post(url, headers=headers, data=data)
        response.raise_for_status()
        token = response.json()['access_token']
        token_type = response.json()['token_type']
        return token, token_type
    except requests.exceptions.HTTPError as e:
        logging.exception(f"HTTP error occurred: {e}")
    except requests.exceptions.RequestException as e:
        logging.exception(f"Request error occurred: {e}")
        



def get_airports_data(cursor, connection, csv_file: str, iata):
    """Insert the legit IATA codes into the database."""
    airport_name, city, country, latitude, longitude = None, None, None, None, None
    
    with open(csv_file, mode='r', newline="", encoding='utf-8') as file:
        csv_reader = csv.DictReader(file)

        for row in csv_reader:
            if row.get("IATA", "").strip() == iata:
        
                
                airport_name = row.get("Name", "").strip()
                city = row.get("City", "").strip()
                country = row.get("Country", "").strip()
                latitude = row.get("Latitude", "").strip()
                longitude = row.get("Longitude", "").strip()
            
    return airport_name, city, country, latitude, longitude


def insert_airports_data(cursor, connection, iata, airport_name, city, country, latitude, longitude):
    
    def insert_cities(city):
        """Insert the cities into the database."""
        try:
            
            cursor.execute(
                "INSERT IGNORE INTO Cities (name) VALUES (%s)",
                (city,)
            )
            logging.info(f"Inserted city data for {city}")
            if cursor.lastrowid:
                logging.info(f"Inserted into `Cities`: {cursor.lastrowid}")
                city_id = cursor.lastrowid
                return city_id
            else:
                
                cursor.execute("SELECT city_id FROM Cities WHERE name = %s", (city,))
                logging.debug(f"Executed query: {cursor._executed}")
                result = cursor.fetchone()
                city_id = result[0] if result else None
                return city_id
            
        except Error as e:
            logging.exception(f"Error inserting city data for airport {city}: {e}")
            connection.rollback()

    def insert_countries(country):
        """Insert the countries into the database."""
        try:
            
            cursor.execute(
                "INSERT IGNORE INTO Countries (name) VALUES (%s)",
                (country,)
            )
            logging.info(f"Inserted country data for {country}")
            if cursor.lastrowid:
                logging.info(f"Inserted into `Countries`: {cursor.lastrowid}")
                country_id = cursor.lastrowid
                return country_id
            else:
                
                cursor.execute("SELECT country_id FROM Countries WHERE name = %s", (country,))
                logging.debug(f"Executed query: {cursor._executed}")
                result = cursor.fetchone()
                country_id = result[0] if result else None
                logging.info(f"Duplicate found in `countries`: {result}")
                return country_id
        
                
        
        except Error as e:
            logging.exception(f"Error inserting country data for airport {country}: {e}")
            connection.rollback()

    def insert_airports(city_id, country_id, iata, airport_name, latitude, longitude):
        """Insert the airports into the database."""
        try:
            
                
            cursor.execute(
                "INSERT IGNORE INTO Airports (Airport_code, Airport_name, City_id, Country_id, latitude, longitude ) VALUES (%s,%s,%s,%s,%s,%s)",
                (iata, airport_name, city_id, country_id, latitude, longitude))
                
            
            logging.info(f"Inserted airport data for {iata}")
            
                
            
        except Error as e:
            logging.exception(f"Error inserting country data for airport {iata}: {e}")
            connection.rollback()




    country_id = insert_countries(country)
    city_id = insert_cities(city)
    insert_airports(city_id, country_id, iata, airport_name,  latitude, longitude)
    logging.info(f"Inserted airports data for {iata}")
    

def get_flight_data(token_type, token, iata: str):
    """Insert the flight data into the database."""
    all_flight_data_departure = []
    all_flight_data_arrival = []
    offset1 = 0
    offset2 = 0
    # Define the range for pagination
    range=100

    while True:
        enforce_rate_limit()
        # Ensure we don't exceed the rate limit
        endpoint_departure = f"operations/flightstatus/departures/{iata}/{current_utc_time}?serviceType=all&limit={range}&offset={offset1}"
        url=base+endpoint_departure
        headers = {
            "Accept": "application/json",
            "Authorization": f"{token_type} {token}"
        }
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            flight_data_departure = data.get('FlightStatusResource', {}).get('Flights', {}).get('Flight', [])
            all_flight_data_departure.extend(flight_data_departure)


            offset1+=range
            if len(flight_data_departure) < range:
                break
            
        else:
            logging.warning(f"Warning {response.status_code}: {response.text}")
            flight_data_departure = []
            break
           

    while True:
        enforce_rate_limit()
        # Ensure we don't exceed the rate limit
        endpoint_arrival = f"operations/flightstatus/arrivals/{iata}/{current_utc_time}?serviceType=all&limit={range}&offset={offset2}"
        url=base+endpoint_arrival
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            flight_data_arrival = data.get('FlightStatusResource', {}).get('Flights', {}).get('Flight', [])
            all_flight_data_arrival.extend(flight_data_arrival)
            offset2+=range
            if len(flight_data_arrival) < range:
                break
        else:
            logging.warning(f"Warning {response.status_code}: {response.text}")
            flight_data_arrival = []
            break

    flight_data = all_flight_data_departure + all_flight_data_arrival 
    if flight_data:
        logging.info(f"Flight data fetched successfully for {flight_data}")
        return flight_data
    else:
        logging.warning(f"No flight data found for {iata}")
        return None
    
    



def insert_flight_data(cursor, connection, flight_data: dict, token: str, token_type: str):
    """Insert the flight data into the database."""
    new_airports = set()
    

    def calculate_flight_completion(actual_departure, scheduled_arrival):
    
        """Calculate the flight completion percentage based on scheduled and actual times."""
        
        try:
            
            # Ensure all required times are present
            if not scheduled_arrival or not actual_departure:
                logging.warning("Missing scheduled or actual times")
                return 0  # Can't calculate completion without schedule
            
            actual_departure_time = datetime.strptime(actual_departure, "%Y-%m-%d %H:%M")
            # Convert to UTC timezone
            actual_departure_time = actual_departure_time.replace(tzinfo=timezone.utc)            
            scheduled_arrival_time = datetime.strptime(scheduled_arrival, "%Y-%m-%d %H:%M")
            # Convert to UTC timezone
            scheduled_arrival_time = scheduled_arrival_time.replace(tzinfo=timezone.utc)
            
            # Calculate total scheduled flight duration
            total_time = (scheduled_arrival_time - actual_departure_time).total_seconds()
            logging.debug(f"Total time: {total_time} seconds")
            if total_time <= 0:
                return 0  # Invalid flight schedule

            # If actual times exist, calculate progress
            if actual_departure and scheduled_arrival:
                current = datetime.now(timezone.utc)
                elapsed_time = (current - actual_departure_time).total_seconds()
                logging.debug(f"Elapsed time: {elapsed_time} seconds")
                if elapsed_time < 0:
                    return 0
            else:
                return 0  # Flight hasn't taken off yet
            
            # Ensure elapsed time isn't greater than total time
            completion_percentage = min(100, max(0, (elapsed_time / total_time) * 100))
            logging.debug(f"Completion percentage: {completion_percentage}%")
            completion_percentage = round(completion_percentage, 2)
            logging.info(f"Flight completion percentage: {completion_percentage}%")
            return completion_percentage
        
        except Exception as e:
            print(f"Error in calculate_flight_completion: {e}")
            return 0
        
    def get_airport_coordinates(departure, arrival):
        """Get the current flight position."""
        try:
            
            
            cursor.execute("select latitude, longitude from Airports where airport_code = %s", (departure,))
            result = cursor.fetchone()
            if result:
                latitude_departure, longitude_departure = result
            else:
                logging.warning(f"Airport {departure} not found in database")
                return None, None, 
            cursor.execute("select latitude, longitude from Airports where airport_code = %s", (arrival,))
            result = cursor.fetchone()
            if result:
                latitude_arrival, longitude_arrival = result
            else:
                logging.warning(f"Airport {arrival} not found in database")
                return None, None,
            
            return latitude_departure, longitude_departure, latitude_arrival, longitude_arrival, 
        except Exception as e:
            print(f"Error in get_flight_position: {e}")
            return None, None, None, None
        
    def calculate_flight_position(dep_lat, dep_lon, arr_lat, arr_lon, completion_percent):
        """Calculate the estimated current latitude and longitude of a flight."""
        completion_ratio = completion_percent / 100  # Convert to decimal

        if dep_lat is None or dep_lon is None or arr_lat is None or arr_lon is None:
            logging.warning("Missing latitude or longitude data")   
            return 0, 0  # Can't calculate position without coordinates
        
        dep_lat = float(dep_lat)
        dep_lon = float(dep_lon)
        arr_lat = float(arr_lat)
        arr_lon = float(arr_lon)

        current_lat = dep_lat + (completion_ratio * (arr_lat - dep_lat))
        current_lon = dep_lon + (completion_ratio * (arr_lon - dep_lon))

        return round(current_lat, 6), round(current_lon, 6)
    
    def get_aircraft_datas(token, token_type, aircraft_id: str):
        """Get the aircraft data."""
        enforce_rate_limit()
        endpoint = f"mds-references/aircraft/{aircraft_id}"
        url=base+endpoint
        headers = {
            "Accept": "application/json",
            "Authorization": f"{token_type} {token}"
        }
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            aircraft_name = data.get("AircraftResource", {}).get("AircraftSummaries", {}).get("AircraftSummary", {}).get("Names", {}).get("Name", {}).get("$", "").strip()

        
            return aircraft_name
        else:
            logging.warning(f"Warning {response.status_code}: {response.text}")
            return None
    def get_airline_datas(airline_id: str, token, token_type):
        """Get the airline data."""
        enforce_rate_limit()
        endpoint = f"mds-references/airlines/{airline_id}"
        url=base+endpoint
        headers = {
            "Accept": "application/json",
            "Authorization": f"{token_type} {token}"
        }
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            airline_name = data.get("AirlineResource", {}).get("Airlines", {}).get("Airline", {}).get("Names", {}).get("Name", {}).get("$", "").strip()
            return airline_name
        else:
            logging.warning(f"Warning {response.status_code}: {response.text}")
            return None
    
    def insert_airline(airline_id: str, airline_name: str):
        """Insert the airline data into the database."""
        try:
            cursor.execute(
                "INSERT IGNORE INTO Airlines (Airline_code, name) VALUES (%s,%s)",
                (airline_id, airline_name)
            )
            logging.info(f"Inserted airline data for {airline_id}")
            connection.commit()
            
        
        except Error as e:
            logging.exception(f"Error inserting airline data for flight {flight_number}: {e}")
            connection.rollback()
    def insert_aircraft(aircraft_id: str, aircraft_name: str):
        """Insert the aircraft data into the database."""
        try:
            cursor.execute(
                "INSERT IGNORE INTO Aircrafts (Aircraft_code, Name) VALUES (%s,%s)",
                (aircraft_id, aircraft_name)
            )
            logging.info(f"Inserted aircraft data for {aircraft_id}")
            connection.commit()
            
        
        except Error as e:
            logging.exception(f"Error inserting aircraft data for flight {flight_number}: {e}")
            connection.rollback()
    
    
    for flight in flight_data:
        
        

        scheduled_departure = flight.get("Departure", {}).get("ScheduledTimeUTC", {}).get("DateTime", "").strip()
        # Convert the scheduled departure time to MySQL DATETIME format
        scheduled_departure = convert_to_mysql_datetime(scheduled_departure)
        if scheduled_departure is None:
            logging.warning(f"Invalid scheduled departure time for flight {flight}")
            scheduled_departure = None
        actual_departure = flight.get("Departure", {}).get("ActualTimeUTC", {}).get("DateTime", "").strip()
        # Convert the actual departure time to MySQL DATETIME format
        actual_departure = convert_to_mysql_datetime(actual_departure)
        if actual_departure is None:
            logging.warning(f"Invalid actual departure time for flight {flight}")
            actual_arrival = None
        scheduled_arrival = flight.get("Arrival", {}).get("ScheduledTimeUTC", {}).get("DateTime", "").strip()
        # Convert the scheduled arrival time to MySQL DATETIME format
        scheduled_arrival = convert_to_mysql_datetime(scheduled_arrival)
        if scheduled_arrival is None:
            logging.warning(f"Invalid scheduled arrival time for flight {flight}")
            scheduled_arrival = None
        actual_arrival = flight.get("Arrival", {}).get("ActualTimeUTC", {}).get("DateTime", "").strip()
        # Convert the actual arrival time to MySQL DATETIME format
        actual_arrival = convert_to_mysql_datetime(actual_arrival)
        if actual_arrival is None:
            logging.warning(f"Invalid actual arrival time for flight {flight}")
            scheduled_arrival = None
        airport_departure = flight.get("Departure", {}).get("AirportCode", "").strip()
        query = "SELECT airport_code FROM Airports WHERE airport_code = %s"
        cursor.execute(query, (airport_departure,))
        result = cursor.fetchone()
        if not result:
            airport_name, city, Country, Latitude, Longitude = get_airports_data(cursor, connection, CSV_FILE_PATH_2, airport_departure)
            insert_airports_data(cursor, connection, airport_departure, airport_name, city, Country, Latitude, Longitude)
            new_airports.add(airport_departure)
            
            
        
        airport_arrival = flight.get("Arrival", {}).get("AirportCode", "").strip()
        cursor.execute(query, (airport_arrival,))
        result = cursor.fetchone()
        if not result:
            airport_name, city, Country, Latitude, Longitude = get_airports_data(cursor, connection, CSV_FILE_PATH_2, airport_arrival)
            insert_airports_data(cursor, connection, airport_arrival, airport_name, city, Country, Latitude, Longitude)
            new_airports.add(airport_arrival)
        
        completion_percentage = calculate_flight_completion( actual_departure, scheduled_arrival)
        latitude_departure, longitude_departure, latitude_arrival, longitude_arrival = get_airport_coordinates(airport_departure, airport_arrival)
        current_lat, current_lon = calculate_flight_position(latitude_departure, longitude_departure, latitude_arrival, longitude_arrival, completion_percentage)

        aircraft_id = flight.get("Equipment", {}).get("AircraftCode", "").strip()
        airline_id = flight.get("OperatingCarrier", {}).get("AirlineID", "").strip()
        flight_number = flight.get("OperatingCarrier", {}).get("FlightNumber", "").strip()


        aircraft_name = get_aircraft_datas(token, token_type, aircraft_id)
        airline_name = get_airline_datas(airline_id, token, token_type)
        insert_aircraft(aircraft_id, aircraft_name)
        insert_airline(airline_id, airline_name)
    
        try:
            cursor.execute(
                """INSERT INTO Flights (
                Airport_departure_id, Airport_arrival_id, Scheduled_departure_time, 
                Actual_departure_time, Departure_time_status_code, Scheduled_arrival_time, Actual_arrival_time, Arrival_time_status_code, Terminal, 
                Gate, Flight_completion, Current_position_latitude, Current_position_longitude, Airline_code, Aircraft_code, Flight_status_code, Flight_number, Service_type) 
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE
                    Airport_departure_id = VALUES(Airport_departure_id),
                    Airport_arrival_id = VALUES(Airport_arrival_id),
                    Scheduled_departure_time = VALUES(Scheduled_departure_time),
                    Actual_departure_time = VALUES(Actual_departure_time),
                    Departure_time_status_code = VALUES(Departure_time_status_code),
                    Scheduled_arrival_time = VALUES(Scheduled_arrival_time),
                    Actual_arrival_time = VALUES(Actual_arrival_time),
                    Arrival_time_status_code = VALUES(Arrival_time_status_code),
                    Terminal = VALUES(Terminal),
                    Gate = VALUES(Gate),
                    Flight_completion = VALUES(Flight_completion),
                    Current_position_latitude = VALUES(Current_position_latitude),
                    Current_position_longitude = VALUES(Current_position_longitude),
                    Airline_code = VALUES(Airline_code),
                    Aircraft_code = VALUES(Aircraft_code),
                    Flight_status_code = VALUES(Flight_status_code),
                    Service_type = VALUES(Service_type)""",
                    (airport_departure,
                    airport_arrival,
                    scheduled_departure,
                    actual_departure,
                    flight.get("Departure", {}).get("TimeStatus", {}).get("Code", "").strip(),
                    scheduled_arrival,
                    actual_arrival,
                    flight.get("Arrival", {}).get("TimeStatus", {}).get("Code", "").strip(),
                    flight.get("Departure", {}).get("Terminal", {}).get("Name", "").strip(),
                    flight.get("Departure", {}).get("Terminal", {}).get("Gate", "").strip(),
                    f"{completion_percentage}  %",
                    current_lat,
                    current_lon,
                    airline_id,
                    aircraft_id,
                    flight.get("FlightStatus", {}).get("Code", "").strip(),
                    flight_number,
                    flight.get("ServiceType").strip()
                    )    
            )
            logging.info(f"Inserted flight data for {flight_number}")
        except Error as e:
            logging.exception(f"Error inserting flight data for flight {flight_number}: {e}")
            connection.rollback()  
        for new_airport in new_airports:
            new_flight_data=get_flight_data(token_type, token, new_airport)
            if new_flight_data:
                insert_flight_data(cursor, connection, new_flight_data, token, token_type)
            else:
                logging.warning(f"No flight data found for {new_airport}")
            logging.info(f"Inserted new airport data for {new_airport}")
        

   


        
        
   

        



        
        

        



def insert_status(cursor, connection):
    """Insert the flight status into the database."""
    try:
        cursor.execute(
            """INSERT IGNORE INTO Status (status_code, name) VALUES ('FE','Flight Early'), ('NI','Next Information'), ('OT','Flight On Time'), 
            ('DL','Flight Delayed'), ('NO','No status'), ('CD','Flight Cancelled'), ('DP','Flight Departed'), 
            ('LD','Flight Landed'), ('RT','Flight Rerouted'), ('NA','No status')""",
  
        )


        logging.info("Inserted flight status data")
        connection.commit()
    except Error as e:
        logging.exception(f"Error inserting flight status data: {e}")
        connection.rollback()


def close_connection(cursor, connection):
    """Close the database connection."""
    try:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
        logging.info("Database connection closed")
    except Error as e:
        logging.exception(f"Error closing database connection: {e}")
              
          
        





def establish_connection():
    
    logging.basicConfig(
    level=logging.DEBUG,  
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
            logging.FileHandler(log_file_path),  # Log to a file
            logging.StreamHandler()         # Log to the console
    ]
    )
    

    try:
        
        # Use the get_db function as a context manager to get the cursor and connection
        cursor, connection = get_db_persistent()
        if connection.is_connected():
            logging.info("Connected to MySQL database")
            return cursor, connection
    except Error as e:
        logging.exception(f"Error while connecting to MySQL: {e}")
        return None, None
    

def load_data_to_db(cursor, connection, OUTPUT_DIR, CSV_FILE_PATH):
    """
    Fetches job offers for each department code and inserts them into the database.
    """
    try:
        # Step 1: Fetch credentials and access token
        credentials = get_credentials(OUTPUT_DIR)
        client_id = credentials["clientID"]
        client_secret = credentials["key"]

        token, token_type = get_token(client_id, client_secret)
        if token is None:
            logging.error("Failed to get access token")
            return None
        

        
        # Step 2: Insert the flight status into the database
        insert_status(cursor, connection)
        logging.info("Flight status data inserted successfully")
        connection.commit()

        # Step 3: Load the flight data into the database
        query = "SELECT airport_code FROM Airports"
        cursor.execute(query)
        result = cursor.fetchall()
        existing_airports = [row[0] for row in result]
        if not existing_airports:
            logging.warning("No airports found in the database. Inserting airports from CSV.")
        
        
            with open(CSV_FILE_PATH, mode='r', newline="", encoding='utf-8') as file:
                csv_reader = csv.DictReader(file)
                iatas = [row.get("IATA", "").strip() for row in csv_reader]
                for iata in iatas:
                    flight_data = get_flight_data(token_type, token, iata)
                    
                    if flight_data:
                        insert_flight_data(cursor, connection, flight_data, token, token_type)
                    else:
                        logging.warning(f"No flight data found for {iata}")
            logging.info("Flight data loaded successfully")
            connection.commit()
        else:
            logging.info("Iterating through existing airports")
            for iata in existing_airports:
                flight_data = get_flight_data(token_type, token, iata)
                if flight_data:
                    insert_flight_data(cursor, connection, flight_data, token, token_type)
                else:
                    logging.warning(f"No flight data found for {iata}")

        





    except Error as e:
        logging.exception(f"Error inserting job data for job ID : {e}")
        connection.rollback()



    


def main():
    with open(log_file_path, "w") as log_file:
        # Redirect stdout to the log file
        #original_stdout = sys.stdout
        sys.stdout = log_file
        try:
            cursor, connection = establish_connection()
            if cursor and connection:
                logging.info("Connection established successfully")
                load_data_to_db(cursor, connection, OUTPUT_DIR, CSV_FILE_PATH)
            else:
                logging.error("Failed to establish database connection")
        except Exception as e:
            logging.exception(f"An error occurred: {e}")

           
    
        finally:
            
            logging.info(f"Program completed. Logs are saved in 'output_log.txt'.")
            close_connection(cursor, connection)
                        




if __name__ == "__main__":
    
    log_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output_log.txt")
    OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))  # Get the directory of the current script
    CSV_FILE_PATH = os.path.join(OUTPUT_DIR, 'airports_iata.csv')  # relative path of the csv file
    CSV_FILE_PATH_2 = os.path.join(OUTPUT_DIR, 'airports.csv')  # absolute path of the csv file
    main()
    



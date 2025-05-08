import requests
import sqlite3
import multiprocessing
import os
import platform
import time
import json
import getpass
import http.cookies
from bs4 import BeautifulSoup
import sys

DATABASE_VERSION = 1
COOKIE_FILE = 'geekbench_cookies.json'

LOGIN_URL = 'https://browser.geekbench.com/session/create'
LOGIN_PAGE_URL = 'https://browser.geekbench.com/session/new'

USERNAME_FIELD_NAME = 'user[username]'
PASSWORD_FIELD_NAME = 'user[password]'
AUTHENTICITY_TOKEN_FIELD_NAME = 'authenticity_token'

DATA_COLUMNS = [
    'date', 'version', 'Platform', 'Compiler', 'Operating_System',
    'Model', 'Processor', 'Threads', 'Cores', 'Processors',
    'Processor_Frequency', 'L1_Instruction_Cache', 'L1_Data_Cache',
    'L2_Cache', 'L3_Cache', 'L4_Cache', 'RAM', 'Type',
    'Processor_Minimum_Multiplier', 'Processor_Maximum_Multiplier',
    'Power_Plan', 'Number_of_Channels', 'multicore_score', 'score',
    'AES_XTS_ST_Score', 'AES_XTS_MT_Score',
    'Text_Compression_ST_Score', 'Text_Compression_MT_Score',
    'Image_Compression_ST_Score', 'Image_Compression_MT_Score',
    'Navigation_ST_Score', 'Navigation_MT_Score',
    'HTML5_ST_Score', 'HTML5_MT_Score',
    'SQLite_ST_Score', 'SQLite_MT_Score',
    'PDF_Rendering_ST_Score', 'PDF_Rendering_MT_Score',
    'Text_Rendering_ST_Score', 'Text_Rendering_MT_Score',
    'Clang_ST_Score', 'Clang_MT_Score',
    'Camera_ST_Score', 'Camera_MT_Score',
    'N_Body_Physics_ST_Score', 'N_Body_Physics_MT_Score',
    'Rigid_Body_Physics_ST_Score', 'Rigid_Body_Physics_MT_Score',
    'Gaussian_Blur_ST_Score', 'Gaussian_Blur_MT_Score',
    'Face_Detection_ST_Score', 'Face_Detection_MT_Score',
    'Horizon_Detection_ST_Score', 'Horizon_Detection_MT_Score',
    'Image_Inpainting_ST_Score', 'Image_Inpainting_MT_Score',
    'HDR_ST_Score', 'HDR_MT_Score',
    'Ray_Tracing_ST_Score', 'Ray_Tracing_MT_Score',
    'Structure_from_Motion_ST_Score', 'Structure_from_Motion_MT_Score',
    'Speech_Recognition_ST_Score', 'Speech_Recognition_MT_Score',
    'Machine_Learning_ST_Score', 'Machine_Learning_MT_Score',
]

def save_cookies(cookies, filename):
    """Saves requests.cookies.RequestsCookieJar to a file."""
    try:
        cookie_list = []
        for cookie in cookies:
            cookie_list.append({
                'name': cookie.name,
                'value': cookie.value,
                'path': cookie.path,
                'domain': cookie.domain,
                'expires': cookie.expires,
                'secure': cookie.secure,
                'rest': cookie._rest
            })
        with open(filename, 'w') as f:
            json.dump(cookie_list, f)
        print(f"Cookies saved to {filename}")
    except Exception as e:
        print(f"Error saving cookies: {e}")

def load_cookies(filename):
    """Loads cookies from a file and returns a requests.cookies.RequestsCookieJar."""
    try:
        with open(filename, 'r') as f:
            cookie_list = json.load(f)
        cookies = requests.cookies.RequestsCookieJar()
        for cookie_dict in cookie_list:
            name = cookie_dict.get('name')
            value = cookie_dict.get('value')
            if name is not None and value is not None:
                cookies.set(
                    name, value,
                    path=cookie_dict.get('path'),
                    domain=cookie_dict.get('domain'),
                    expires=cookie_dict.get('expires'),
                    secure=cookie_dict.get('secure', False)
                )
            else:
                 print(f"Skipping invalid cookie entry: {cookie_dict}")
        print(f"Cookies loaded from {filename}")
        return cookies
    except FileNotFoundError:
        print(f"Cookie file not found: {filename}")
        return None
    except Exception as e:
        print(f"Error loading cookies: {e}")
        return None

def login_and_get_cookies(username, password):
    """
    Attempts to log in to Geekbench and returns the session cookies if successful.
    Returns None or handles error if login fails.
    """
    print("Attempting to log in...")
    login_session = requests.Session()

    try:
        print(f"Fetching login page from {LOGIN_PAGE_URL} to get authenticity token...")
        login_page_response = login_session.get(LOGIN_PAGE_URL)
        login_page_response.raise_for_status()

        soup = BeautifulSoup(login_page_response.text, 'html.parser')
        authenticity_token_tag = soup.find('meta', {'name': 'csrf-token'})

        if authenticity_token_tag and 'content' in authenticity_token_tag.attrs:
            authenticity_token = authenticity_token_tag['content']
            print(f"Successfully extracted authenticity token.")
        else:
            print("Could not find authenticity token on the login page.")
            return None

    except requests.RequestException as e:
        print(f"Error fetching login page: {e}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred while parsing login page: {e}")
        return None

    login_payload = {
        AUTHENTICITY_TOKEN_FIELD_NAME: authenticity_token,
        USERNAME_FIELD_NAME: username,
        PASSWORD_FIELD_NAME: password,
    }

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36',
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': LOGIN_PAGE_URL,
        'Content-Type': 'application/x-www-form-urlencoded',
        'Connection': 'keep-alive',
    }

    try:
        print(f"Sending login POST request to {LOGIN_URL}...")
        response = login_session.post(LOGIN_URL, data=login_payload, headers=headers, allow_redirects=False)

        if response.status_code in [200, 302]:
             print("Login request successful based on status code!")
             if login_session.cookies:
                 print("Session cookies obtained.")
                 return login_session.cookies
             else:
                 print("Login request successful, but no session cookies were obtained.")
                 print(f"Response status code: {response.status_code}")
                 return None
        else:
            print(f"Login request failed. Status code: {response.status_code}")
            print(f"Response headers: {response.headers}")
            return None

    except requests.RequestException as e:
        print(f"An error occurred during login POST request: {e}")
        return None

def fetch_data(count, cookies):
    """
    Fetches data for a given ID, saving raw data and extracting key info.
    Uses the provided cookies for authentication.
    Inserts or replaces data in the database.
    Returns True on success, False on JSON decode error, None on request error.
    """
    url = f'https://browser.geekbench.com/v5/cpu/{count}.gb5'

    raw_data_dir = 'raw_data_5'
    if not os.path.exists(raw_data_dir):
        os.makedirs(raw_data_dir)

    raw_file_path = os.path.join(raw_data_dir, f'{count}.gb5')

    conn = sqlite3.connect('geekbench_5_data.db')
    c = conn.cursor()

    data_entry = {
        'id': count,
    }
    for col in DATA_COLUMNS:
        data_entry[col] = None

    raw_text_data = ""

    worker_session = requests.Session()
    if cookies:
        worker_session.cookies.update(cookies)

    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': 'https://browser.geekbench.com/',
            'DNT': '1',
            'Connection': 'keep-alive',
        }

        response = worker_session.get(url, headers=headers)
        response.raise_for_status()

        raw_text_data = response.text

        try:
            with open(raw_file_path, 'w', encoding='utf-8') as f:
                f.write(raw_text_data)
        except IOError as e:
            print(f"Error saving raw data for ID {count} to file: {e}")

        try:
            raw_json_data = json.loads(raw_text_data)

            data_entry['date'] = raw_json_data.get('date')
            data_entry['version'] = raw_json_data.get('version')
            data_entry['multicore_score'] = str(raw_json_data.get('multicore_score')) if raw_json_data.get('multicore_score') is not None else None
            data_entry['score'] = str(raw_json_data.get('score')) if raw_json_data.get('score') is not None else None

            metrics = raw_json_data.get('metrics', [])
            metric_values = {}
            for metric in metrics:
                name = metric.get('name')
                value = metric.get('value')
                if name:
                    metric_values[name] = value

            metric_fields_map = {
                'Platform': 'Platform',
                'Compiler': 'Compiler',
                'Operating System': 'Operating_System',
                'Model': 'Model',
                'Processor': 'Processor',
                'Threads': 'Threads',
                'Cores': 'Cores',
                'Processors': 'Processors',
                'Processor Frequency': 'Processor_Frequency',
                'Size': 'RAM',
                'Type': 'Type',
                'Processor Minimum Multiplier': 'Processor_Minimum_Multiplier',
                'Processor Maximum Multiplier': 'Processor_Maximum_Multiplier',
                'Power Plan': 'Power_Plan',
                'Number of Channels': 'Number_of_Channels'
            }

            for json_name, entry_key in metric_fields_map.items():
                if json_name in metric_values:
                    value = metric_values[json_name]
                    data_entry[entry_key] = str(value) if value is not None else None

            cache_levels = ['L1', 'L2', 'L3', 'L4']
            for level in cache_levels:
                if level == 'L1':
                    cache_types = ['Instruction', 'Data']
                else:
                    cache_types = ['']

                for cache_type in cache_types:
                    cache_type_suffix = f" {cache_type} Cache" if cache_type else " Cache"
                    size_key_json = f'{level}{cache_type_suffix}'
                    count_key_json = f'{level}{cache_type_suffix} Count'
                    data_entry_key = f'{level}{"_" + cache_type if cache_type else ""}_Cache'

                    size = metric_values.get(size_key_json)
                    count = metric_values.get(count_key_json)

                    size_value = 0
                    size_unit = ""
                    if isinstance(size, str):
                        parts = size.split()
                        if len(parts) == 2:
                            try:
                                size_value = float(parts[0])
                                size_unit = parts[1].upper()
                            except ValueError:
                                pass

                    count_value = 0
                    if isinstance(count, (int, float)):
                        count_value = float(count)
                    elif isinstance(count, str):
                         try:
                             count_value = float(count)
                         except ValueError:
                             pass

                    if size_value > 0 and count_value > 0:
                         data_entry[data_entry_key] = f"{int(count_value)}x {size}"
                    else:
                         data_entry[data_entry_key] = None


            sections = raw_json_data.get('sections', [])

            for section in sections:
                workloads = section.get('workloads', [])
                for workload in workloads:
                    workload_name = workload.get('name')
                    if workload_name:
                        column_base_name = workload_name.replace(' ', '_').replace('-', '_').replace('.', '_')
                        score = workload.get('score')
                        section_name = section.get('name')

                        if section_name == 'Single-Core':
                            st_column_name = f'{column_base_name}_ST_Score'
                            if st_column_name in data_entry:
                                data_entry[st_column_name] = str(score) if score is not None else None

                        elif section_name == 'Multi-Core':
                            mt_column_name = f'{column_base_name}_MT_Score'
                            if mt_column_name in data_entry:
                                data_entry[mt_column_name] = str(score) if score is not None else None

            columns = ', '.join(data_entry.keys())
            placeholders = ', '.join('?' * len(data_entry))
            sql = f"INSERT OR REPLACE INTO data ({columns}) VALUES ({placeholders})"
            values = tuple(data_entry.values())

            try:
                c.execute(sql, values)
                conn.commit()
                return True
            except sqlite3.Error as e:
                print(f"Database error inserting/replacing data for ID {count}: {e}")
                conn.rollback()
                return None
            finally:
                conn.close()

        except json.JSONDecodeError as e:
            print(f"JSON Decode Error for ID {count}: {e}. Response text starts with: {raw_text_data[:500]}...")
            return False
        except Exception as e:
            print(f"An unexpected error occurred for ID {count} during parsing key data: {e}")
            return None

    except requests.HTTPError as e:
        print(f"HTTP Error for ID {count}: {e}")
        if e.response.status_code == 404:
             try:
                 c.execute("INSERT OR IGNORE INTO data (id) VALUES (?)", (count,))
                 conn.commit()
                 print(f"ID {count} returned 404, marked as checked in DB.")
             except sqlite3.Error as db_err:
                 print(f"Database error marking ID {count} as checked: {db_err}")
             finally:
                 conn.close()
             return True
        elif e.response.status_code in [401, 403]:
             print(f"Authentication/Authorization error for ID {count}. Cookies might be invalid.")
             conn.close()
             return None
        else:
            conn.close()
            return None

    except requests.RequestException as e:
        print(f"Request Exception for ID {count}: {e}")
        conn.close()
        return None

    except Exception as e:
        print(f"An unexpected error occurred for ID {count}: {e}")
        conn.close()
        return None


def get_db_connection():
    """Gets a database connection."""
    return sqlite3.connect('geekbench_5_data.db')

def initialize_database():
    """Initializes the database table if it doesn't exist or version mismatch occurs."""
    conn = get_db_connection()
    c = conn.cursor()

    c.execute('''CREATE TABLE IF NOT EXISTS db_version
                 (version INTEGER PRIMARY KEY)''')

    current_version = None
    c.execute('SELECT version FROM db_version LIMIT 1')
    row = c.fetchone()
    if row:
        current_version = row[0]

    columns_sql = 'id INTEGER PRIMARY KEY'
    for col in DATA_COLUMNS:
        columns_sql += f', {col} TEXT'

    create_data_table_sql = f'CREATE TABLE IF NOT EXISTS data ({columns_sql})'
    c.execute(create_data_table_sql)
    conn.commit()

    if current_version != DATABASE_VERSION:
        print(f"Database version mismatch or first run. Expected {DATABASE_VERSION}, found {current_version}. "
              "WARNING: This versioning strategy drops and recreates the 'data' table.")
        c.execute('DROP TABLE IF EXISTS data')
        conn.commit()
        c.execute(create_data_table_sql)
        conn.commit()

        c.execute('DELETE FROM db_version')
        c.execute('INSERT INTO db_version VALUES (?)', (DATABASE_VERSION,))
        conn.commit()
        print("Database table recreated due to version mismatch.")

    conn.close()

def get_last_id_from_db():
    """Gets the highest ID from the data table."""
    conn = get_db_connection()
    c = conn.cursor()
    c.execute('SELECT MAX(id) FROM data')
    last_id = c.fetchone()[0]
    conn.close()
    return last_id if last_id is not None else 0

def find_all_null_rows_ids(data_columns):
    """Finds IDs where all specified data columns are NULL."""
    conn = get_db_connection()
    c = conn.cursor()

    where_clause = ' AND '.join([f'"{col}" IS NULL' for col in data_columns])
    sql = f'SELECT id FROM data WHERE {where_clause}'

    print(f"Executing query to find all-NULL rows...")
    try:
        c.execute(sql)
        null_ids = [row[0] for row in c.fetchall()]
        print(f"Found {len(null_ids)} rows with all data columns as NULL.")
    except sqlite3.Error as e:
        print(f"Database error finding all-NULL rows: {e}")
        null_ids = []
    finally:
        conn.close()
    return null_ids

def process_ids_with_pool(id_list, pool, cookies, description="Processing"):
    """Processes a list of IDs using the multiprocessing pool."""
    total_ids = len(id_list)
    processed_count = 0
    needs_relogin = False
    successful_fetches = 0
    failed_fetches = []
    parsing_errors = []

    print(f"Starting {description} for {total_ids} IDs...")

    batch_size = pool._processes

    for i in range(0, total_ids, batch_size):
        if needs_relogin:
            print("Re-login needed. Stopping current processing batch.")
            break

        current_batch_ids = id_list[i : i + batch_size]
        print(f"{description} batch: {i+1}-{min(i+batch_size, total_ids)}")

        jobs = [pool.apply_async(fetch_data, args=(id, cookies,)) for id in current_batch_ids]

        batch_needs_relogin = False
        for j, job in enumerate(jobs):
            try:
                result = job.get()
                processed_count += 1
                if result is True:
                    successful_fetches += 1
                elif result is False:
                    parsing_errors.append(current_batch_ids[j])
                    batch_needs_relogin = True
                    print(f"Parsing error for ID {current_batch_ids[j]}. Setting batch_needs_relogin.")
                elif result is None:
                    failed_fetches.append(current_batch_ids[j])
                    batch_needs_relogin = True
                    print(f"Request error for ID {current_batch_ids[j]}. Setting batch_needs_relogin.")

            except Exception as e:
                print(f"An unexpected error occurred getting result for ID in batch: {e}")
                failed_fetches.extend(current_batch_ids[j:])
                batch_needs_relogin = True

        if batch_needs_relogin:
             needs_relogin = True
             print("Batch finished with errors. Re-login will be attempted.")
             break

    print(f"{description} phase finished.")
    print(f"Successful fetches: {successful_fetches}")
    if failed_fetches:
        print(f"Failed fetches (request/other errors): {len(failed_fetches)}")
    if parsing_errors:
        print(f"Parsing errors (JSON): {len(parsing_errors)}")

    return not needs_relogin

if __name__ == '__main__':
    print("Geekbench Data Scraper - Version 1.0")

    initialize_database()

    authenticated_cookies = None
    authenticated_cookies = load_cookies(COOKIE_FILE)

    while not authenticated_cookies:
        print("No saved cookies found or failed to load cookies. Proceeding with login.")
        print("Please enter your Geekbench account credentials to log in.")
        username = input("Username (Email): ")
        password = getpass.getpass("Password: ")

        authenticated_cookies = login_and_get_cookies(username, password)

        if not authenticated_cookies:
             print("Authentication failed. Please try again.")

    save_cookies(authenticated_cookies, COOKIE_FILE)
    print("Authentication successful.")

    startTime = time.time()

    print("\n--- Phase 1: Checking and Re-fetching All-NULL Rows ---")
    data_columns = DATA_COLUMNS
    ids_to_refetch = find_all_null_rows_ids(data_columns)

    if ids_to_refetch:
        print(f"Found {len(ids_to_refetch)} IDs with all data columns NULL that need re-fetching.")
        if input("Do you want to re-fetch these IDs? (y/n): ").strip().lower() == 'y':
            relogin_attempts = 0
            max_relogin_attempts = 3

            while ids_to_refetch and relogin_attempts < max_relogin_attempts:
                print(f"\nAttempt {relogin_attempts + 1} for re-fetching all-NULL rows.")
                try:
                    with multiprocessing.Pool(processes=6) as pool:
                         processing_successful = process_ids_with_pool(
                             ids_to_refetch, pool, authenticated_cookies, description="Re-fetching NULL IDs"
                         )

                    if processing_successful:
                        print("Re-fetching of all-NULL rows completed successfully.")
                        ids_to_refetch = []
                    else:
                        print("Re-login required for re-fetching phase.")
                        authenticated_cookies = None
                        while not authenticated_cookies:
                             print("Please re-enter credentials to continue re-fetching.")
                             username = input("Username (Email): ")
                             password = getpass.getpass("Password: ")
                             authenticated_cookies = login_and_get_cookies(username, password)
                             if not authenticated_cookies:
                                  print("Re-login failed. Trying again.")
                             else:
                                  save_cookies(authenticated_cookies, COOKIE_FILE)
                                  print("Re-login successful. Resuming re-fetching.")
                                  ids_to_refetch = find_all_null_rows_ids(data_columns)
                                  relogin_attempts += 1
                                  break

                except Exception as e:
                    print(f"An unexpected error occurred during the re-fetching phase: {e}")
                    relogin_attempts += 1
                    time.sleep(1)

            if ids_to_refetch:
                 print(f"Warning: {len(ids_to_refetch)} IDs could not be re-fetched after multiple attempts.")
            else:
                 print("All identified all-NULL rows have been processed or re-fetched.")

        else:
            print("Skipping re-fetching of all-NULL rows.")
    else:
        print("No rows found with all data columns as NULL.")

    print("\n--- Phase 2: Continuing Scraping New IDs ---")
    last_id_after_refetch = get_last_id_from_db()
    start_scraping_id = last_id_after_refetch + 1

    print(f"Current highest ID in database: {last_id_after_refetch}")
    print(f"Starting scraping for new IDs from: {start_scraping_id}")

    if input("Do you want to continue scraping new data? (y/n): ").strip().lower() != 'y':
        print("Exiting script.")
        sys.exit()

    if platform.system() == 'Windows':
        os.system('cls')
    else:
        os.system('clear')

    relogin_attempts = 0
    max_relogin_attempts = 5

    while relogin_attempts < max_relogin_attempts:
        print(f"\nAttempt {relogin_attempts + 1} for scraping new IDs from {start_scraping_id}.")
        try:
            with multiprocessing.Pool(processes=6) as pool:
                current_id = start_scraping_id
                while True:
                     batch_ids = list(range(current_id, current_id + 6))
                     print(f"Fetching batch: {batch_ids[0]}-{batch_ids[-1]}")

                     jobs = [pool.apply_async(fetch_data, args=(id, authenticated_cookies,)) for id in batch_ids]

                     batch_needs_relogin = False
                     for j, job in enumerate(jobs):
                         try:
                             result = job.get()
                             if result is False or result is None:
                                 print(f"Error processing ID {batch_ids[j]} in new scraping phase.")
                                 batch_needs_relogin = True
                         except Exception as e:
                              print(f"An unexpected error occurred getting result for ID {batch_ids[j]}: {e}")
                              batch_needs_relogin = True

                     if batch_needs_relogin:
                          print("Batch finished with errors. Re-login will be attempted.")
                          start_scraping_id = batch_ids[0]
                          break

                     current_id += len(batch_ids)

            if not batch_needs_relogin:
                 pass

        except Exception as e:
             print(f"An unexpected error occurred during the new scraping phase: {e}")

        if batch_needs_relogin:
            print("Re-login required for new scraping phase.")
            authenticated_cookies = None
            while not authenticated_cookies:
                 print("Please re-enter credentials to continue scraping.")
                 username = input("Username (Email): ")
                 password = getpass.getpass("Password: ")
                 authenticated_cookies = login_and_get_cookies(username, password)
                 if not authenticated_cookies:
                      print("Re-login failed. Trying again.")
                 else:
                      save_cookies(authenticated_cookies, COOKIE_FILE)
                      print("Re-login successful. Resuming scraping from ID", start_scraping_id)
                      relogin_attempts += 1
                      break
        else:
            pass

    print("\nStarting continuous scraping...")
    current_id = start_scraping_id
    relogin_attempts = 0
    max_relogin_attempts = 5

    while relogin_attempts < max_relogin_attempts:
         needs_relogin = False

         try:
              with multiprocessing.Pool(processes=6) as pool:
                   while True:
                        batch_ids = list(range(current_id, current_id + 6))
                        print(f"Fetching batch: {batch_ids[0]}-{batch_ids[-1]}")

                        jobs = [pool.apply_async(fetch_data, args=(id, authenticated_cookies,)) for id in batch_ids]

                        batch_failed = False
                        for j, job in enumerate(jobs):
                            try:
                                result = job.get()
                                if result is False or result is None:
                                    print(f"Error processing ID {batch_ids[j]}. Batch failed.")
                                    batch_failed = True
                                    needs_relogin = True
                                    break
                            except Exception as e:
                                print(f"An unexpected error getting result for ID {batch_ids[j]}: {e}")
                                batch_failed = True
                                needs_relogin = True
                                break

                        if batch_failed:
                             start_scraping_id = batch_ids[0]
                             print("Batch failed. Attempting re-login...")
                             break

                        current_id += len(batch_ids)

         except Exception as e:
             print(f"An unexpected error occurred setting up the pool or in main loop: {e}")
             needs_relogin = True


         if needs_relogin:
            print("Re-login procedure initiated.")
            authenticated_cookies = None
            relogin_attempts += 1
            if relogin_attempts >= max_relogin_attempts:
                 print("Max re-login attempts reached. Exiting.")
                 break

            while not authenticated_cookies:
                 print(f"Attempt {relogin_attempts}/{max_relogin_attempts} to re-login.")
                 username = input("Username (Email): ")
                 password = getpass.getpass("Password: ")
                 authenticated_cookies = login_and_get_cookies(username, password)
                 if not authenticated_cookies:
                      print("Re-login failed. Trying again.")
                 else:
                      save_cookies(authenticated_cookies, COOKIE_FILE)
                      print("Re-login successful. Resuming scraping.")
                      break

         else:
              print("Scraping loop finished without needing re-login (unlikely in infinite loop).")
              break

    print("\nScraping process ended.")
    endTime = time.time()
    print(f"Total time taken: {endTime - startTime:.2f} seconds")

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
import zipfile

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

metric_id_map = {
    'Platform': (1, 'value'),
    'Compiler': (2, 'value'),
    'Operating_System': (3, 'value'),
    'Model': (5, 'value'),
    'Processor': (9, 'value'),
    'Threads': (12, 'value'),
    'Cores': (13, 'value'),
    'Processors': (14, 'value'),
    'Processor_Frequency': (15, 'value'),
    'RAM': (29, 'value'),
    'Type': (30, 'value'),
    'Processor_Minimum_Multiplier': (66, 'value'),
    'Processor_Maximum_Multiplier': (67, 'value'),
    'Power_Plan': (70, 'value'),
    'Number_of_Channels': (76, 'value'),
}

cache_id_map = {
    'L1_Instruction_Cache': {'size_id': 17, 'count_id': 18},
    'L1_Data_Cache': {'size_id': 19, 'count_id': 20},
    'L2_Cache': {'size_id': 21, 'count_id': 22},
    'L3_Cache': {'size_id': 23, 'count_id': 24},
    'L4_Cache': {'size_id': 25, 'count_id': 26},
}

workload_id_map = {
    'AES_XTS_ST_Score': (1, 101),
    'AES_XTS_MT_Score': (2, 101),
    'Text_Compression_ST_Score': (1, 201),
    'Text_Compression_MT_Score': (2, 201),
    'Image_Compression_ST_Score': (1, 202),
    'Image_Compression_MT_Score': (2, 202),
    'Navigation_ST_Score': (1, 203),
    'Navigation_MT_Score': (2, 203),
    'HTML5_ST_Score': (1, 204),
    'HTML5_MT_Score': (2, 204),
    'SQLite_ST_Score': (1, 205),
    'SQLite_MT_Score': (2, 205),
    'PDF_Rendering_ST_Score': (1, 206),
    'PDF_Rendering_MT_Score': (2, 206),
    'Text_Rendering_ST_Score': (1, 207),
    'Text_Rendering_MT_Score': (2, 207),
    'Clang_ST_Score': (1, 208),
    'Clang_MT_Score': (2, 208),
    'Camera_ST_Score': (1, 209),
    'Camera_MT_Score': (2, 209),
    'N_Body_Physics_ST_Score': (1, 301),
    'N_Body_Physics_MT_Score': (2, 301),
    'Rigid_Body_Physics_ST_Score': (1, 302),
    'Rigid_Body_Physics_MT_Score': (2, 302),
    'Gaussian_Blur_ST_Score': (1, 303),
    'Gaussian_Blur_MT_Score': (2, 303),
    'Face_Detection_ST_Score': (1, 305),
    'Face_Detection_MT_Score': (2, 305),
    'Horizon_Detection_ST_Score': (1, 306),
    'Horizon_Detection_MT_Score': (2, 306),
    'Image_Inpainting_ST_Score': (1, 307),
    'Image_Inpainting_MT_Score': (2, 307),
    'HDR_ST_Score': (1, 308),
    'HDR_MT_Score': (2, 308),
    'Ray_Tracing_ST_Score': (1, 309),
    'Ray_Tracing_MT_Score': (2, 309),
    'Structure_from_Motion_ST_Score': (1, 310),
    'Structure_from_Motion_MT_Score': (2, 310),
    'Speech_Recognition_ST_Score': (1, 312),
    'Speech_Recognition_MT_Score': (2, 312),
    'Machine_Learning_ST_Score': (1, 313),
    'Machine_Learning_MT_Score': (2, 313),
}


def save_cookies(cookies, filename):
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
    print("Attempting to log in...")
    login_session = requests.Session()

    try:
        print(f"Fetching login page from {LOGIN_PAGE_URL} to get authenticity token...")
        login_page_response = login_session.get(LOGIN_PAGE_URL, timeout=10)
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
        response = login_session.post(LOGIN_URL, data=login_payload, headers=headers, allow_redirects=False, timeout=10)

        if response.status_code == 302 or (response.status_code == 200 and 'Set-Cookie' in response.headers):
             print("Login request successful based on status code/headers!")
             if login_session.cookies:
                 print("Session cookies obtained.")
                 return login_session.cookies
             else:
                 print("Login request successful, but no session cookies were obtained.")
                 print(f"Response status code: {response.status_code}")
                 return None
        else:
            print(f"Login request failed. Status code: {response.status_code}")
            try:
                error_data = response.json()
                print(f"Error response: {error_data}")
            except json.JSONDecodeError:
                print("No JSON error response.")
            print(f"Response headers: {response.headers}")
            return None

    except requests.RequestException as e:
        print(f"An error occurred during login POST request: {e}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred during login: {e}")
        return None

def fetch_data(count, cookies, last_id_in_db):
    url = f'https://browser.geekbench.com/v5/cpu/{count}.gb5'

    raw_data_dir = 'raw_data_5'
    if not os.path.exists(raw_data_dir):
        os.makedirs(raw_data_dir)

    raw_file_path = os.path.join(raw_data_dir, f'{count}.gb5')

    raw_text_data = None

    if count <= last_id_in_db and os.path.exists(raw_file_path):
        try:
            with open(raw_file_path, 'r', encoding='utf-8') as f:
                raw_text_data = f.read()
        except Exception as e:
            print(f"Error reading local file for ID {count}: {e}. Falling back to network.")
            raw_text_data = None

    conn = None
    try:
        conn = sqlite3.connect('geekbench_5_data.db')
        c = conn.cursor()

        data_entry = {
            'id': count,
        }
        for col in DATA_COLUMNS:
            data_entry[col] = None

        if raw_text_data is None:
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

                response = worker_session.get(url, headers=headers, timeout=10)
                response.raise_for_status()

                raw_text_data = response.text

                try:
                    with open(raw_file_path, 'w', encoding='utf-8') as f:
                        f.write(raw_text_data)
                except IOError as e:
                    print(f"Error saving raw data for ID {count} to file: {e}")

            except requests.HTTPError as e:
                print(f"HTTP Error for ID {count}: {e}")
                if e.response.status_code == 404:
                     try:
                         c.execute("INSERT OR REPLACE INTO data (id) VALUES (?)", (count,))
                         conn.commit()
                         print(f"ID {count} returned 404, marked as checked in DB with NULL data.")
                     except sqlite3.Error as db_err:
                         print(f"Database error marking ID {count} as checked (404): {db_err}")
                         return False
                     return True
                elif e.response.status_code in [401, 403]:
                     print(f"Authentication/Authorization error for ID {count}.")
                     return 'auth_error'
                else:
                    print(f"Other HTTP error {e.response.status_code} for ID {count}.")
                    return False

            except requests.RequestException as e:
                print(f"Request Exception for ID {count}: {e}")
                return False

            except Exception as e:
                print(f"An unexpected error occurred for ID {count} during network fetch: {e}")
                return False


        if raw_text_data is not None:
            try:
                raw_json_data = json.loads(raw_text_data)

                data_entry['date'] = raw_json_data.get('date')
                data_entry['version'] = raw_json_data.get('version')
                data_entry['multicore_score'] = str(raw_json_data.get('multicore_score')) if raw_json_data.get('multicore_score') is not None else None
                data_entry['score'] = str(raw_json_data.get('score')) if raw_json_data.get('score') is not None else None

                metrics = raw_json_data.get('metrics', [])
                metrics_by_id = {metric.get('id'): metric for metric in metrics if metric.get('id') is not None}

                for db_col, (metric_id, json_key) in metric_id_map.items():
                    metric = metrics_by_id.get(metric_id)
                    if metric:
                        value = metric.get(json_key)
                        data_entry[db_col] = str(value) if value is not None else None

                for db_col, ids in cache_id_map.items():
                    size_metric = metrics_by_id.get(ids.get('size_id'))
                    count_metric = metrics_by_id.get(ids.get('count_id'))

                    size = size_metric.get('value') if size_metric else None
                    count = count_metric.get('value') if count_metric else None

                    if size is not None and count is not None:
                        try:
                            count_value_num = float(count)
                            if count_value_num > 0:
                                data_entry[db_col] = f"{int(count_value_num)}x {size}"
                            else:
                                data_entry[db_col] = str(size) if size is not None else None
                        except (ValueError, TypeError):
                             data_entry[db_col] = str(size) if size is not None else None
                    elif size is not None:
                        data_entry[db_col] = str(size)
                    elif count is not None:
                         data_entry[db_col] = str(count)
                    else:
                        data_entry[db_col] = None

                sections = raw_json_data.get('sections', [])
                workloads_by_section_and_id = {}
                for section in sections:
                    section_id = section.get('id')
                    if section_id is not None:
                        workloads_by_section_and_id[section_id] = {}
                        for workload in section.get('workloads', []):
                            workload_id = workload.get('id')
                            if workload_id is not None:
                                workloads_by_section_and_id[section_id][workload_id] = workload

                for db_col, (section_id, workload_id) in workload_id_map.items():
                    section_data = workloads_by_section_and_id.get(section_id)
                    if section_data:
                        workload_data = section_data.get(workload_id)
                        if workload_data:
                            score = workload_data.get('score')
                            data_entry[db_col] = str(score) if score is not None else None


            except json.JSONDecodeError as e:
                print(f"JSON Decode Error for ID {count}: {e}. Response text starts with: {raw_text_data[:500]}...")
                error_occured_during_parsing = True
            except Exception as e:
                print(f"An unexpected error occurred for ID {count} during JSON parsing/data extraction: {e}")
                error_occured_during_parsing = True
            else:
                 error_occured_during_parsing = False


            columns = ', '.join(data_entry.keys())
            placeholders = ', '.join('?' * len(data_entry))
            sql = f"INSERT OR REPLACE INTO data ({columns}) VALUES ({placeholders})"
            values = tuple(data_entry.values())

            try:
                c.execute(sql, values)
                conn.commit()
                if error_occured_during_parsing:
                    print(f"Successfully saved NULL/partial data for ID {count} after parsing errors.")
                    return False
                else:
                    return True
            except sqlite3.Error as e:
                print(f"Database error inserting/replacing data for ID {count}: {e}")
                conn.rollback()
                return False
        else:
             print(f"Failed to obtain raw data for ID {count} from local file or network.")
             try:
                 c.execute("INSERT OR IGNORE INTO data (id) VALUES (?)", (count,))
                 conn.commit()
                 print(f"Marked ID {count} as attempted with NULL data.")
             except sqlite3.Error as db_err:
                 print(f"Database error marking ID {count} as attempted (no data): {db_err}")

             return False


    except sqlite3.Error as e:
        print(f"Critical Database connection error for ID {count}: {e}")
        return False

    finally:
        if conn:
            conn.close()

def get_db_connection():
    return sqlite3.connect('geekbench_5_data.db')

def initialize_database():
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()

        c.execute('''CREATE TABLE IF NOT EXISTS db_version
                     (version REAL PRIMARY KEY)''')

        current_version = None
        c.execute('SELECT version FROM db_version LIMIT 1')
        row = c.fetchone()
        if row:
            current_version = row[0]

        columns_sql = 'id INTEGER PRIMARY KEY'
        for col in DATA_COLUMNS:
            columns_sql += f', "{col}" TEXT'

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
        else:
            print(f"Database version {DATABASE_VERSION} is current.")

    except sqlite3.Error as e:
        print(f"Database initialization error: {e}")
        sys.exit(1)
    finally:
        if conn:
            conn.close()

def get_last_id_from_db():
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute('SELECT MAX(id) FROM data')
        last_id = c.fetchone()[0]
        return last_id if last_id is not None else 0
    except sqlite3.Error as e:
        print(f"Database error getting last ID: {e}")
        return 0
    finally:
        if conn:
            conn.close()


def find_all_null_rows_ids(data_columns):
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()

        where_clause = ' AND '.join([f'"{col}" IS NULL' for col in data_columns])
        sql = f'SELECT id FROM data WHERE {where_clause}'

        print(f"Executing query to find all-NULL rows...")
        c.execute(sql)
        null_ids = [row[0] for row in c.fetchall()]
        print(f"Found {len(null_ids)} rows with all data columns as NULL.")
        return null_ids
    except sqlite3.Error as e:
        print(f"Database error finding all-NULL rows: {e}")
        return []
    finally:
        if conn:
            conn.close()

def compress_raw_data(data_dir='raw_data_5', group_size=2000):
    print(f"Starting compression of raw data in {data_dir}...")
    if not os.path.exists(data_dir):
        print(f"Raw data directory {data_dir} not found. Skipping compression.")
        return

    files_in_dir = []
    highest_id_in_directory = 0
    for filename in os.listdir(data_dir):
        if filename.endswith('.gb5'):
            try:
                file_id = int(filename[:-4])
                files_in_dir.append((file_id, filename))
                if file_id > highest_id_in_directory:
                    highest_id_in_directory = file_id
            except ValueError:
                print(f"Skipping invalid filename in raw data directory: {filename}")
                continue

    if not files_in_dir:
        print("No .gb5 files found for compression.")
        return

    files_in_dir.sort()

    # Calculate the ID threshold for compression. Compress files with ID <= compress_up_to_id.
    # This is the end of the last full 2000-ID block before the block containing the highest ID.
    compress_up_to_id = (highest_id_in_directory // group_size) * group_size
    if highest_id_in_directory % group_size == 0 and highest_id_in_directory > 0:
         # If the highest ID is a multiple of 2000, the block containing it is a full block.
         # We still want to skip this block according to the user's example (38001-40000 skipped if highest is 39000).
         # So compress up to the end of the *previous* block.
         compress_up_to_id = highest_id_in_directory - group_size
         if compress_up_to_id < 0: # Handle cases where highest is <= group_size
             compress_up_to_id = 0


    print(f"Highest ID found in directory: {highest_id_in_directory}")
    print(f"Will compress files with ID up to: {compress_up_to_id}")

    compressed_groups = 0
    deleted_files_count = 0
    current_file_index = 0 # Keep track of our position in the sorted files_in_dir list

    # Iterate through potential 2000-ID ranges up to the compression threshold
    current_range_start = 1
    while current_range_start <= compress_up_to_id:
        current_range_end = current_range_start + group_size - 1
        zip_filename = os.path.join(data_dir, f'{current_range_start}-{current_range_end}.zip')

        # Collect all files from the sorted list that fall within the current ID range
        group_files_to_compress = []
        # Advance current_file_index past files smaller than current_range_start
        while current_file_index < len(files_in_dir) and files_in_dir[current_file_index][0] < current_range_start:
             current_file_index += 1

        # Collect files within the current range [current_range_start, current_range_end]
        start_index_for_this_range = current_file_index # Files for this range start from here
        while current_file_index < len(files_in_dir) and files_in_dir[current_file_index][0] <= current_range_end:
             # We've already ensured file_id <= compress_up_to_id by the outer loop condition
             group_files_to_compress.append(files_in_dir[current_file_index])
             current_file_index += 1 # Move to the next file in the overall sorted list


        if group_files_to_compress: # Only compress if there are files in this ID range
             print(f"Compressing files in ID range {current_range_start}-{current_range_end} ({len(group_files_to_compress)} files found)...")

             try:
                 with zipfile.ZipFile(zip_filename, 'w', zipfile.ZIP_DEFLATED) as zipf:
                     for file_id, filename in group_files_to_compress:
                         file_path = os.path.join(data_dir, filename)
                         if os.path.exists(file_path):
                             zipf.write(file_path, filename)
                         else:
                             print(f"Warning: File {file_path} not found during zipping.")

                 print(f"Compression successful. Deleting original files for ID range {current_range_start}-{current_range_end}.")
                 for file_id, filename in group_files_to_compress:
                      file_path = os.path.join(data_dir, filename)
                      try:
                          if os.path.exists(file_path):
                              os.remove(file_path)
                              deleted_files_count += 1
                      except OSError as e:
                          print(f"Error deleting file {file_path}: {e}")
                 compressed_groups += 1

             except Exception as e:
                 print(f"Error compressing ID range {current_range_start}-{current_range_end}: {e}. Original files NOT deleted for this range.")

        # Move to the start of the next 2000-ID range
        current_range_start = current_range_end + 1


    print("Compression process finished.")
    print(f"Compressed {compressed_groups} groups.")
    print(f"Deleted {deleted_files_count} original files.")


def process_ids_with_pool(id_list, pool, cookies, last_id_in_db, description="Processing"):
    total_ids_attempted = len(id_list)
    successful_fetches = 0
    failed_fetches = 0
    auth_error_ids = []

    print(f"Starting {description} for {total_ids_attempted} IDs...")

    if not id_list:
        print(f"No IDs to {description}.")
        return (0, 0, 0, [])

    batch_size = pool._processes

    processed_count = 0
    for i in range(0, total_ids_attempted, batch_size):
        current_batch_ids = id_list[i : i + batch_size]
        print(f"{description} batch: {i+1}-{min(i+batch_size, total_ids_attempted)}")

        jobs = []
        for id in current_batch_ids:
             jobs.append(pool.apply_async(fetch_data, args=(id, cookies, last_id_in_db,)))

        batch_auth_errors_found = False
        for j, job in enumerate(jobs):
            processed_count += 1
            current_id = current_batch_ids[j]

            try:
                result = job.get()

                if result is True:
                    successful_fetches += 1
                elif result == 'auth_error':
                    auth_error_ids.append(current_id)
                    batch_auth_errors_found = True
                elif result is False:
                    failed_fetches += 1

            except Exception as e:
                print(f"An unexpected error occurred getting result for ID {current_id} in batch: {e}")
                failed_fetches += 1

        print(f"Batch {i+1}-{min(i+batch_size, total_ids_attempted)} processed.")
        print(f"  Batch Summary: Success/404: {successful_fetches}, Failed (Other): {failed_fetches}, Auth Errors: {len(auth_error_ids)}")

        if batch_auth_errors_found:
             print("Authentication errors detected in batch. Stopping further batch processing in this call.")
             break


    print(f"{description} phase processing finished.")
    print(f"Total attempted: {processed_count}")
    print(f"Total successful/404 handled: {successful_fetches}")
    print(f"Total failed (other errors): {failed_fetches}")
    if auth_error_ids:
        print(f"Total authentication errors: {len(auth_error_ids)}")


    return (processed_count, successful_fetches, failed_fetches, auth_error_ids)


if __name__ == '__main__':
    print("Geekbench Data Scraper - Version 1.1")

    print("\n--- Cleaning: Compressing raw data ---")
    compress_raw_data(group_size=2000)
    print("--- Cleaning finished ---")

    initialize_database()

    authenticated_cookies = None
    print("Attempting to load cookies from file...")
    authenticated_cookies = load_cookies(COOKIE_FILE)

    initial_login_attempts = 0
    max_initial_login_attempts = 3
    while not authenticated_cookies and initial_login_attempts < max_initial_login_attempts:
        print(f"\nAttempt {initial_login_attempts + 1}/{max_initial_login_attempts} for initial login.")
        print("No valid saved cookies found. Proceeding with login.")
        username = input("Username (Email): ")
        password = getpass.getpass("Password: ")

        authenticated_cookies = login_and_get_cookies(username, password)

        if not authenticated_cookies:
            print("Authentication failed. Please try again.")
            initial_login_attempts += 1
        else:
            save_cookies(authenticated_cookies, COOKIE_FILE)
            print("Initial authentication successful.")
            break

    if not authenticated_cookies:
         print("\nFailed to authenticate after multiple attempts. Exiting script.")
         sys.exit(1)

    last_id_in_db_at_start = get_last_id_from_db()
    print(f"\nHighest ID found in database at start: {last_id_in_db_at_start}")

    startTime = time.time()
    pool_processes = 6

    try:
        with multiprocessing.Pool(processes=pool_processes) as pool:

            fetch_specific = input("\nDo you want to fetch specific IDs? (y/n): ").strip().lower()
            if fetch_specific == 'y':
                ids_input = input("Enter IDs separated by commas (e.g., 5, 9, 1400): ")
                specific_ids_str_list = [id_str.strip() for id_str in ids_input.split(',')]
                specific_ids = []
                invalid_inputs = []

                for id_str in specific_ids_str_list:
                    if id_str:
                        try:
                            specific_ids.append(int(id_str))
                        except ValueError:
                            invalid_inputs.append(id_str)

                if invalid_inputs:
                    print(f"Warning: Skipping invalid inputs: {', '.join(invalid_inputs)}")

                specific_ids = sorted(list(set(specific_ids)))

                if specific_ids:
                    print(f"\nAttempting to fetch specific IDs: {specific_ids}")

                    specific_relogin_attempts = 0
                    max_specific_relogin_attempts = 3

                    ids_to_process_in_specific_phase = list(specific_ids)
                    while ids_to_process_in_specific_phase and specific_relogin_attempts < max_specific_relogin_attempts:
                        print(f"\nFetching Specific IDs (Attempt {specific_relogin_attempts + 1}/{max_specific_relogin_attempts}) for {len(ids_to_process_in_specific_phase)} IDs.")

                        if not authenticated_cookies:
                            print("Cookies invalid. Attempting re-login for specific ID fetching.")
                            username = input("Username (Email): ")
                            password = getpass.getpass("Password: ")
                            authenticated_cookies = login_and_get_cookies(username, password)
                            if not authenticated_cookies:
                                print("Re-login failed for specific ID fetching. Trying again.")
                                specific_relogin_attempts += 1
                                time.sleep(1)
                                continue
                            else:
                                save_cookies(authenticated_cookies, COOKIE_FILE)
                                print("Re-login successful. Resuming specific ID fetching.")

                        (processed, success, failed, auth_errs) = process_ids_with_pool(
                            ids_to_process_in_specific_phase, pool, authenticated_cookies, last_id_in_db_at_start, description="Specific IDs Fetching"
                        )

                        if auth_errs:
                            print("\nAuthentication errors encountered while fetching specific IDs.")
                            authenticated_cookies = None
                            specific_relogin_attempts += 1
                            time.sleep(1)
                            continue

                        ids_to_process_in_specific_phase = []
                        print("\nSpecific IDs fetching completed successfully (or max re-login attempts reached for this phase).")
                        break

                    if ids_to_process_in_specific_phase:
                        print(f"\nWarning: {len(ids_to_process_in_specific_phase)} specific IDs could not be attempted after multiple re-login attempts for this phase.")
                    else:
                         print("\nSpecific IDs fetching phase finished.")

                else:
                    print("No valid specific IDs entered. Skipping specific ID fetching.")

                continue_normal_scraping = input("Do you want to continue with standard scraping (NULL re-fetching and new IDs)? (y/n): ").strip().lower()
                if continue_normal_scraping != 'y':
                     print("Exiting script as requested after fetching specific IDs.")
                     sys.exit(0)

            print("\n--- Phase 1: Attempting to fetch existing All-NULL Rows ---")
            data_columns_to_check = DATA_COLUMNS
            ids_to_refetch = find_all_null_rows_ids(data_columns_to_check)

            if ids_to_refetch:
                print(f"Found {len(ids_to_refetch)} IDs with all specified data columns NULL.")
                if input("Do you want to attempt fetching these IDs once? (y/n): ").strip().lower() == 'y':
                    refetch_relogin_attempts = 0
                    max_refetch_relogin_attempts = 3

                    initial_null_ids = list(ids_to_refetch)
                    while initial_null_ids and refetch_relogin_attempts < max_refetch_relogin_attempts:
                        print(f"\nRefetching (Attempt {refetch_relogin_attempts + 1}/{max_refetch_relogin_attempts}) for {len(initial_null_ids)} initial NULL IDs.")

                        if not authenticated_cookies:
                            print("Cookies invalid. Attempting re-login for NULL re-fetching phase.")
                            username = input("Username (Email): ")
                            password = getpass.getpass("Password: ")
                            authenticated_cookies = login_and_get_cookies(username, password)
                            if not authenticated_cookies:
                                print("Re-login failed for NULL re-fetching phase. Trying again.")
                                refetch_relogin_attempts += 1
                                time.sleep(1)
                                continue
                            else:
                                save_cookies(authenticated_cookies, COOKIE_FILE)
                                print("Re-login successful. Resuming NULL re-fetching.")


                        (processed, success, failed, auth_errs) = process_ids_with_pool(
                            initial_null_ids, pool, authenticated_cookies, last_id_in_db_at_start, description="Re-fetching NULL IDs"
                        )

                        if auth_errs:
                             print("\nAuthentication errors encountered during NULL re-fetching.")
                             authenticated_cookies = None
                             refetch_relogin_attempts += 1
                             time.sleep(1)
                             continue

                        initial_null_ids = []
                        print("\nAll initial NULL IDs attempted once (or max re-login attempts reached for this phase).")
                        break


                    if initial_null_ids:
                         print(f"\nWarning: {len(initial_null_ids)} initial NULL IDs could not be attempted after multiple re-login attempts.")
                    else:
                         print("\nPhase 1 (NULL re-fetching) completed.")

                else:
                    print("Skipping attempt to fetch all-NULL rows.")
            else:
                print("No rows found with all specified data columns as NULL.")

            print("\n--- Phase 2: Continuing Scraping New IDs ---")
            start_scraping_id = get_last_id_from_db() + 1

            print(f"Current highest ID in database: {start_scraping_id - 1}")
            print(f"Starting scraping for new IDs from: {start_scraping_id}")

            if input("Do you want to continue scraping new data? (y/n): ").strip().lower() != 'y':
                print("Exiting script as requested.")
                sys.exit(0)

            if platform.system() == 'Windows':
                os.system('cls')
            else:
                os.system('clear')

            continuous_relogin_attempts = 0
            max_continuous_relogin_attempts = 5
            current_batch_start_id = start_scraping_id

            while continuous_relogin_attempts < max_continuous_relogin_attempts:
                print(f"\nContinuous Scraping (Attempt {continuous_relogin_attempts + 1}/{max_continuous_relogin_attempts}) starting from ID {current_batch_start_id}")

                if not authenticated_cookies:
                     print("Cookies invalid. Attempting re-login for continuous scraping.")
                     username = input("Username (Email): ")
                     password = getpass.getpass("Password: ")
                     authenticated_cookies = login_and_get_cookies(username, password)
                     if not authenticated_cookies:
                         print("Re-login failed for continuous scraping. Trying again.")
                         continuous_relogin_attempts += 1
                         time.sleep(1)
                         continue
                     else:
                         save_cookies(authenticated_cookies, COOKIE_FILE)
                         print("Re-login successful. Resuming continuous scraping from ID", current_batch_start_id)

                while True:
                    batch_ids = list(range(current_batch_start_id, current_batch_start_id + pool_processes))
                    if not batch_ids:
                         print("Generated an empty batch. Stopping continuous scraping.")
                         break

                    print(f"Fetching batch: {batch_ids[0]}-{batch_ids[-1]}")

                    (processed, success, failed, auth_errs) = process_ids_with_pool(
                        batch_ids, pool, authenticated_cookies, last_id_in_db_at_start, description="Scraping new IDs"
                    )

                    if auth_errs:
                        print("\nAuthentication errors encountered during scraping batch.")
                        authenticated_cookies = None
                        continuous_relogin_attempts += 1
                        time.sleep(1)
                        break

                    current_batch_start_id += len(batch_ids)

            print("\nMax re-login attempts reached for continuous scraping. Stopping process.")

    except Exception as e:
        print(f"\nAn unexpected critical error occurred in the main process: {e}")

    finally:
        endTime = time.time()
        print(f"\nScraping process ended.")
        print(f"Total time taken: {endTime - startTime:.2f} seconds")

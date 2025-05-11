import requests
import sqlite3
import multiprocessing
import os
import time
import json
import getpass
from bs4 import BeautifulSoup
import sys
import zipfile
import argparse
import shutil
import threading

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
        return cookies
    except FileNotFoundError:
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
                pass
            return None
    except requests.RequestException as e:
        print(f"An error occurred during login POST request: {e}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred during login: {e}")
        return None

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
            print(f"Database version mismatch or first run. Expected {DATABASE_VERSION}, found {current_version}. ")
            print(f"If you are downgrading the database version (e.g., from 1.1 or 1.2 to {DATABASE_VERSION}), you will lose existing data.")
            c.execute('DROP TABLE IF EXISTS data')
            conn.commit()
            c.execute(create_data_table_sql)
            conn.commit()
            c.execute('DELETE FROM db_version')
            c.execute('INSERT INTO db_version VALUES (?)', (DATABASE_VERSION,))
            conn.commit()
            print("Database table recreated due to version mismatch.")
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

def get_max_remote_id():
    url = 'https://browser.geekbench.com/v5/cpu/'
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        first_link = soup.find('a', href=lambda href: href and href.startswith('/v5/cpu/'))
        if first_link and 'href' in first_link.attrs:
            href = first_link['href']
            try:
                max_id = int(href.split('/')[-1])
                return max_id
            except ValueError:
                print(f"Could not parse ID from href: {href}")
                return None
    except requests.RequestException as e:
        print(f"Error fetching max remote ID page: {e}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred while parsing max remote ID page: {e}")
        return None

def cleanup_null_rows_from_top():
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute('SELECT MAX(id) FROM data')
        highest_db_id = c.fetchone()[0]
        if highest_db_id is None or highest_db_id == 0:
            return
        all_null_ids = set(find_all_null_rows_ids())
        if highest_db_id not in all_null_ids:
            return
        print(f"Highest database ID ({highest_db_id}) is a NULL row. Starting top-down contiguous NULL row cleanup...")
        current_id = highest_db_id
        deleted_count = 0
        while current_id >= 1:
            if current_id in all_null_ids:
                c.execute('DELETE FROM data WHERE id = ?', (current_id,))
                deleted_count += 1
            else:
                break
            current_id -= 1
        if deleted_count > 0:
            conn.commit()
            print(f"Contiguous NULL row cleanup finished. Deleted {deleted_count} rows.")
        else:
            print("No contiguous NULL rows found from the top after initial check.")
    except sqlite3.Error as e:
        print(f"Database error during NULL row cleanup: {e}")
    except Exception as e:
        print(f"An unexpected error occurred during NULL row cleanup: {e}")
    finally:
        if conn:
            conn.close()

def find_all_null_rows_ids():
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        where_clause_parts = []
        for col in DATA_COLUMNS:
            if col != 'id':
                 where_clause_parts.append(f'"{col}" IS NULL')
        if not where_clause_parts:
             print("No data columns specified to check for NULLs.")
             return []
        where_clause = ' AND '.join(where_clause_parts)
        sql = f'SELECT id FROM data WHERE {where_clause}'
        c.execute(sql)
        null_ids = [row[0] for row in c.fetchall()]
        print(f"Found {len(null_ids)} rows with all specified data columns as NULL.")
        return null_ids
    except sqlite3.Error as e:
        print(f"Database error finding all-NULL rows: {e}")
        return []
    finally:
        if conn:
            conn.close()

def validate_missing_ids():
    conn = None
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute('SELECT MAX(id) FROM data')
        max_id_in_db = c.fetchone()[0]
        if max_id_in_db is None or max_id_in_db == 0:
            return []
        print(f"Checking for missing IDs between 1 and {max_id_in_db}...")
        c.execute('SELECT id FROM data ORDER BY id')
        present_ids = [row[0] for row in c.fetchall()]
        present_ids_set = set(present_ids)
        all_expected_ids_set = set(range(1, max_id_in_db + 1))
        missing_ids_set = all_expected_ids_set - present_ids_set
        missing_ids = sorted(list(missing_ids_set))
        if missing_ids:
            print(f"\nFound {len(missing_ids)} missing IDs less than or equal to {max_id_in_db}:")
            print(",".join(map(str, missing_ids[:100])) + ("..." if len(missing_ids) > 100 else ""))
        return missing_ids
    except sqlite3.Error as e:
        print(f"Database error during validation: {e}")
        return []
    finally:
        if conn:
            conn.close()

def print_compress_progress(current_end_id, max_end_id, current_folder_range, bar_length=30):
    progress_ratio = (current_end_id / max_end_id) if max_end_id > 0 else 0
    progress_ratio = max(0.0, min(1.0, progress_ratio))
    filled_length = int(bar_length * progress_ratio)
    bar = '█' * filled_length + '-' * (bar_length - filled_length)
    sys.stdout.write(f'\rCompressing folder: [{bar}] ({current_folder_range} / {max_end_id}) ')
    sys.stdout.flush()

def finish_compress_progress(max_end_id):
    bar_length = 30
    bar = '█' * bar_length
    sys.stdout.write(f'\rCompressing folder: [{bar}] ({max_end_id} / {max_end_id}) Finished.\n')
    sys.stdout.flush()

def get_raw_data_subfolder(id, group_size=5000):
    start_id = ((id - 1) // group_size) * group_size + 1
    end_id = start_id + group_size - 1
    return os.path.join('raw_data_5', f'{start_id}-{end_id}')

def fetch_data(count, cookies):
    url = f'https://browser.geekbench.com/v5/cpu/{count}.gb5'
    subfolder_path = get_raw_data_subfolder(count, 5000)
    os.makedirs(subfolder_path, exist_ok=True)
    raw_file_path = os.path.join(subfolder_path, f'{count}.gb5')
    raw_text_data = None
    if os.path.exists(raw_file_path):
        try:
            with open(raw_file_path, 'r', encoding='utf-8') as f:
                raw_text_data = f.read()
        except Exception as e:
            print(f"\nError reading local file {raw_file_path}: {e}. Falling back to network.")
            raw_text_data = None
    conn = None
    try:
        conn = sqlite3.connect('geekbench_5_data.db')
        c = conn.cursor()
        data_entry = {'id': count}
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
                response = worker_session.get(url, headers=headers, timeout=20)
                response.raise_for_status()
                raw_text_data = response.text
                try:
                    with open(raw_file_path, 'w', encoding='utf-8') as f:
                        f.write(raw_text_data)
                except IOError as e:
                    print(f"\nError saving raw data for ID {count} to file {raw_file_path}: {e}")
            except requests.HTTPError as e:
                if e.response.status_code == 404:
                    c.execute("INSERT OR REPLACE INTO data (id) VALUES (?)", (count,))
                    conn.commit()
                    print(f"ID {count} returned 404, marked as checked in DB with NULL data.")
                    return '404'
                elif e.response.status_code in [401, 403]:
                     print(f"\nAuthentication/Authorization error for ID {count}.")
                     return 'auth_error'
                else:
                    print(f"\nOther HTTP error {e.response.status_code} for ID {count}.")
                    return 'other_error'
            except requests.Timeout:
                print(f"\nRequest timed out for ID {count}.")
                return 'other_error'
            except requests.RequestException as e:
                print(f"\nRequest Exception for ID {count}: {e}")
                return 'other_error'
            except Exception as e:
                print(f"\nAn unexpected error occurred for ID {count} during network fetch: {e}")
                return 'other_error'
        error_occured_during_parsing = False
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
                print(f"\nJSON Decode Error for ID {count}: {e}. Response text starts with: {raw_text_data[:500]}...")
                error_occured_during_parsing = True
            except Exception as e:
                print(f"\nAn unexpected error occurred for ID {count} during JSON parsing/data extraction: {e}")
                error_occured_during_parsing = True
            columns = ', '.join(data_entry.keys())
            placeholders = ', '.join('?' * len(data_entry))
            sql = f"INSERT OR REPLACE INTO data ({columns}) VALUES ({placeholders})"
            values = tuple(data_entry.values())
            try:
                c.execute(sql, values)
                conn.commit()
                if error_occured_during_parsing:
                    return 'other_error'
                else:
                    return 'success'
            except sqlite3.Error as e:
                print(f"\nDatabase error inserting/replacing data for ID {count}: {e}")
                conn.rollback()
                return 'other_error'
        else:
             try:
                 c.execute("INSERT OR IGNORE INTO data (id) VALUES (?)", (count,))
                 conn.commit()
             except sqlite3.Error as db_err:
                 print(f"\nDatabase error marking ID {count} as attempted (no data): {db_err}")
             return 'other_error'
    except sqlite3.Error as e:
        print(f"\nCritical Database connection error for ID {count} during fetch: {e}")
        return 'other_error'
    finally:
        if conn:
            conn.close()

def organize_loose_raw_files(data_dir='raw_data_5', group_size=5000):
    print(f"Starting organization of loose .gb5 files...")
    if not os.path.exists(data_dir):
        print(f"Raw data directory {data_dir} not found. Skipping organization.")
        return
    loose_files_list = []
    for entry in os.listdir(data_dir):
        entry_path = os.path.join(data_dir, entry)
        if os.path.isfile(entry_path) and entry.endswith('.gb5'):
             try:
                 int(entry[:-4])
                 loose_files_list.append(entry)
             except ValueError:
                 print(f"\nSkipping invalid loose filename (not an integer ID): {entry}")
                 continue
    total_loose_files = len(loose_files_list)
    if total_loose_files == 0:
        print("No loose .gbml files found directly in the base directory. Skipping organization.")
        return
    print(f"Found {total_loose_files} potentially loose .gb5 files.")
    for i, entry in enumerate(loose_files_list):
        entry_path = os.path.join(data_dir, entry)
        print_organize_progress(i, total_loose_files)
        try:
            if not os.path.exists(entry_path):
                 continue
            file_id_str = entry[:-4]
            try:
                file_id = int(file_id_str)
            except ValueError:
                 print(f"\nError: Invalid filename {entry} encountered during move loop.")
                 continue
            target_subfolder_path = get_raw_data_subfolder(file_id, group_size)
            target_file_path = os.path.join(target_subfolder_path, entry)
            os.makedirs(target_subfolder_path, exist_ok=True)
            shutil.move(entry_path, target_file_path)
        except OSError as e:
            print(f"\nError moving file {entry_path} to {target_subfolder_path}: {e}")
            continue
        except Exception as e:
            print(f"\nAn unexpected error occurred organizing {entry_path}: {e}")
            continue
    print_organize_progress(total_loose_files, total_loose_files)
    sys.stdout.write(" Finished\n")
    sys.stdout.flush()

def print_organize_progress(current, total):
    percent = f"{(current / total) * 100:.1f}" if total > 0 else "0.0"
    sys.stdout.write(f'\rOrganizing loose files: ({current}/{total}) {percent}% ')
    sys.stdout.flush()

def compress_raw_data(data_dir='raw_data_5', group_size=5000):
    print(f"Starting compression of raw data in {data_dir} based on database content...")
    if not os.path.exists(data_dir):
        print(f"Raw data directory {data_dir} not found. Skipping compression.")
        return
    max_id_in_db = get_last_id_from_db()
    if max_id_in_db is None or max_id_in_db == 0:
        return
    print(f"Highest ID found in database: {max_id_in_db}")
    compress_up_to_id = (max_id_in_db // group_size) * group_size
    if max_id_in_db > 0 and max_id_in_db % group_size == 0:
         compress_up_to_id = max_id_in_db
    all_subfolders = []
    for entry in os.listdir(data_dir):
        entry_path = os.path.join(data_dir, entry)
        if os.path.isdir(entry_path):
            try:
                start_id_str, end_id_str = entry.split('-')
                start_id = int(start_id_str)
                end_id = int(end_id_str)
                if start_id > 0 and end_id > start_id and end_id - start_id + 1 == group_size:
                     all_subfolders.append((start_id, end_id, entry_path, entry))
            except ValueError:
                continue
    folders_to_compress = sorted([
        (start_id, end_id, folder_path, folder_name)
        for start_id, end_id, folder_path, folder_name in all_subfolders
        if end_id <= compress_up_to_id
    ])
    if not folders_to_compress:
        print(f"No full {group_size}-ID folders to compress up to {compress_up_to_id}.")
        return
    max_compress_end_id_display = folders_to_compress[-1][1] if folders_to_compress else 0 # For progress bar
    print(f"Will compress {len(folders_to_compress)} folders with End ID up to: {max_compress_end_id_display}")
    compressed_folders_count = 0
    for start_id, end_id, folder_path, folder_name in folders_to_compress:
        zip_filename = os.path.join(data_dir, f'{start_id}-{end_id}.zip')
        print_compress_progress(end_id, max_compress_end_id_display, folder_name)
        try:
            if os.path.exists(zip_filename):
                 if os.path.exists(folder_path):
                     try:
                         shutil.rmtree(folder_path)
                         compressed_folders_count += 1
                     except OSError as e:
                         print(f"\nError deleting folder {os.path.basename(folder_path)} when zip existed: {e}")
                 continue
            if not os.path.exists(folder_path) or not os.listdir(folder_path):
                continue
            try:
                with zipfile.ZipFile(zip_filename, 'w', compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zipf:
                    files_added_to_zip = 0
                    for filename in os.listdir(folder_path):
                        if filename.endswith('.gb5'): # GB5 extension
                            file_path = os.path.join(folder_path, filename)
                            if os.path.exists(file_path):
                                zipf.write(file_path, filename)
                                files_added_to_zip += 1
                if files_added_to_zip > 0:
                    try:
                        shutil.rmtree(folder_path)
                        compressed_folders_count += 1
                    except OSError as e:
                         print(f"\nError deleting folder {os.path.basename(folder_path)} after compression: {e}")
            except KeyboardInterrupt:
                 print(f"\nCompression interrupted for folder {os.path.basename(folder_path)}. Cleaning up incomplete zip file...")
                 if os.path.exists(zip_filename):
                     try:
                         os.remove(zip_filename)
                         print(f"Deleted incomplete zip file: {os.path.basename(zip_filename)}")
                     except OSError as e:
                         print(f"Error deleting incomplete zip file {os.path.basename(zip_filename)}: {e}")
                 raise
        except Exception as e:
            print(f"\nError compressing folder {os.path.basename(folder_path)}: {e}. Original folder NOT deleted.")
            if os.path.exists(zip_filename):
                 try:
                     os.remove(zip_filename)
                     print(f"Deleted potentially incomplete zip file due to error: {os.path.basename(zip_filename)}")
                 except OSError as cleanup_e:
                     print(f"Error during cleanup of incomplete zip file {os.path.basename(zip_filename)}: {cleanup_e}")
    if folders_to_compress:
         finish_compress_progress(max_compress_end_id_display)
    print("Compression process finished.")
    print(f"Compressed and deleted {compressed_folders_count} folders in this run.")

spinner_chars = ['|', '/', '-', '\\']
spinner_index = 0

def spinner_task(stop_event, message):
    global spinner_index
    while not stop_event.is_set():
        current_spinner = spinner_chars[spinner_index % len(spinner_chars)]
        sys.stdout.write(f'\r{message}: {current_spinner} ')
        sys.stdout.flush()
        spinner_index += 1
        time.sleep(0.05)
    line_length = len(message) + 2 + 1 + 1
    sys.stdout.write('\r' + ' ' * line_length + '\r')
    sys.stdout.flush()

def execute_finite_phase(phase_ids_list, pool, cookies, phase_name):
    total_ids_for_phase = len(phase_ids_list)
    if not phase_ids_list:
        print(f"\nNo IDs for {phase_name}, skipping phase.")
        return (0, 0, 0, 0)
    print(f"\n--- {phase_name} ---")
    total_successful_fetches = 0
    total_failed_fetches = 0
    total_404_handled = 0
    auth_error_occurred = False
    batch_size = pool._processes * 2
    for i in range(0, total_ids_for_phase, batch_size):
        current_batch = phase_ids_list[i:i + batch_size]
        if not current_batch:
             continue
        batch_start_id = current_batch[0]
        batch_end_id = current_batch[-1]
        spinner_message = f"Fetching batch {batch_start_id}-{batch_end_id}"
        stop_spinner_event = threading.Event()
        spinner_thread = threading.Thread(target=spinner_task, args=(stop_spinner_event, spinner_message))
        spinner_thread.daemon = True
        spinner_thread.start()
        batch_results = []
        jobs = []
        for id in current_batch:
             jobs.append(pool.apply_async(fetch_data, args=(id, cookies)))
        for j, job in enumerate(jobs):
            current_id_in_batch = current_batch[j]
            try:
                result = job.get()
                batch_results.append((current_id_in_batch, result))
            except Exception as e:
                 print(f"\nError processing ID {current_id_in_batch} from pool: {e}")
                 batch_results.append((current_id_in_batch, 'other_error'))
        if spinner_thread and spinner_thread.is_alive():
            stop_spinner_event.set()
            spinner_thread.join()
        for id, result in batch_results:
             if result == 'success':
                 total_successful_fetches += 1
             elif result == '404':
                 total_404_handled += 1
             elif result == 'auth_error':
                 auth_error_occurred = True
                 break
             else:
                 total_failed_fetches += 1
        if auth_error_occurred:
            break
    processed_count = total_successful_fetches + total_failed_fetches + total_404_handled
    return (processed_count, total_successful_fetches, total_failed_fetches, total_404_handled)

def execute_continuous_scraping_phase(pool, cookies):
    phase_name = "Phase 2: Catch-up Scraping to Max Remote ID"
    print(f"\n--- {phase_name} ---")
    current_id_to_fetch = get_last_id_from_db() + 1
    max_remote_id = None
    print(f"Initial highest ID in database: {current_id_to_fetch - 1}")
    print("Getting max remote ID...")
    max_remote_id = get_max_remote_id()
    if max_remote_id is None:
         print("Failed to get max remote ID. Cannot proceed with Phase 2.")
         print(f"--- {phase_name} aborted ---")
         return current_id_to_fetch
    print(f"Max remote ID found: {max_remote_id}. Starting fetch from DB ID {current_id_to_fetch}.")
    batch_size = pool._processes * 2
    auth_error_occurred = False
    while current_id_to_fetch <= max_remote_id:
        if auth_error_occurred:
            print("Authentication error detected. Stopping Phase 2.")
            break
        ids_to_fetch_batch = list(range(current_id_to_fetch, min(current_id_to_fetch + batch_size, max_remote_id + 1)))
        if not ids_to_fetch_batch:
            break
        batch_start_id = ids_to_fetch_batch[0]
        batch_end_id = ids_to_fetch_batch[-1]
        spinner_message = f"Fetching batch {batch_start_id}-{batch_end_id}"
        stop_spinner_event = threading.Event()
        spinner_thread = threading.Thread(target=spinner_task, args=(stop_spinner_event, spinner_message))
        spinner_thread.daemon = True
        spinner_thread.start()
        batch_results = []
        jobs = []
        for id in ids_to_fetch_batch:
            jobs.append(pool.apply_async(fetch_data, args=(id, cookies)))
        batch_highest_processed_id = current_id_to_fetch - 1
        for j, job in enumerate(jobs):
            current_id_in_batch = ids_to_fetch_batch[j]
            try:
                result = job.get()
                batch_results.append((current_id_in_batch, result))
                if result in ['success', '404']:
                    batch_highest_processed_id = max(batch_highest_processed_id, current_id_in_batch)
                elif result == 'auth_error':
                    auth_error_occurred = True
            except Exception as e:
                print(f"\nError processing ID {current_id_in_batch} from pool: {e}")
                pass
        if spinner_thread and spinner_thread.is_alive():
            stop_spinner_event.set()
            spinner_thread.join()
        current_id_to_fetch = batch_highest_processed_id + 1
    if not auth_error_occurred:
        print(f"\nSuccessfully caught up to max remote ID ({max_remote_id}).")
    return current_id_to_fetch

def execute_sync_fetch_phase(pool, cookies):
    phase_name = "Phase 3: Sync Fetch"
    print(f"\n--- {phase_name} ---")
    sync_interval = 15
    auth_error_occurred = False
    while not auth_error_occurred:
        current_db_max_id = get_last_id_from_db()
        max_remote_id = None
        max_remote_id = get_max_remote_id()
        if max_remote_id is None:
            print("Failed to get max remote ID during sync. Waiting before next sync check.")
            time.sleep(sync_interval)
            continue
        ids_to_fetch_sync = list(range(current_db_max_id + 1, max_remote_id + 1))
        if ids_to_fetch_sync:
            sync_current_id = ids_to_fetch_sync[0]
            successful_sync_fetches = 0
            failed_sync_fetches = 0
            _404_sync_fetches = 0
            spinner_message = f"Sync Fetching (current DB max ID {current_db_max_id})"
            stop_spinner_event = threading.Event()
            spinner_thread = threading.Thread(target=spinner_task, args=(stop_spinner_event, spinner_message))
            spinner_thread.daemon = True
            spinner_thread.start()
            while sync_current_id <= max_remote_id:
                if sync_current_id > max_remote_id:
                    latest_max_remote_id = get_max_remote_id()
                    if latest_max_remote_id is not None and latest_max_remote_id > max_remote_id:
                         max_remote_id = latest_max_remote_id
                         continue
                    else:
                        break
                fetch_result = fetch_data(sync_current_id, cookies)
                if fetch_result == 'success':
                    successful_sync_fetches += 1
                    sync_current_id += 1
                    time.sleep(0.1)
                elif fetch_result == '404':
                    _404_sync_fetches += 1
                    sync_current_id += 1
                elif fetch_result == 'auth_error':
                    print(f"\nAuthentication error encountered during sync fetch for ID {sync_current_id}.")
                    auth_error_occurred = True
                    break
                elif fetch_result == 'other_error':
                    failed_sync_fetches += 1
                    sync_current_id += 1
            if spinner_thread and spinner_thread.is_alive():
                stop_spinner_event.set()
                spinner_thread.join()
        else:
            for i in range(sync_interval, 0, -1):
                current_db_max_id_waiting = get_last_id_from_db()
                sys.stdout.write(f'\rCurrent highest ID in database: {current_db_max_id_waiting} (Waiting {i} seconds for next sync) ')
                sys.stdout.flush()
                time.sleep(1)
        sys.stdout.write('\r' + ' ' * 80 + '\r')
        sys.stdout.flush()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Geekbench 5 Data Scraper Script.")
    parser.add_argument('-N', action='store_true', help='Run Phase N: Attempt to fetch data for rows with all NULL data.')
    parser.add_argument('-c', action='store_true', help='Run Cleaning: Raw data compression.')
    parser.add_argument('-s', '--specific-ids', type=str, help='Run Phase X: Fetch specific benchmark IDs (comma-separated).')
    parser.add_argument('-C', '--continuous', action='store_true', help='Run Continuous Scraping (Phase 2 then Phase 3). Default if -N and -s are NOT used.')
    parser.add_argument('-o', action='store_true', help='Run Organization: Organize loose raw .gb5 files into subfolders.')
    args = parser.parse_args()
    print("Geekbench 5 Data Scraper - Version 1.3")
    authenticated_cookies_ref = [None]
    try:
        if args.c:
            print("\n--- Cleaning: Compressing raw data ---")
            try:
                compress_raw_data(data_dir='raw_data_5', group_size=5000)
            except KeyboardInterrupt:
                print("\nCompression process interrupted by user, cleanup performed.")
                pass
        if args.o:
            print("\n--- Organizing Loose Raw Files ---")
            organize_loose_raw_files(data_dir='raw_data_5', group_size=5000)
        initialize_database()
        loaded_cookies = load_cookies(COOKIE_FILE)
        if loaded_cookies:
            authenticated_cookies_ref[0] = loaded_cookies
            print("Loaded cookies from file.")
        else:
            print("No valid saved cookies found. Attempting to log in.")
            initial_login_attempts = 0
            max_initial_login_attempts = 5
            while not authenticated_cookies_ref[0] and initial_login_attempts < max_initial_login_attempts:
                username = input(f"Attempt {initial_login_attempts + 1}/{max_initial_login_attempts} - Username (Email): ")
                password = getpass.getpass("Password: ")
                authenticated_cookies = login_and_get_cookies(username, password)
                if not authenticated_cookies:
                    print("Authentication failed.")
                    initial_login_attempts += 1
                else:
                    authenticated_cookies_ref[0] = authenticated_cookies
                    save_cookies(authenticated_cookies, COOKIE_FILE)
                    print("Authentication successful.")
                    break
            if not authenticated_cookies_ref[0]:
                print("\nFailed to authenticate after multiple attempts. Exiting script.")
                sys.exit(1)
        cleanup_null_rows_from_top()
        run_any_fetch_phase = args.N or args.specific_ids or args.continuous or (not args.N and not args.specific_ids)
        pool = None
        if run_any_fetch_phase:
            pool_processes = 6
            pool = multiprocessing.Pool(processes=pool_processes)
        print("\n--- Phase 1: Running Database Validation and Fetching Missing IDs ---")
        missing_ids_found = validate_missing_ids()
        if missing_ids_found:
             if authenticated_cookies_ref[0] and pool:
                  execute_finite_phase(
                      missing_ids_found, pool, authenticated_cookies_ref[0],
                      phase_name="Phase 1: Fetching Missing IDs"
                  )
             elif not authenticated_cookies_ref[0]:
                  print("Authentication required for Phase 1. Skipping.")
             else:
                  print("Multiprocessing pool not initialized. Skipping Phase 1.")
        else:
            print("\nNo missing IDs found by validation.")
        specific_ids_to_fetch = []
        if args.specific_ids:
            print("\n--- Phase X: Fetching Specific IDs ---")
            specific_ids_str_list = [id_str.strip() for id_str in args.specific_ids.split(',')]
            invalid_inputs = []
            for id_str in specific_ids_str_list:
                if id_str:
                    try:
                        specific_ids_to_fetch.append(int(id_str))
                    except ValueError:
                        invalid_inputs.append(id_str)
            if invalid_inputs:
                print(f"Warning: Skipping invalid specific ID inputs: {', '.join(invalid_inputs)}")
            specific_ids_to_fetch = sorted(list(set(specific_ids_to_fetch)))
            if specific_ids_to_fetch:
                if authenticated_cookies_ref[0] and pool:
                     execute_finite_phase(
                         specific_ids_to_fetch, pool, authenticated_cookies_ref[0],
                         phase_name="Phase X: Fetching Specific IDs"
                     )
                elif not authenticated_cookies_ref[0]:
                     print("Authentication required for Phase X. Skipping.")
                else:
                     print("Multiprocessing pool not initialized. Skipping Phase X.")
            else:
                print("\nNo valid specific IDs provided for Phase X.")
        if args.N:
            print("\n--- Phase N: Finding and Fetching Rows with All NULL Data ---")
            ids_to_refetch_nulls = find_all_null_rows_ids()

            if ids_to_refetch_nulls:
                if authenticated_cookies_ref[0] and pool:
                     execute_finite_phase(
                         ids_to_refetch_nulls, pool, authenticated_cookies_ref[0],
                         phase_name="Phase N: Fetching All-NULL Rows"
                     )
                elif not authenticated_cookies_ref[0]:
                     print("Authentication required for Phase N. Skipping.")
                else:
                     print("Multiprocessing pool not initialized. Skipping Phase N.")
            else:
                print("\nNo rows found with all specified data columns as NULL, skipping Phase N fetching.")
        run_continuous_process = args.continuous or (not args.N and not args.specific_ids)
        if run_continuous_process:
             if authenticated_cookies_ref[0] and pool:
                 caught_up_id = execute_continuous_scraping_phase(
                     pool, authenticated_cookies_ref[0]
                 )
                 execute_sync_fetch_phase(
                     pool, authenticated_cookies_ref[0]
                 )
             elif not authenticated_cookies_ref[0]:
                 print("\nAuthentication required for Continuous/Sync Scraping (Phase 2 & 3). Skipping.")
             else:
                  print("\nMultiprocessing pool not initialized. Skipping Continuous/Sync Scraping (Phase 2 & 3).")
    except KeyboardInterrupt:
         print("\nCtrl+C detected. Shutting down...")
         if pool:
             print("Terminating worker processes...")
             pool.terminate()
             pool.join()
             print("Worker processes terminated.")
         pass
    except Exception as e:
        print(f"\nAn unexpected critical error occurred in the main process: {e}")
        if pool:
             print("Terminating worker processes due to main process error...")
             pool.terminate()
             pool.join()
             print("Worker processes terminated.")

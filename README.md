# Geekbench Data Scraper

[中文](README-zh.md)

This is a Python script designed to scrape Geekbench 5/6/AI benchmark result data from the Geekbench Browser website and store it in a local SQLite database. It supports resuming from where it left off, fetching missing IDs, fetching specific IDs, fetching rows with all NULL data, and organizing/compressing raw data files.

**Note:** This script requires a Geekbench Browser account to log in and fetch data.

## Features

* **Login and Session Management:** Logs into Geekbench Browser using provided credentials and saves/loads session cookies for authentication.
* **Database Integration:** Stores scraped benchmark data in a local SQLite database.
* **Database Initialization and Versioning:** Automatically initializes the database table and includes a basic versioning mechanism (note: the data table will be recreated if the version doesn't match).
* **NULL Row Cleanup:** Added a cleanup step to remove contiguous rows from the highest ID downwards that consist entirely of NULL values. This helps in cleaning up entries corresponding to 404s or incomplete fetches at the end of the data range.
* **Resume Capability:** Can resume fetching new benchmark results from the highest ID already present in the database.
* **Missing ID Validation and Fetching (Phase 1):** Checks for gaps in IDs within the database (missing IDs less than or equal to the current highest ID) and attempts to fetch data for these missing IDs.
* **Fetch All NULL Data Rows (Phase N):** Identifies rows in the database where all specified data columns are NULL and attempts to refetch the data for these IDs.
* **Fetch Specific IDs (Phase X):** Allows the user to specify one or more specific benchmark IDs to fetch via a command-line argument.
* **Continuous Catch-up Scraping (Phase 2):** Starts fetching new benchmark results from the ID immediately following the highest ID in the database and continues scraping up to the currently available maximum ID on the browser.
* **Continuous Sync Fetch (Phase 3):** When running in continuous mode, after fetching historical data up to the current maximum, the script transitions to a synchronization phase. It periodically checks the Geekbench Browser for newly added benchmark results and fetches them to keep the database up-to-date.
* **Raw Data File Saving:** Saves the raw file for each benchmark result locally.
* **Raw Data Organization:** Organizes scattered raw files from the main directory into subfolders grouped by ID range (e.g., `raw_data_5/1-5000`).
* **Raw Data Compression:** Compresses subfolders of organized raw data files into `.zip` archives and deletes the original folders to save space.
* **Multiprocess Fetching:** Utilizes multiprocessing to fetch data concurrently, improving efficiency.
* **Enhanced Authentication Handling:** Improved logic for detecting and responding to authentication errors that may occur during data fetching phases, facilitating re-authentication.
* **Progress Indicator:** Displays a simple progress indicator during fetching batches, file organization, and compression.

## Requirements

* Python 3.x
* `requests` library
* `beautifulsoup4` library
* Standard libraries (`sqlite3`, `multiprocessing`, `os`, `time`, `json`, `getpass`, `sys`, `zipfile`, `argparse`, `shutil`, `threading`)

You can install the dependencies using pip:

```bash
pip install requests beautifulsoup4
```

## Usage

Running the script requires specifying its operational mode via command-line arguments.

```bash
python gb5.py [options]
```

### Command-line Options

* `-N`
    * Run Phase N: Attempt to fetch data for rows in the database where all specified data columns are NULL.
* `-c`
    * Run Cleaning: Compress raw data subfolders into `.zip` files.
* `-s <ids>`
    * Run Phase X: Fetch specific benchmark IDs. `<ids>` should be one or more IDs separated by commas (e.g., `-s 100,101,105`).
* `-C`
    * Run Phase 2: Continue scraping new benchmark data. This is the default behavior if neither `-N` nor `-s` is used.
* `-o`
    * Run Organization: Organize loose raw files from the `raw_data` main directory into subfolders grouped by ID range.

### Running Mode Examples

* **Default Mode (Continuous Scraping of New Data)：** If neither `-N` nor `-s` is specified, the script will run Phase 1 (Validate Missing IDs) and Phase 2 (Continuous Scraping of New Data).
    ```bash
    python gb5.py
    ```
* **Fetch only Missing and All NULL Data Rows (No Continuous Scraping)：**
    ```bash
    python gb5.py -N
    ```
    This will run Phase 1 (Validate Missing IDs) and Phase N (Fetch All-NULL Rows), then exit.
* **Fetch Specific IDs only：**
    ```bash
    python gb5.py -s 12345,67890
    ```
    This will run Phase 1 (Validate Missing IDs) and Phase X (Fetch Specific IDs), then exit.
* **Fetch Missing IDs, All NULL Data Rows, and then Continuous Scraping：**
    ```bash
    python gb5.py -N -C
    ```
    This will run Phase 1, Phase N, and then enter Phase 2.
* **Organize and Compress Raw Data Files：**
    ```bash
    python gb5.py -o -c
    ```
    This will first organize the raw files and then compress the completed ID range folders. These operations can be run while scraping or independently.

### Authentication

When the script runs for the first time, it will prompt for your Geekbench Browser account username (email) and password. Upon successful login, the cookies will be saved to a file named `geekbench_cookies.json` for use in subsequent runs.

## File Structure

The script will create the following files and folders when run:

* `geekbench_x_data.db`： The SQLite database file storing the scraped benchmark data.
* `geekbench_cookies.json`： The file storing the login session cookies.
* `raw_data_x/`： The root directory for storing raw files.
    * `raw_data_x/<start_id>-<end_id>/`: Subfolders for organized raw data, e.g., `raw_data_5/1-5000/`.
    * `raw_data_x/<start_id>-<end_id>.zip`: Compressed raw data files, e.g., `raw_data_5/1-5000.zip`.

## Important Notes

* The database versioning is currently basic: if the `DATABASE_VERSION` in the code does not match the version in the database, the `data` table will be **dropped and recreated**, resulting in the loss of existing data. Exercise caution if modifying `DATABASE_VERSION`.
* Scraping speed depends on your internet connection, the responsiveness of the Geekbench website, and the number of multiprocessing processes set.
* Setting a very high number of concurrent processes might lead to blocking or authentication errors from the Geekbench website.
* Organizing and compressing raw data is optional but highly recommended for managing a large number of raw files.

## License

This project is licensed under the terms of the GNU Affero General Public License v3.0 (AGPL-3.0).

See the [LICENSE](LICENSE) file for the full license details.

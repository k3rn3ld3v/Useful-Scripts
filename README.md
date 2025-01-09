# Useful-Script

A collection of scripts to automate and simplify common tasks.

## Current Script: IIS-Log2CSV.py

A Python script to parse IIS logs and export them as CSV or Excel files.

### Features

- Supports `.csv` and `.xlsx` formats.
- Handles large files by processing in chunks.
- Automatically splits large datasets across multiple Excel sheets.
- Allows batch processing of files in a folder.

## Installation

1. Clone the repository:

    ```sh
    git clone https://github.com/your-username/Useful-Script.git
    cd Useful-Script
    ```

2. Install dependencies:

    ```sh
    pip install -r requirements.txt
    ```

## Usage

### Convert IIS Logs to CSV/Excel

#### Command:

```sh
python IIS-Log2CSV.py --file <path-to-log-file> --output <output-file> --format <csv|xlsx>
```

#### Batch Process a Folder:

```sh
python IIS-Log2CSV.py --folder <path-to-folder> --output-folder <output-folder> --format <csv|xlsx> [--recurse]
```

### Examples:

- Convert a single log file to CSV:

    ```sh
    python IIS-Log2CSV.py --file logs/example.log --output output/example.csv --format csv
    ```

- Convert all logs in a folder to Excel:

    ```sh
    python IIS-Log2CSV.py --folder logs/ --output-folder output/ --format xlsx --recurse
    ```
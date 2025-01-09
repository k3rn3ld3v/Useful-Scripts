import argparse
import logging
import pandas as pd
from pathlib import Path
from time import time
from concurrent.futures import ProcessPoolExecutor

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("log_parser.log", mode="w"),
    ],
)

def validate_log_data(lines, source_file):
    """
    Validate IIS log data: check for headers and consistent rows.
    """
    header_line = next((line for line in reversed(lines) if line.startswith("#Fields")), None)
    if not header_line:
        raise ValueError(f"No #Fields line found in log file: {source_file}")

    headers = header_line.strip().split(" ")[1:]
    log_lines = [line.strip() for line in lines if not line.startswith("#") and line.strip()]

    if not log_lines:
        raise ValueError(f"No valid data lines found in log file: {source_file}")

    inconsistent_rows = [
        i + 1 for i, row in enumerate(log_lines) if len(row.split()) != len(headers)
    ]
    if inconsistent_rows:
        raise ValueError(
            f"Inconsistent rows found in log file {source_file}. Rows: {inconsistent_rows}"
        )

    return headers, log_lines

def convert_log_to_output(args):
    """
    Convert a single IIS log file to the specified output format (CSV/XLSX).
    """
    log_file, source_dir, destination_dir, output_format = args
    try:
        relative_path = log_file.relative_to(source_dir)
        destination_file = destination_dir / relative_path

        with open(log_file, "r", encoding="utf-8") as file:
            lines = file.readlines()

        headers, log_lines = validate_log_data(lines, log_file)
        df = pd.DataFrame([line.split() for line in log_lines], columns=headers)

        destination_file.parent.mkdir(parents=True, exist_ok=True)

        if output_format.lower() == "csv":
            df.to_csv(destination_file.with_suffix(".csv"), index=False)
        elif output_format.lower() == "xlsx":
            df.to_excel(destination_file.with_suffix(".xlsx"), index=False, engine="openpyxl")
        else:
            raise ValueError(f"Unsupported output format: {output_format}")

        logging.info(f"Converted: {log_file} -> {destination_file}")
    except FileNotFoundError:
        logging.error(f"File not found: {log_file}. Skipping...")
    except UnicodeDecodeError:
        logging.error(f"File {log_file} has an unsupported encoding. Skipping...")
    except Exception as e:
        logging.error(f"Failed to process {log_file}: {e}")

def process_log_files_parallel(log_files, source_dir, destination_dir, output_format):
    """
    Process log files in parallel using ProcessPoolExecutor.
    """
    args = [(log_file, source_dir, destination_dir, output_format) for log_file in log_files]

    with ProcessPoolExecutor() as executor:
        executor.map(convert_log_to_output, args)

def process_folder(source_dir, destination_dir, recurse, output_format):
    """
    Process all log files in a folder, maintaining the folder structure.
    """
    log_files = source_dir.rglob("*.log") if recurse else source_dir.glob("*.log")
    log_files = list(log_files)

    if not log_files:
        logging.warning(f"No log files found in: {source_dir}")
        return

    logging.info(f"Found {len(log_files)} log files in: {source_dir}")
    process_log_files_parallel(log_files, source_dir, destination_dir, output_format)

def main():
    parser = argparse.ArgumentParser(description="IIS Log Parser")
    group = parser.add_mutually_exclusive_group(required=True)

    group.add_argument("--file", type=str, help="Path to a single log file to process.")
    parser.add_argument("--output", type=str, help="Path to output file for single log processing.")

    group.add_argument("--folder", type=str, help="Path to a folder containing IIS logs.")
    parser.add_argument("--output-folder", type=str, help="Path to output folder for processed logs.")
    parser.add_argument("--recurse", action="store_true", help="Recursively find logs in subdirectories.")
    parser.add_argument("--format", type=str, default="csv", choices=["csv", "xlsx"], help="Output file format (default: csv).")

    args = parser.parse_args()

    start_time = time()

    if args.file:
        source_file = Path(args.file)
        destination_file = Path(args.output) if args.output else None

        if not source_file.exists():
            logging.error(f"Source file not found: {source_file}")
            return

        if not destination_file:
            logging.error("Output file path must be specified with --file.")
            return

        convert_log_to_output((source_file, source_file.parent, destination_file.parent, args.format))

    elif args.folder:
        source_dir = Path(args.folder)
        destination_dir = Path(args.output_folder) if args.output_folder else None

        if not source_dir.exists():
            logging.error(f"Source folder not found: {source_dir}")
            return

        if not destination_dir:
            logging.error("Output folder path must be specified with --folder.")
            return

        process_folder(source_dir, destination_dir, args.recurse, args.format)

    end_time = time()
    logging.info(f"Script completed in {end_time - start_time:.2f} seconds.")

if __name__ == "__main__":
    main()

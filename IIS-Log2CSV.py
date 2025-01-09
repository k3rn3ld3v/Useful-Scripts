import argparse
import logging
import pandas as pd
from pathlib import Path
from time import time
from concurrent.futures import ProcessPoolExecutor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("log_parser.log", mode="w"),
    ],
)

CHUNK_SIZE = 10_000
EXCEL_ROW_LIMIT = 1_048_576


def validate_log_data(file_path):
    """
    Validate IIS log data and extract headers and a generator for rows.
    """
    with open(file_path, "r", encoding="utf-8") as f:
        headers = None
        log_lines = []
        for line in f:
            if line.startswith("#Fields"):
                headers = line.strip().split(" ")[1:]
            elif not line.startswith("#") and line.strip():
                log_lines.append(line.strip())
        if not headers:
            raise ValueError(f"No #Fields line found in log file: {file_path}")
        if not log_lines:
            raise ValueError(f"No valid data lines found in log file: {file_path}")
    return headers, (line for line in log_lines)


def write_to_csv(destination_file, headers, log_line_generator):
    """
    Write log data to a CSV file in chunks.
    """
    with open(destination_file, "w", encoding="utf-8") as out_file:
        out_file.write(",".join(headers) + "\n")
        chunk = []
        for line in log_line_generator:
            chunk.append(",".join(line.split()))
            if len(chunk) >= CHUNK_SIZE:
                out_file.write("\n".join(chunk) + "\n")
                chunk.clear()
        if chunk:
            out_file.write("\n".join(chunk) + "\n")


def write_to_excel(destination_file, headers, log_line_generator):
    """
    Write log data to an Excel file in chunks, handling row limits.
    """
    with pd.ExcelWriter(destination_file, engine="openpyxl") as writer:
        sheet_number = 1
        current_row_count = 0
        chunk = []
        for line in log_line_generator:
            chunk.append(line.split())
            current_row_count += 1
            if len(chunk) >= CHUNK_SIZE:
                pd.DataFrame(chunk, columns=headers).to_excel(
                    writer, index=False, sheet_name=f"Sheet{sheet_number}"
                )
                chunk.clear()
            if current_row_count >= EXCEL_ROW_LIMIT:
                sheet_number += 1
                current_row_count = 0
        if chunk:
            pd.DataFrame(chunk, columns=headers).to_excel(
                writer, index=False, sheet_name=f"Sheet{sheet_number}"
            )


def convert_log_to_output(args):
    """
    Convert a single IIS log file to the specified output format.
    """
    log_file, source_dir, destination_dir, output_format = args
    try:
        relative_path = log_file.relative_to(source_dir)
        destination_file = destination_dir / relative_path
        destination_file = destination_file.with_suffix(f".{output_format.lower()}")
        destination_file.parent.mkdir(parents=True, exist_ok=True)

        headers, log_line_generator = validate_log_data(log_file)

        if output_format.lower() == "csv":
            write_to_csv(destination_file, headers, log_line_generator)
        elif output_format.lower() == "xlsx":
            write_to_excel(destination_file, headers, log_line_generator)
        else:
            raise ValueError(f"Unsupported output format: {output_format}")

        logging.info(f"Converted: {log_file} -> {destination_file}")
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

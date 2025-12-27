import gzip
import os


def check_raw_integrity(file_path, expected_cols=82):
    """
    Verifies the structural integrity of a gzipped CSV containing LOB data.

    This function ensures that the number of features matches the expected
    input shape of the RL environment. It checks for common issues like
    corrupted files or schema changes.

    Args:
        file_path: The absolute or relative path to the .csv.gz file.
        expected_cols: The total number of columns expected.
            Default is 82 (2 timestamps + 20 levels of Bid/Ask P&Q).

    Returns:
        None: Prints the status of the integrity check to the console.

    Raises:
        Exception: Catches and reports file read errors or decompression issues.
    """
    if not os.path.exists(file_path):
        print(f"Error: File {file_path} not found.")
        return

    try:
        with gzip.open(file_path, "rt") as f:
            header = next(f).strip().split(",")
            first_data_row = next(f).strip().split(",")

            print(f"--- Integrity Check: {os.path.basename(file_path)} ---")
            print(f"    Header Count: {len(header)}")
            print(
                f"    Data Col Count: {len(first_data_row)} (Expected {expected_cols})"
            )

            if len(first_data_row) != expected_cols:
                print(
                    f"Warning: Column mismatch! Found {len(first_data_row)}, expected {expected_cols}"
                )
            else:
                print("Column count matches expected structure.")

    except Exception as e:
        print(f"Critical Failure: {e}")


if __name__ == "__main__":
    SAMPLE_PATH = os.path.join(
        "data",
        "raw",
        "binance",
        "ws_depth20",
        "BTCUSDT",
        "2025-12-25",
        "BTCUSDT_depth20_2025-12-25_19.csv.gz",
    )
    check_raw_integrity(SAMPLE_PATH)

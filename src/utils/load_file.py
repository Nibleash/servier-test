import re
import json
import pandas as pd
import os


def explore_directory(directory_path):
    """
    Explore a directory and list paths to files along with their
    relative file formats.
    """
    file_paths = []

    try:
        with os.scandir(directory_path) as entries:
            for entry in entries:
                if entry.is_file():
                    file_path = entry.path
                    file_format = os.path.splitext(entry.name)[1][1:]
                    file_paths.append((file_path, file_format))
    except FileNotFoundError:
        print(f"Directory not found: {directory_path}")
    except PermissionError:
        print(f"Permission error accessing directory: {directory_path}")
    except Exception as e:
        print(f"Error exploring directory: {e}")

    return file_paths


def load_json(file_path: str):
    """Loads a JSON file into a DataFrame after removing trailing comma."""
    with open(file_path, 'r') as file:
        json_text = file.read()

    json_text = re.sub(r',(?=\s*])', '', json_text)
    json_data = json.loads(json_text)
    return pd.DataFrame(json_data)

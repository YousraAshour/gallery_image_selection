import json
import logging

def setup_logging():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def read_jsonl(file_path):
    """
    Reads a JSONL (JSON Lines) file and returns the data as a list of dictionaries.

    :param file_path: Path to the JSONL file.
    :return: List of dictionaries containing the data from the JSONL file.
    """
    logging.info(f"Reading JSONL file: {file_path}")
    try:
        with open(file_path, 'r') as file:
            data = [json.loads(line) for line in file if validate_json_schema(line) == True]
        logging.info(f"Successfully read {len(data)} records from {file_path}")
        return data
    except Exception as e:
        logging.error(f"Error reading JSONL file: {file_path} - {e}")
        raise

def read_json(file_path):
    """
    Reads a JSON file and returns the data as a dictionary.

    :param file_path: Path to the JSON file.
    :return: Dictionary containing the data from the JSON file.
    """
    logging.info(f"Reading JSON file: {file_path}")
    try:
        with open(file_path, 'r') as file:
            data = json.load(file)
        logging.info(f"Successfully read JSON file: {file_path}")
        return data
    except Exception as e:
        logging.error(f"Error reading JSON file: {file_path} - {e}")
        raise

def main():
    # Example usage
    jsonl_file = "data/images.jsonl"
    json_file = "schemas/image.json"

    # Reading JSONL file
    try:
        data_jsonl = read_jsonl(jsonl_file)
        logging.info(f"First record in JSONL: {data_jsonl[0]}")
    except Exception as e:
        logging.error(f"Failed to read JSONL file: {e}")

    # Reading JSON file
    try:
        data_json = read_json(json_file)
        logging.info(f"JSON data: {data_json}")
    except Exception as e:
        logging.error(f"Failed to read JSON file: {e}")

if __name__ == "__main__":
    setup_logging()
    main()

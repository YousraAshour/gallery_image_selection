import json
import jsonschema
import logging
from jsonschema import validate

from utils.read_json import read_json ,read_jsonl

def setup_logging():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def validate_jsonl(file_path, schema):
    """
    Validates a JSONL file against a JSON schema.

    :param file_path: Path to the JSONL file.
    :param schema: JSON schema to validate against.
    :return: True if validation is successful, otherwise raises a ValidationError.
    """
    logging.info(f"Validating JSONL file: {file_path}")
    data = read_jsonl(file_path)
    for i, record in enumerate(data):
        try:
            validate(instance=record, schema=schema)
            logging.info(f"Record {i+1} is valid.")
        except jsonschema.exceptions.ValidationError as e:
            logging.error(f"Validation error in record {i+1}: {e.message}")
            raise
    return True

def main():
    setup_logging()

    json_schema_file = "schemas/image_tags.json"
    jsonl_file = "data/image_tags.jsonl"

    # Load schema
    schema = read_json(json_schema_file)
    
    # Validate JSONL file against schema
    try:
        validate_jsonl(jsonl_file, schema)
        logging.info("All records in JSONL file are valid according to the schema.")
    except jsonschema.exceptions.ValidationError:
        logging.error("Validation failed.")
    except Exception as e:
        logging.error(f"An error occurred: {e}")

if __name__ == "__main__":
    main()

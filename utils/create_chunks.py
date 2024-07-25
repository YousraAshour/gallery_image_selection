import os
import json
import logging

def setup_logging():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def chunk_json_file(input_file_path, output_directory, num_chunks):
    """
    Splits a JSON file into chunks and saves them in the specified directory.

    :param input_file_path: Path to the input JSON file.
    :param output_directory: Directory where chunks will be saved.
    :param num_chunks: Number of chunks to split the file into.
    """
    # Read the JSON file
    logging.info(f"Reading input file: {input_file_path}")
    with open(input_file_path, 'r') as file:
        data = list(file)

    # Ensure the output directory exists
    os.makedirs(output_directory, exist_ok=True)
    logging.info(f"Output directory created: {output_directory}")

    # Calculate the size of each chunk
    chunk_size = len(data) // num_chunks + (len(data) % num_chunks > 0)

    # Split the data into chunks
    chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]

    # Save each chunk to a separate file
    for i, chunk in enumerate(chunks):
        output_file_path = os.path.join(output_directory, f'chunk_{i + 1}.jsonl')
        with open(output_file_path, 'w') as file:
            file.writelines(chunk)
        logging.info(f'Chunk {i + 1} saved to {output_file_path}')


if __name__ == "__main__":
    input_file = "data/main_images.jsonl"
    # Example usage
    output_dir = input_file.replace(".jsonl", "_chunks")


    setup_logging()
    chunk_json_file(input_file  , output_dir , 3)
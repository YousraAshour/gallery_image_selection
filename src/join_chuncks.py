from pyspark.sql import SparkSession
import os
import logging

def setup_logging():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def join_with_directory(spark, file_path, directory_path, join_column):
    """
    Joins a given file with each file in a specified directory on a common column.

    :param file_path: Path to the input file.
    :param directory_path: Path to the directory containing files to join with.
    :param join_column: The column to join on.
    :return: DataFrame resulting from the joins.
    """
    # Read the input file into a DataFrame
    logging.info(f"Reading input file: {file_path}")
    input_df = spark.read.json(file_path)

    # Get a list of all JSON files in the directory
    files = [os.path.join(directory_path, f) for f in os.listdir(directory_path) if f.endswith('.jsonl')]

    if not files:
        logging.warning(f"No JSON files found in directory: {directory_path}")
        return input_df

    # Read each file in the directory and perform the join
    for file in files:
        logging.info(f"Joining with file: {file}")
        df_to_join = spark.read.json(file)
        input_df = input_df.join(df_to_join, join_column, "inner")

    return input_df

def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("JoinWithDirectory") \
        .getOrCreate()

    # Example usage
    logging.info("Starting the join process")
    result_df = join_with_directory(spark, 'data/images_chunks/chunk_1.json', 'data/image_tags_chunks/', 'image_id')

    # Read complete tags and images data
    df_tags_all = spark.read.json("data/image_tags.jsonl")
    df_images = spark.read.json("data/images.jsonl")

    # Perform join on complete datasets
    logging.info("Joining complete datasets")
    df_all = df_tags_all.join(df_images, ["image_id"], "inner")

    # Output counts for verification
    logging.info(f"Total counts - df_all: {df_all.count()}, df_tags_all: {df_tags_all.count()}, df_images: {df_images.count()}")
    logging.info(f"Joined chunk result count: {result_df.count()}")

    # Example chunk join
    df_image_chunk = spark.read.json("data/images_chunks/chunk_1.json")
    df_image_chunk_meta = df_image_chunk.join(df_tags_all, ["image_id"], "inner")
    logging.info(f"Image chunk meta count: {df_image_chunk_meta.count()}")

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    setup_logging()
    main()

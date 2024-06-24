"""
This file contains the main functionality of the ELT pipeline.
"""

import subprocess
import time
import logging
import configparser

# Create logger instance for this module
logger = logging.getLogger(__name__)


def read_config():
    """
    This function reads the configuration file and returns the configuration dictionary.

    Returns:
        dict: The configuration dictionary.
    """
    config = configparser.ConfigParser()
    config.read("config.ini")

    if not config.has_section("source_postgres"):
        raise ValueError("source_postgres section not found in config.ini")

    if not config.has_section("destination_postgres"):
        raise ValueError("destination_postgres section not found in config.ini")

    source_config = {
        "dbname": config.get("source_db", "dbname"),
        "user": config.get("source_db", "user"),
        "password": config.get("source_db", "password"),
        "host": config.get("source_db", "host"),
    }

    destination_config = {
        "dbname": config.get("destination_db", "dbname"),
        "user": config.get("destination_db", "user"),
        "password": config.get("destination_db", "password"),
        "host": config.get("destination_db", "host"),
    }

    return source_config, destination_config


def wait_for_postgres(host, max_retries=5, delay_seconds=5):
    """
    This function waits for a PostgreSQL server to be available.

    Args:
        host (str): The hostname or IP address of the PostgreSQL server.
        max_retries (int, optional): The maximum number of retries. Defaults to 5.
        delay_seconds (int, optional): The number of seconds to wait between retries. Defaults to 5.
    """

    retries = 0

    while retries < max_retries:
        try:
            result = subprocess.run(
                ["pg_isready", "-h", host], check=True, capture_output=True, text=True
            )
            if "accepting connections" in result.stdout:
                logger.info("PostgreSQL server is available")
                return True

        except subprocess.CalledProcessError as e:
            logger.error("PostgreSQL server is not available %s", e)
            retries += 1
            logger.info("Retrying in %s seconds...", delay_seconds)
            time.sleep(delay_seconds)

    logger.error("PostgreSQL server is not available after %s retries", max_retries)
    return False


def dump_data(source_config):
    """
    This function dumps the data from the source PostgreSQL server to a file.

    Args:
        source_config (dict): The configuration for the source PostgreSQL server.
    """

    # Create dump command
    dump_command = [
        "pg_dump",
        "-h",
        source_config["host"],
        "-u",
        source_config["user"],
        "-d",
        source_config["dbname"],
        "-f",
        "data_dump.sql",
        "-w",
    ]

    # Create env variable for password
    subprocess_env = dict(PGPASSWORD=source_config["password"])

    # Run dump command
    subprocess.run(dump_command, env=subprocess_env, check=True)


def load_data(destination_config):
    """
    This function loads the data from the dump file to the destination PostgreSQL server.

    Args:
        destination_config (dict): The configuration for the destination PostgreSQL server.
    """

    # Use psql to load the dumped SQL file into the destination database
    load_command = [
        "psql",
        "-h",
        destination_config["host"],
        "-U",
        destination_config["user"],
        "-d",
        destination_config["dbname"],
        "-a",
        "-f",
        "data_dump.sql",
    ]

    # Set the PGPASSWORD environment variable for the destination database
    subprocess_env = dict(PGPASSWORD=destination_config["password"])

    # Execute the load command
    subprocess.run(load_command, env=subprocess_env, check=True)


def run_elt_pipeline():
    """
    This function runs the ELT pipeline and waits for the PostgreSQL server to be available.
    """

    # Wait for PostgreSQL server to be available
    if not wait_for_postgres(host="source_postgres"):
        exit(1)

    # Wait for PostgreSQL server to be available
    if not wait_for_postgres(host="destination_postgres"):
        exit(1)

    # Run the ELT pipeline
    logger.info("Running ELT pipeline...")

    # Getting configurations
    source_config, destination_config = read_config()

    # Dump data
    try:
        dump_data(source_config)
    except Exception as e:
        logger.error("Failed to dump data: %s", e)
        exit(1)

    # Load data
    try:
        load_data(destination_config)
    except Exception as e:
        logger.error("Failed to load data: %s", e)
        exit(1)

    logger.info("ELT pipeline completed")

import json
import requests
import urllib3
from lxml import html
import re

import pandas as pd
from io import StringIO
import logging
import urllib3
import time
import random
import csv
import pathlib
import sqlite3
from . import get_topics, get_topic_page, extract_dimensions, get_request_configs
import os
from threading import Thread


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


save_folder_path = str(pathlib.Path(__file__).parent.resolve()).replace("\\", "/")

dimensions_path = f"{save_folder_path}/dimensions.json"
topics_path = f"{save_folder_path}/topics.json"

db_path = f"{save_folder_path}/requests.db"

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(message)s",
    filename=f"{save_folder_path}/scraper_log.txt",
    filemode="w",
)


# URLs
CATALOG_URL = "https://statistik.bra.se/solwebb/action/start?menykatalogid=1"
TOPIC_URL_TEMPLATE = (
    "https://statistik.bra.se/solwebb/action/anmalda/urval/urval?menyid={}"
)

PAYLOAD_URL = "https://statistik.bra.se/solwebb/action/anmalda/urval/vantapopup"
SEARCH_URL = "https://statistik.bra.se/solwebb/action/anmalda/urval/sok"
RESULT_URL = "https://statistik.bra.se/solwebb/action/anmalda/resultat/dbfil"


def init_db():
    """Initializes the database."""
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS Requests (
                request_id INTEGER PRIMARY KEY AUTOINCREMENT,
                topic_id INTEGER NOT NULL,
                payload TEXT NOT NULL, -- JSON
                status TEXT DEFAULT 'Pending' CHECK(status IN ('Pending', 'Done', 'Error')),
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS Responses (
                response_id INTEGER PRIMARY KEY AUTOINCREMENT,
                request_id INTEGER NOT NULL,
                response_text TEXT, -- CSV
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(request_id) REFERENCES Requests(request_id)
            )
            """
        )
        # table for storing failed requests
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS FailedRequests (
                request_id INTEGER PRIMARY KEY AUTOINCREMENT,
                info TEXT, -- Any additional info
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            """
        )


def bulk_insert_requests(payloads: list[dict], topic_id: int):
    """Inserts the requests into the database.
    :param payloads (list): The payloads.
    :param topic_id (int): The topic ID.
    """
    with sqlite3.connect(db_path) as conn:
        conn.executemany(
            """
            INSERT INTO Requests (topic_id, payload)
            VALUES (?, ?)
            """,
            [(topic_id, json.dumps(payload)) for payload in payloads],
        )


def get_pending_requests(topic_id: int = None) -> list[dict]:
    """Gets the pending requests from the database.
    :param topic_id (int): The topic ID. Optional.
    :returns (list): The pending requests.
    """
    with sqlite3.connect(db_path) as conn:
        if topic_id:
            return conn.execute(
                """
                SELECT payload
                FROM Requests
                WHERE topic_id = ? AND status = 'Pending'
                """,
                (topic_id,),
            ).fetchall()
        else:
            return conn.execute(
                """
                SELECT payload
                FROM Requests
                WHERE status = 'Pending'
                """
            ).fetchall()


def update_request_status(request_id: int, status: str):
    """Updates the status of the request.
    :param request_id (int): The request ID.
    :param status (str): The status.
    """
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            UPDATE Requests
            SET status = ?
            WHERE request_id = ?
            """,
            (status, request_id),
        )


def insert_response(request_id: int, response_text: str):
    """Inserts the response into the database.
    :param request_id (int): The request ID.
    :param response_text (str): The response data.
    """
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO Responses (request_id, response_text)
            VALUES (?, ?, ?)
            """,
            (request_id, response_text),
        )


def _reset_db():
    """Resets the database."""
    with sqlite3.connect(db_path) as conn:
        conn.execute("DROP TABLE IF EXISTS Requests")
        conn.execute("DROP TABLE IF EXISTS Responses")
    init_db()


def construct_payload(request_config: dict) -> dict:
    """Constructs the payload for a request. Either per 100k or total MUST be True.
    :param request_config (dict): The request configuration. Format:
    {
        "crime": ["id1", "id2", ...],
        "region": ["id1", "id2", ...],
        "period": ["id1", "id2", ...],
        "measure": ["measure1", "measure2"]
    }
    Note: The measure can only contain "total" and/or "antal_100k".
    :returns: The payload as a dictionary.
    """
    payload = {
        "brottstyp_id_string": "*".join(request_config["crime"]),
        "region_id_string": "*".join(request_config["region"]),
        "period_id_string": "*".join(request_config["period"]),
        "antal": 1 if "antal" in request_config["measure"] else 0,
        "antal_100k": 1 if "antal_100k" in request_config["measure"] else 0,
    }

    return payload


def generate_requests(
    topic_id: int, dimensions: dict[str, dict[str, list[str]]], row_limit: int = 10000
) -> list[dict]:
    """Generates the requests for the topic.
    :param topic_id (int): The topic ID.
    :param dimensions (dict): The dimensions.
    :returns (list): The requests.
    """
    variables = {
        "crime": list(dimensions["crime"].keys()),
        "region": list(dimensions["region"].keys()),
        "period": list(dimensions["period"].keys()),
        "measure": ["antal", "antal_100k"],
    }

    request_configs = get_request_configs(variables, row_limit)
    payloads = [construct_payload(config) for config in request_configs]
    bulk_insert_requests(payloads, topic_id)


def parse_db_response(response_text: str) -> str:
    """Parses the response from the database and returns a CSV string."""
    db_lines = response_text.split("\n")
    # remove, if present, trailing separators ";"
    db_lines = [re.sub(r";$", "", line) for line in db_lines]
    response_text = "\n".join(db_lines)
    df = pd.read_csv(StringIO(response_text), sep=";", encoding="utf-8")
    # drop duplicate rows
    df = df.drop_duplicates()
    # sort columns by Region, Brott, År, Period (in that order)
    df = df.sort_values(by=["Region", "Brott", "År", "Period"])
    return df.to_csv(index=False, sep=",", encoding="utf-8", lineterminator="\n")


def combine_and_deduplicate_csv(csv_strings, output_path):
    """Combines and deduplicates the CSV strings and writes the result to a file."""
    combined_rows = set()  # Use a set to automatically remove duplicates
    header = None

    for csv_string in csv_strings:
        # Work with CSV string directly
        csv_file = csv_string.splitlines()
        reader = csv.reader(csv_file)

        for i, row in enumerate(reader):
            if i == 0:
                # Assume the first row is the header and set it if not already done
                if header is None:
                    header = row
            else:
                # Check for missing values represented by '..'
                if ".." not in row:
                    # Convert the row to a tuple (which is hashable) to be able to add it to the set
                    combined_rows.add(tuple(row))

    # Now we write the combined, deduplicated rows to the output file
    with open(output_path, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(header)  # Write the header first
        for row in combined_rows:
            writer.writerow(row)


import requests


def is_valid_csv(response, separator=";"):
    # Check if the input is a valid requests.Response object and not empty
    if not response or not isinstance(response, requests.Response) or not response.text:
        return False

    # Use response.text to work with the actual content
    content = response.text
    lines = content.split("\n")

    # Ensure there are at least two lines to check
    if len(lines) < 2:
        return False

    # Check if the first two lines contain more than one separator and have equal numbers of separators
    separator_count_first_line = lines[0].count(separator)
    separator_count_second_line = lines[1].count(separator)

    if (
        separator_count_first_line > 1
        and separator_count_first_line == separator_count_second_line
    ):
        return True
    else:
        return False


def save_response_async(request_id: int, response: requests.Response):
    def save_response(request_id: int, response: requests.Response):
        sql_query = None
        query_params = None
        mark_as_done = False
        if not is_valid_csv(response):
            logging.error(f"Invalid CSV response for request {request_id}")
            # Save the failed request to the database
            sql_query = "INSERT INTO FailedRequests (request_id, info) VALUES (?, ?)"
            query_params = (request_id, "Invalid CSV response")
        else:
            response_text = response.text
            sql_query = (
                "REPLACE INTO Responses(request_id, response_text) VALUES (?, ?)"
            )
            query_params = (request_id, response_text)
            mark_as_done = True

        conn = sqlite3.connect(db_path)
        try:
            c = conn.cursor()
            c.execute(sql_query, query_params)
            if mark_as_done:
                c.execute(
                    "UPDATE Requests SET status = 'Done' WHERE request_id = ?",
                    (request_id,),
                )
            conn.commit()
        except Exception as e:
            logging.error(f"Error saving response: {e}")
            print(f"Error saving response: {e}")

        conn.close()

    thread = Thread(target=save_response, args=(request_id, response), daemon=False)
    thread.start()


def execute_and_save_requests(requests: list[dict], topic_id: int):
    """Executes and saves the requests for the topic.
    :param requests (list): The requests.
    :param topic_id (int): The topic ID.
    """
    done_count = 0
    total_count = len(requests)

    with requests.Session() as session:
        session.get(
            "https://statistik.bra.se/solwebb/action/start?menykatalogid=1",
            verify=False,
        )
        session.get(
            f"https://statistik.bra.se/solwebb/action/anmalda/urval/urval?menyid={topic_id}",
            verify=False,
        )
        for request in requests:
            logging.info(
                f"Executing request {done_count + 1}/{total_count} for topic {topic_id}"
            )
            payload = request["payload"]
            session.post(
                "https://statistik.bra.se/solwebb/action/anmalda/urval/vantapopup",
                data=payload,
                verify=False,
            )
            session.get(
                "https://statistik.bra.se/solwebb/action/anmalda/urval/sok",
                verify=False,
            )
            response_db = session.post(
                "https://statistik.bra.se/solwebb/action/anmalda/resultat/dbfil",
                verify=False,
            )
            save_response_async(request["request_id"], response_db)
            done_count += 1


class BraScraper:

    def __init__(
        self,
        row_limit: int = 10000,
    ):
        self.topics = self._load_topics()
        self.topic_ids = [topic["id"] for topic in self.topics]
        self.dimensions = self._load_dimensions()
        self.row_limit = row_limit
        init_db()

    def _load_topics(self) -> list[dict]:
        """Loads the topics from the JSON file.
        :returns (list): The topics.
        """
        if os.path.exists(topics_path):
            with open(topics_path, "r", encoding="utf-8") as file:
                return json.load(file)
        else:
            return get_topics()

    def _load_dimensions(self) -> dict:
        """Loads the dimensions from the JSON file.
        :returns (dict): The dimensions.
        """
        if os.path.exists(dimensions_path):
            with open(dimensions_path, "r", encoding="utf-8") as file:
                return json.load(file)
        else:
            return self._get_dimensions()

    def _get_dimensions(self) -> dict:
        """Gets the dimensions for each topic.
        :returns (dict): The dimensions for each topic.
        """
        dimensions = {}
        for topic in self.topics:
            dimensions[topic["id"]] = extract_dimensions(get_topic_page(topic["id"]))
        with open(dimensions_path, "w", encoding="utf-8") as file:
            json.dump(dimensions, file, ensure_ascii=False, indent=4)
        return dimensions

    def _populate_requests(self):
        """Populates the requests for the topic.
        :param topic_id (int): The topic ID.
        :param row_limit (int): The row limit for each request.
        """
        _reset_db()
        for topic_id in self.topic_ids:
            generate_requests(topic_id, self.dimensions[topic_id], self.row_limit)

    def scrape_all(self):
        """Retrieves, executes and saves pending requests for all topics.
        Assumes that the requests have been populated."""
        topic_id_to_requests = {
            topic_id: get_pending_requests(topic_id) for topic_id in self.topic_ids
        }
        for topic_id, requests in topic_id_to_requests.items():
            execute_and_save_requests(requests, topic_id)

    def resume_scrape(self):
        """Resumes the scraping of pending requests."""
        self.scrape_all()

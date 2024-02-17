import json
import requests
import pandas as pd
from io import StringIO
import re
import logging
import urllib3
from lxml import html
import time
import random
import csv

logging.basicConfig(level=logging.INFO)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def get_topics():
    """Get a list of all topics
    :returns (list): A list of Topic instances
    """
    topics = []
    response = requests.get(
        "https://statistik.bra.se/solwebb/action/start?menykatalogid=1", verify=False
    )
    tree = html.fromstring(response.content)
    links = tree.xpath("//li[@class='menySol']/a")

    for link in links:
        id = link.get("href").split("=")[-1]
        url = f"https://statistik.bra.se/solwebb/action/anmalda/urval/urval?menyid={id}"

        name = link.xpath("span[@class='menytext']")[0].text
        desc = link.xpath("../following-sibling::li[@class='menyText']")[0].text
        topic = {
            "id": id,
            "url": url,
            "name": name,
            "description": desc.strip(),
        }

        topics.append(topic)
    return sorted(topics, key=lambda x: int(x["id"]))


def fetch_topic_page(meny_id: str | int) -> str:
    """Fetches the page for a specific topic.
    :param meny_id (str): The menu id of the topic.
    :returns: The HTML page as a string.
    """
    with requests.Session() as session:
        session.get("https://statistik.bra.se/solwebb/action/", verify=False)
        session.get(
            f"https://statistik.bra.se/solwebb/action//start?menykatalogid=1",
            verify=False,
        )
        response = session.get(
            f"https://statistik.bra.se/solwebb/action/anmalda/urval/urval?menyid={meny_id}",
            verify=False,
        )
    return response.text


def get_dict_from_html_page(html_page: str, type: str):
    """Get the dictionary from the HTML page."""
    assert type in [
        "crime",
        "region",
        "period",
    ], f"Unknown type: {type}. Valid types: 'crime', 'region', 'period'."
    if type == "period":
        return {
            int(line.split("*")[0]): {
                "År": line.split("*")[3].split(",")[0],
                "Period": line.split("*")[1],
            }
            for line in re.findall('arrayPeriod\[\d+\]="(.+)"', html_page)
        }

    def _extract_lines(page: str, niva: str):
        """Extracts lines from the JavaScript variables in the page script."""
        if type == "crime":
            niva = niva.lower()
            pattern = "arrayNiva" + niva + '\[\d+\]="(.+)"'
        else:
            pattern = "arrayRegionNiva" + niva + '\[\d+\]="(.+)"'
        return re.findall(pattern, page)

    def _get_lines(page: str):
        lines_2 = _extract_lines(page, "Tva")
        return lines_2 if len(lines_2) > 0 else _extract_lines(page, "Ett")

    def _filter_lines(lines: list[str]) -> list[str]:
        """Filters the lines to remove duplicates and return a list."""
        return list({line.split("*")[0]: line for line in lines}.values())

    def _parse_lines(lines: list[str]) -> dict:
        """Parses the lines and returns a dictionary."""
        return {
            int(parts[0]): {
                "label": parts[1].replace("\\xA0", "").strip().replace(" totalt", ""),
                "parent": None if int(parts[0]) == int(parts[2]) else int(parts[2]),
            }
            for line in lines
            for parts in [line.split("*")]
        }

    raw_lines = _get_lines(html_page)
    filtered_lines = _filter_lines(raw_lines)
    return _parse_lines(filtered_lines)


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


def parse_int(value):
    try:
        return int(float(value.replace(" ", "").replace("\xa0", "")))
    except ValueError:
        return None


def combine_rows_with_missing_values(
    records, identifying_keys=["Region", "Brott", "År", "Period"]
):
    combined_records = {}

    # Iterate through each record
    for record in records:
        # Create a unique key based on the identifying fields
        key = tuple(record[id_key] for id_key in identifying_keys)

        # If the key is not in the combined_records, add it
        if key not in combined_records:
            combined_records[key] = record.copy()
        else:
            # For existing keys, update only the null values from the new record
            for k, v in record.items():
                if combined_records[key].get(k) is None and v is not None:
                    combined_records[key][k] = v

    return list(combined_records.values())


def parse_response_data(response_text, crime_dict, region_dict, period_dict):
    tree = html.fromstring(response_text)
    data = []

    for td in tree.xpath("//td[@class='resultatAntal']"):
        ids = td.get("headers").split(" ")

        if len(ids) == 1:
            # Empty rows
            continue

        period_id = int(ids[0])
        region_id = int(ids[3])
        crime_id = int(ids[2])
        value = parse_int(td.text)
        _measures = {"antal": "count", "antal_100": "per capita"}
        assert ids[-1] in _measures, "Unknown measure: {}".format(ids[-1])
        measure_id = _measures[ids[-1]]
        status = None

        measure_id_to_label = {
            "antal": "Antal",
            "count": "Antal",
            "antal_100": "/100 000 inv",
            "per capita": "/100 000 inv",
        }

        if td.text == "..":
            """In case value is missing, we store 'value' as None
            and 'status' as 'missing'
            """
            status = "missing"

        datapoint = {
            "Region": region_dict.get(region_id)["label"],
            "Brott": crime_dict.get(crime_id)["label"],
            "År": period_dict.get(period_id)["År"],
            "Period": period_dict.get(period_id)["Period"],
        }
        value_key = measure_id_to_label[measure_id]
        if status != "missing":
            if value_key == "Antal":
                datapoint["Antal"] = value
                datapoint["/100 000 inv"] = None
            else:
                datapoint["Antal"] = None
                datapoint["/100 000 inv"] = value

            data.append(datapoint)

    return combine_rows_with_missing_values(data)


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


def dicts_to_csv(data: list[dict]) -> str:
    """Converts a list of dictionaries to a CSV string."""
    try:
        df = pd.DataFrame(data)
        # sort columns by Region, Brott, År, Period (in that order)
        df = df.sort_values(by=["Region", "Brott", "År", "Period"])
        return df.to_csv(index=False, sep=",", encoding="utf-8", lineterminator="\n")
    except Exception as e:
        logging.error(f"Error: {e}. Data: \n{data}")
        raise e


def fetch_data(meny_id: int, payload: dict, fetch_db: bool = True):
    """Fetches the data for a specific topic and payload."""
    session = requests.session()
    session.get(
        "https://statistik.bra.se/solwebb/action/start?menykatalogid=1", verify=False
    )
    session.get(
        f"https://statistik.bra.se/solwebb/action/anmalda/urval/urval?menyid={meny_id}",
        verify=False,
    )

    session.post(
        "https://statistik.bra.se/solwebb/action/anmalda/urval/vantapopup",
        data=payload,
        verify=False,
    )
    session.get(
        "https://statistik.bra.se/solwebb/action/anmalda/urval/sok", verify=False
    )
    if fetch_db:
        response = session.post(
            "https://statistik.bra.se/solwebb/action/anmalda/resultat/dbfil",
            verify=False,
        )
    else:
        response = session.get(
            "https://statistik.bra.se/solwebb/action/anmalda/urval/soktabell",
            verify=False,
        )
    return response.text


def fetch_all_data(meny_id: int, payloads: list[dict], fetch_db: bool = True):
    """Fetches all the data for a specific topic and payloads."""
    session = requests.session()
    session.get(
        "https://statistik.bra.se/solwebb/action/start?menykatalogid=1", verify=False
    )
    session.get(
        f"https://statistik.bra.se/solwebb/action/anmalda/urval/urval?menyid={meny_id}",
        verify=False,
    )

    output_data = []
    total_count = len(payloads)
    done_count = 0

    if fetch_db:
        method = "db_file"
    else:
        method = "table"
    logging.info(f"Fetching {total_count} payloads using {method} method.")

    for payload in payloads:
        session.post(
            "https://statistik.bra.se/solwebb/action/anmalda/urval/vantapopup",
            data=payload,
            verify=False,
        )
        session.get(
            "https://statistik.bra.se/solwebb/action/anmalda/urval/sok", verify=False
        )
        if fetch_db:
            response_db = session.post(
                "https://statistik.bra.se/solwebb/action/anmalda/resultat/dbfil",
                verify=False,
            )
            output_data.append(response_db.text)
        else:
            response_data = session.get(
                "https://statistik.bra.se/solwebb/action/anmalda/urval/soktabell",
                verify=False,
            )
            output_data.append(response_data.text)

        done_count += 1
        logging.info(f"{done_count}/{total_count} ({method})")

    return output_data


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


def _compare_fetch_methods(
    # "C:\Users\Admin\Documents\statscraper\data\payloads_37.json",
    payloads_path: str = "c:/Users/Admin/Documents/statscraper/data/payloads_37.json",
    meny_id: int = 37,
    max_payloads: int = 50,
    log_file: str = "c:/Users/Admin/Documents/statscraper/data/fetch_comparison.log",
):
    """Compares the two fetch methods."""

    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        filemode="w",
    )
    with open(payloads_path, "r", encoding="utf-8") as file:
        payloads = json.load(file)

    # pick out max_payloads of random payloads
    test_payloads = random.sample(payloads, max_payloads)

    html_page = fetch_topic_page(meny_id)
    crime_dict = get_dict_from_html_page(html_page, "crime")
    region_dict = get_dict_from_html_page(html_page, "region")
    period_dict = get_dict_from_html_page(html_page, "period")

    test_result_dict = {
        "info": {
            "meny_id": meny_id,
            "max_payloads": max_payloads,
            "payloads_path": payloads_path,
            "log_file": log_file,
        },
        "table_method": {
            "time": None,
            "output_length": None,
            "seconds_per_payload": None,
        },
        "db_method": {
            "time": None,
            "output_length": None,
            "seconds_per_payload": None,
        },
    }
    db_output_path = "c:/Users/Admin/Documents/statscraper/data/db_output.csv"
    table_output_path = "c:/Users/Admin/Documents/statscraper/data/table_output.csv"

    time_start_db = time.time()
    output_list_db = fetch_all_data(meny_id, test_payloads, fetch_db=True)
    final_output_db = []
    for output in output_list_db:
        final_output_db.append(parse_db_response(output))
    # combine and deduplicate the CSVs
    combine_and_deduplicate_csv(final_output_db, db_output_path)
    time_end_db = time.time()

    logging.info(
        f"DB method took {time_end_db - time_start_db:.2f} seconds. ({len(output_list_db)}/{max_payloads} successful payloads)"
    )
    print(
        f"DB method took {time_end_db - time_start_db:.2f} seconds. ({len(output_list_db)}/{max_payloads} successful payloads)"
    )

    test_result_dict["db_method"]["time"] = time_end_db - time_start_db
    test_result_dict["db_method"]["output_length"] = len(output_list_db)
    test_result_dict["db_method"]["seconds_per_payload"] = round(
        (time_end_db - time_start_db) / max_payloads, 3
    )

    time_start_table = time.time()
    output_list_table_raw = fetch_all_data(meny_id, test_payloads, fetch_db=False)
    final_output_table = []
    for output in output_list_table_raw:
        output_dicts = parse_response_data(output, crime_dict, region_dict, period_dict)
        if len(output_dicts) == 0:
            logging.warning("Empty output from table method. Continuing.")
            continue
        parsed_csv_output = dicts_to_csv(output_dicts)
        final_output_table.append(parsed_csv_output)

    # combine and deduplicate the CSVs
    combine_and_deduplicate_csv(final_output_table, table_output_path)
    time_end_table = time.time()

    test_result_dict["table_method"]["time"] = time_end_table - time_start_table
    test_result_dict["table_method"]["output_length"] = len(output_list_table_raw)
    test_result_dict["table_method"]["seconds_per_payload"] = round(
        (time_end_table - time_start_table) / max_payloads, 3
    )

    logging.info(
        f"Table method took {time_end_table - time_start_table:.2f} seconds. ({len(output_list_table_raw)}/{max_payloads} successful payloads)"
    )
    print(
        f"Table method took {time_end_table - time_start_table:.2f} seconds. ({len(output_list_table_raw)}/{max_payloads} successful payloads)"
    )

    logging.info(f"Test result: {test_result_dict}")
    print(f"Test result: {json.dumps(test_result_dict, indent=4, ensure_ascii=False)}")

    with open(
        "c:/Users/Admin/Documents/statscraper/data/fetch_comparison.json",
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(test_result_dict, file, ensure_ascii=False, indent=4)

    # print the 5 first rows of first output csv in both lists
    db_lines = final_output_db[0].splitlines()[0:5]
    table_lines = final_output_table[0].splitlines()[0:5]
    print("\n\n")
    print("DB method:")
    print("\n".join(db_lines))
    print("\n\n")
    print("Table method:")
    print("\n".join(table_lines))


if __name__ == "__main__":
    _compare_fetch_methods()

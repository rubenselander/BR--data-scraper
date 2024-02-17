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
import pathlib


save_folder_path = str(pathlib.Path(__file__).parent.resolve()).replace("\\", "/")


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


def fetch_all_data(meny_id: int, payloads: list[dict]):
    """Fetches all the data for a specific topic and payloads."""
    output_data = []
    with requests.Session() as session:
        session.get(
            "https://statistik.bra.se/solwebb/action/start?menykatalogid=1",
            verify=False,
        )
        session.get(
            f"https://statistik.bra.se/solwebb/action/anmalda/urval/urval?menyid={meny_id}",
            verify=False,
        )
        for payload in payloads:
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
            output_data.append(response_db.text)
    return output_data


def _compare_fetch_methods(
    # "C:\Users\Admin\Documents\statscraper\data\payloads_37.json",
    payloads_path: str = "c:/Users/Admin/Documents/statscraper/data/payloads_37.json",
    meny_id: int = 37,
    max_payloads: int = 50,
):
    """Compares the two fetch methods."""

    with open(payloads_path, "r", encoding="utf-8") as file:
        payloads = json.load(file)

    # pick out max_payloads of random payloads
    test_payloads = random.sample(payloads, max_payloads)

    html_page = fetch_topic_page(meny_id)
    crime_dict = get_dict_from_html_page(html_page, "crime")
    region_dict = get_dict_from_html_page(html_page, "region")
    period_dict = get_dict_from_html_page(html_page, "period")

    db_output_path = "c:/Users/Admin/Documents/statscraper/data/db_output.csv"

    output_list_db = fetch_all_data(meny_id, test_payloads, fetch_db=True)
    final_output_db = []
    for output in output_list_db:
        final_output_db.append(parse_db_response(output))
    # combine and deduplicate the CSVs
    combine_and_deduplicate_csv(final_output_db, db_output_path)


# def get_all_crime_dimensions():

#     crime_lines_path = save_folder_path + "/crime_lines.json"

#     def _extract_crime_lines_for(page: str):
#         pattern1 = "arrayNiva" + "ett" + '\[\d+\]="(.+)"'
#         pattern2 = "arrayNiva" + "tva" + '\[\d+\]="(.+)"'
#         lines_1 = re.findall(pattern1, page)
#         lines_2 = re.findall(pattern2, page)
#         # combine and deduplicate the lines
#         return lines_1, lines_2

#     def _get_all_lines():
#         combined_lines_1 = set()
#         combined_lines_2 = set()
#         topics = get_topics()
#         pages = [fetch_topic_page(topic["id"]) for topic in topics]
#         for page in pages:
#             lines_1, lines_2 = _extract_crime_lines_for(page)
#             combined_lines_1.update(lines_1)
#             combined_lines_2.update(lines_2)

#         return list(combined_lines_1), list(combined_lines_2)

#     lines1, lines2 = _get_all_lines()

#     # print the length of the lines
#     print(f"Length of lines1: {len(lines1)}")
#     print(f"Length of lines2: {len(lines2)}")

#     # check if there are any duplicate ids in either list
#     found_ids1 = set()
#     found_ids2 = set()

#     dup1_count = 0
#     dup2_count = 0

#     for line in lines1:
#         id = line.split("*")[0]
#         if id in found_ids1:
#             dup1_count += 1
#         found_ids1.add(id)

#     for line in lines2:
#         id = line.split("*")[0]
#         if id in found_ids2:
#             dup2_count += 1
#         found_ids2.add(id)

#     # print length of found ids
#     print(f"Length of found ids in lines1: {len(found_ids1)}")
#     print(f"Length of found ids in lines2: {len(found_ids2)}")

#     print(f"Duplicate ids in lines1: {dup1_count}")
#     print(f"Duplicate ids in lines2: {dup2_count}")

#     # check if all ids are represented in lines2
#     ids_in_lines2 = [line.split("*")[0] for line in lines2]
#     ids_in_lines1 = [line.split("*")[0] for line in lines1]
#     # remove duplicates
#     ids_in_lines2 = set(ids_in_lines2)
#     ids_in_lines1 = set(ids_in_lines1)


#     print(f"Length of ids in lines1 not in lines2: {len(ids_in_1_not_in_2)}")
#     print(f"Length of ids in lines2 not in lines1: {len(ids_in_2_not_in_1)}")


def save_all_raw_dimension_lines():
    save_path = save_folder_path + "/raw_dimension_lines.json"

    combined_lines_dict = {
        "crime": set(),
        "region": set(),
        "period": set(),
    }

    def _extract_lines_for_type_niva(page: str, type: str, niva: str):
        if type == "crime":
            niva = niva.lower()
            pattern = "arrayNiva" + niva + '\[\d+\]="(.+)"'
        elif type == "region":
            pattern = "arrayRegionNiva" + niva + '\[\d+\]="(.+)"'
        elif type == "period":
            pattern = 'arrayPeriod\[\d+\]="(.+)"'

        return re.findall(pattern, page)

    def _get_all_type_lines(page: str, type: str):
        lines_2 = _extract_lines_for_type_niva(page, type, "Tva")
        return (
            lines_2
            if len(lines_2) > 0
            else _extract_lines_for_type_niva(page, type, "Ett")
        )

    def _get_all_lines(page: str):
        page_lines = {
            "crime": _get_all_type_lines(page, "crime"),
            "region": _get_all_type_lines(page, "region"),
            "period": _get_all_type_lines(page, "period"),
        }
        return page_lines

    topics = get_topics()
    topic_pages = [fetch_topic_page(topic["id"]) for topic in topics]
    for page in topic_pages:
        lines = _get_all_lines(page)
        for type, type_lines in lines.items():
            combined_lines_dict[type].update(type_lines)

    # convert sets to lists
    for type in ["crime", "region", "period"]:
        combined_lines_dict[type] = list(combined_lines_dict[type])
    with open(save_path, "w", encoding="utf-8") as file:
        json.dump(combined_lines_dict, file, ensure_ascii=False, indent=4)


# if __name__ == "__main__":
# get_all_crime_dimensions()
# save_all_raw_dimension_lines()
# _compare_fetch_methods()

import re

# urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# # URLs
# BASE_URL = "https://statistik.bra.se/solwebb/action/"
# CATALOG_URL = "https://statistik.bra.se/solwebb/action/start?menykatalogid=1"
# TOPIC_URL_TEMPLATE = (
#     "https://statistik.bra.se/solwebb/action/anmalda/urval/urval?menyid={}"
# )

# PAYLOAD_URL = "https://statistik.bra.se/solwebb/action/anmalda/urval/vantapopup"
# SEARCH_URL = "https://statistik.bra.se/solwebb/action/anmalda/urval/sok"
# RESULT_URL = "https://statistik.bra.se/solwebb/action/anmalda/resultat/dbfil"


# Regex patterns
dimension_to_type = {
    "Nivaett": "crime",
    "Nivatva": "crime",
    "RegionNivaEtt": "region",
    "RegionNivaTva": "region",
    "Period": "period",
}
dimension_regex = "array(" + "|".join(dimension_to_type.keys()) + ')\\[\\d+\\]="(.+)"'


def _extract_dimension_lines(html_content: str) -> dict[str, list[str]]:
    """Extracts the dimensions from the HTML content.
    :param html_content (str): The HTML content of the topic page.
    :returns (dict): A dictionary with the raw lines for each dimension.
    """
    raw_lines = {
        "crime": set(),
        "region": set(),
        "period": set(),
    }

    for match in re.findall(dimension_regex, html_content):
        type, raw_line = match
        raw_lines[dimension_to_type[type]].add(raw_line)

    return {type: list(lines) for type, lines in raw_lines.items()}


def _parse_dimension_lines(raw_lines: dict[str, list[str]]) -> dict[str, list[str]]:
    """Parses the raw lines for each dimension.
    :param raw_lines (dict): The raw lines for each dimension.
    :returns (dict): A dictionary with the parsed lines for each dimension.
    """
    clean_lines = {
        type: [line.replace("\\xA0", "").strip() for line in lines]
        for type, lines in raw_lines.items()
    }
    dimension_data = {
        "crime": {},
        "region": {},
        "period": {},
    }

    def _get_labels(parts: list[str]) -> list[str]:
        labels = [part for part in parts if not part.isdigit()]
        return list(set(labels)) if labels else []

    def _get_parent_id(parts: list[str], line_id: str) -> str | None:
        ids = [part for part in parts if part.isdigit() and part != line_id]
        return ids[0] if ids else None

    for type, lines in clean_lines.items():
        for line in lines:
            line_id = line.split("*")[0]
            line_parts = line.split("*")[1:]
            labels = _get_labels(line_parts)
            parent_id = (
                _get_parent_id(line_parts, line_id) if type != "period" else None
            )

            if line_id not in dimension_data[type]:
                dimension_data[type][line_id] = {
                    "id": line_id,
                    "labels": labels,
                }
                if type != "period":
                    dimension_data[type][line_id]["parent"] = parent_id
            else:
                current_labels = dimension_data[type][line_id]["labels"]
                new_labels = list(set(current_labels + labels))
                dimension_data[type][line_id]["labels"] = new_labels
                if parent_id:
                    dimension_data[type][line_id]["parent"] = parent_id

    return dimension_data


def extract_dimensions(html_content: str) -> dict[str, dict[str, list[str]]]:
    """Extracts the dimensions from the HTML content.
    :param html_content (str): The HTML content of the topic page.
    :returns (dict): A dictionary with the parsed lines for each dimension.
    """
    raw_lines = _extract_dimension_lines(html_content)
    return _parse_dimension_lines(raw_lines)

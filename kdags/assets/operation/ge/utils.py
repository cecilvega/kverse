import re
from datetime import datetime


def extract_header_info(data: str, columns: list) -> dict:
    """
    Extract header information using regex
    For instance Counter / Profile
    :param columns:
    :param data:
    :return:
    """

    def _parse_datetime(date_string):
        date_string = date_string[:19]
        if date_string.__len__() == 19:
            # Format: 2023-04-23 10:37:27.864
            format_code = "%Y-%m-%d %H:%M:%S"
        elif date_string.__len__() == 16:
            # Format: 27APR23 03:57:39
            format_code = "%d%b%y %H:%M:%S"
        else:
            raise ValueError(f"Invalid date format {date_string}")

        date = datetime.strptime(date_string, format_code)
        return date

    data = re.sub(r"<.*?>", "", data[:100]).lower().replace("'", "").replace("id", "#").replace("cust unit", "truck #")
    info = {}
    if "upload_datetime" in columns:
        # (.+) = match any char except newline one or more times
        header_upload_datetime = re.findall(r"upload date\s?\=?\:?\s?(.+)\n", data)

        header_upload_datetime = (
            _parse_datetime(header_upload_datetime[0].upper().strip(" ")) if header_upload_datetime else None
        )
        info["header_upload_datetime"] = header_upload_datetime
    if "equipment_name" in columns:
        equipment_name = re.findall(r"truck\s?#\s?\=?\:?\,?\s?\s?\s?(\w+)", data)
        equipment_name = equipment_name[0].upper().strip(" ") if equipment_name else None
        info["header_equipment_name"] = equipment_name

    if "equipment_model" in columns:
        model = re.findall(r"truck\s?model\:?\,?(.*)", data)
        model = model[0].upper().strip(" ") if model else None
        info["equipment_model"] = model

    if "equipment_serial" in columns:
        equipment_serial = re.findall(r"frame\s?sn\:?\s+?(\w+)", data)
        equipment_serial = equipment_serial[0].upper().strip(" ") if equipment_serial else None
        info["header_equipment_serial"] = equipment_serial
    return info


def extract_filename_info(file_path: str, columns: list = None) -> dict:
    """
    Extract filename information using regex
    :param columns:
    :param file_path:
    :return:
    """
    if columns is None:
        columns = ["file_path"]

    available_columns = ["equipment_serial", "file_path", "site_name", "cycle"]
    assert set(columns).issubset(available_columns), f"Columns to search must be in {available_columns}"
    info = {}
    if "equipment_serial" in columns:
        equipment_serial = re.search(r"(A\d{5})", file_path.upper())
        equipment_serial = equipment_serial if equipment_serial is None else equipment_serial.group()
        info["filename_equipment_serial"] = equipment_serial
    if "site_name" in columns:
        faenas = [
            "SPENCE",
            "ESCONDIDA",
        ]
        pattern_faenas = r"|".join(faenas).lower()
        site_name = re.search(
            pattern_faenas,
            file_path.lower(),
        )
        site_name = site_name if site_name is None else site_name.group().title()

        info["site_name"] = site_name

    if "cycle" in columns:
        cycle = re.search(r"haulcycle(\d)", file_path)
        cycle = "-1" if cycle is None else cycle.group()
        info["filename_cycle"] = cycle
    info["file_path"] = file_path
    return info

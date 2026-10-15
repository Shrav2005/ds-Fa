import json
import os
from pathlib import Path

CONFIG_FILE = Path(__file__).with_name("hospitals.json")


def load_hospital_config():
    """Read the hospital list from the JSON configuration file."""
    with open(CONFIG_FILE, "r", encoding="utf-8") as file:
        hospitals = json.load(file)

    if os.getenv("DOCKER_ENV") == "true":
        for hospital in hospitals:
            hospital_number = hospital["hospital_id"].replace("H", "").lstrip("0")
            hospital["host"] = f"hospital-{hospital_number}"

    return hospitals


def get_hospital_by_id(hospital_id):
    """Return the configuration of one hospital by its unique identifier."""
    for hospital in load_hospital_config():
        if hospital["hospital_id"] == hospital_id:
            return hospital
    return None

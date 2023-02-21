import pymongo

from pathlib import Path

MAIN_FOLDER = Path(__file__).parent.parent
DATA_FOLDER = MAIN_FOLDER / "data"

DB_NAME = "WasteData"
METADATA_COLL = "metadata"
DATA_COLL = "collection_data"

KEY = "key"
VAL = "val"

class DataError(Exception):
    pass

def get_client():
    return pymongo.MongoClient(
        "localhost:27018",
        username="user",
        password="user_password",
        authSource="WasteData",
    )


def get_db():
    return get_client()[DB_NAME]


def get_schema():
    return ["authority", "year", "quarter"]

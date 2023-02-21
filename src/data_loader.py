import csv
import json
import re
import time

from calendar import month_abbr

from src.utils import (
    get_db,
    get_schema,
    DATA_COLL,
    METADATA_COLL,
    DATA_FOLDER,
    KEY,
    VAL,
)

MAX_COL = 15


class DataError(Exception):
    pass


def get_year_and_quarter(period):
    year = period[-2:]

    for j in range(1, 5):
        if period == f"{month_abbr[3*j - 2]} {year} - {month_abbr[3*j]} {year}":
            return int(f"20{year}"), f"Q{j}"

    raise DataError("Cannot parse date field")


def extract_data(csv_file):
    def _convert_to_number_if_possible(x):
        if re.search("^\d+$", x):
            return int(x)
        elif re.search("^\d*\.\d+$", x) or re.search("^\d+\.\d*$", x):
            return float(x)
        elif x in ["", "-"]:
            return 0
        else:
            return x

    headers = {}
    data = []

    with open(csv_file, encoding="cp1252") as f:
        csv_reader = csv.reader(f)

        for idx, row in enumerate(csv_reader):
            if idx == 14:
                col_labels = row
            elif all([val == "" for val in row[2:]]):
                if row[0] != "":
                    headers[row[0]] = _convert_to_number_if_possible(row[1])
            else:
                record = {
                    col_labels[j]: _convert_to_number_if_possible(row[j])
                    for j in range(MAX_COL)
                    if j != 13
                    if row[j] != ""
                }
                data.append(record)

    questions = set([(row[col_labels[2]], row[col_labels[3]]) for row in data])
    with open(DATA_FOLDER / "questions.json", "w") as f:
        json.dump(sorted(questions), f, indent=2)

    rows_with_material_group = [row for row in data if col_labels[14] in row]
    row_vals = set([row[col_labels[5]] for row in rows_with_material_group])

    material_groups = {}

    for val in row_vals:
        material_groups[val] = list(
            {
                row[col_labels[14]]
                for row in rows_with_material_group
                if row[col_labels[5]] == val
            }
        )

    assert all([len(x) == 1 for x in material_groups.values()])
    # since all the lists have only 1 item, we can skip the list structure
    material_groups = {key: val[0] for key, val in material_groups.items()}

    return headers, material_groups, data, col_labels


def list_to_dict(records_list, col_labels):
    # check that all the rows have the same set of keys
    if not all([set(record) == set(records_list[0]) for record in records_list]):
        raise DataError(f"Inconsistent keys: {records_list}")

    # create appropriate nested structure depending on which keys are specified
    if set(records_list[0]) == {col_labels[x] for x in range(5, 13) if x != 6}:
        return {record[col_labels[5]]: record[col_labels[7]] for record in records_list}
    elif set(records_list[0]) == {col_labels[x] for x in range(6, 13) if x != 8}:
        row_ident_vals = {record[col_labels[10]] for record in records_list}
        ret = []
        for val in row_ident_vals:
            ret.append(
                {
                    record[col_labels[6]]: record[col_labels[7]]
                    for record in records_list
                    if record[col_labels[10]] == val
                }
            )
        return ret
    elif set(records_list[0]) == {col_labels[x] for x in range(5, 13)} or set(
        records_list[0]
    ) == {col_labels[x] for x in range(5, 13)}:
        row_vals = {record[col_labels[5]] for record in records_list}
        ret = {}

        for val in row_vals:
            ret[val] = {
                record[col_labels[6]]: record[col_labels[7]]
                for record in records_list
                if record[col_labels[5]] == val
            }
        return ret
    elif set(records_list[0]) == {col_labels[x] for x in range(4, 13)}:
        row_vals = {record[col_labels[5]] for record in records_list}
        ret = {}

        for val in row_vals:
            ret[val] = {}

            collate_vals = {
                record[col_labels[4]]
                for record in records_list
                if record[col_labels[5]] == val
            }
            for val2 in collate_vals:
                ret[val][val2] = {
                    record[col_labels[6]]: record[col_labels[7]]
                    for record in records_list
                    if record[col_labels[5]] == val and record[col_labels[4]] == val2
                }
            return ret
    else:
        breakpoint()
        raise DataError(f"Unexpected row format: {records_list}")


def preprocess_data(data, col_labels):
    records = {}

    # in the first pass we group all records belonging to the same authority, year and quarter
    for row in data:
        year, quarter = get_year_and_quarter(row[col_labels[1]])
        key = (row[col_labels[0]], year, quarter)

        if key not in records:
            records[key] = []

        record = {
            k: v for k, v in row.items() if k in [col_labels[x] for x in range(4, 13)]
        }
        records[key].append((row[col_labels[2]], record))

    records_nested = {}

    # in the second pass we organise each list by question number
    for key, val in records.items():
        if key not in records_nested:
            records_nested[key] = {}

        questions = {x[0] for x in val}
        for question_no in questions:
            rows = [row[1] for row in val if row[0] == question_no]
            records_nested[key][question_no] = list_to_dict(rows, col_labels)

    return records_nested


def add_to_database(db, records_nested):
    schema = get_schema()
    authorities = []

    for key, val in records_nested.items():
        if key[0] not in authorities:
            authorities.append(key[0])

        record = dict(zip(schema, key))
        record["questions"] = val
        db[DATA_COLL].insert_one(record)

    db[METADATA_COLL].insert_one({KEY: "authorities", VAL: authorities})


if __name__ == "__main__":
    t0 = time.time()
    db = get_db()

    if db[METADATA_COLL].count_documents({}) > 0:
        raise DataError("Database is not empty, will skip ingesting data")

    csv_file = DATA_FOLDER / "201213_england_wastedata.csv"

    headers, material_groups, data, col_labels = extract_data(csv_file)
    db[METADATA_COLL].insert_one({KEY: "headers", VAL: headers})
    db[METADATA_COLL].insert_one({KEY: "material_groups", VAL: material_groups})
    records_nested = preprocess_data(data, col_labels)
    add_to_database(db, records_nested)
    print("Data ingestion completed successfully.")
    print(f"Time elapsed: {int(time.time()-t0)} secs")

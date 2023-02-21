import json
import time

from src.utils import (
    get_db,
    get_schema,
    DATA_COLL,
    METADATA_COLL,
    KEY,
    VAL,
    DATA_FOLDER,
)


def get_record(customer, year, quarter, db):
    schema = get_schema()
    filt = dict(zip(schema, [customer, year, quarter]))

    assert (
        db[DATA_COLL].count_documents(filt) <= 1
    ), "Multiple documents found when at most 1 was expected"

    if db[DATA_COLL].count_documents(filt) == 1:
        return next(db[DATA_COLL].find(filt))
    else:
        return None


def get_value(x, fields):
    for field in fields:
        if field in x:
            return x[field]
    breakpoint()
    return 0


def get_total_tonnage(x):
    return get_value(x, ["Tonnage Input", "Tonnage input"])


def get_landfill_tonnage(x):
    return get_value(
        x,
        [
            "Tonnes of Incinerator Bottom Ash to landfill",
            "Tonnes rejected to Landfill",
            "Tonnes to Landfill",
            "Tonnes of reject to Landfill",
            "Tonnes of Char / Slag to Landfill",
        ],
    )


def _generate_report(customer, year, quarter, db):
    total = 0
    landfill = 0

    if (record := get_record(customer, year, quarter, db)) is None:
        return None
    else:
        relevant_questions = [
            x
            for x in record["questions"].keys()
            if x in [f"Q0{j}" for j in range(51, 66)]
        ]

        for question_no in relevant_questions:
            data = record["questions"][question_no].values()
            if question_no in ["Q051", "Q052", "Q053"]:
                total += sum([get_total_tonnage(x) for x in data])
                landfill += sum([get_total_tonnage(x) for x in data])
            else:
                total += sum([get_total_tonnage(x) for x in data])
                landfill += sum([get_landfill_tonnage(x) for x in data])

        report = {
            "total": total,
            "landfill": landfill,
            "num_relevant_questions": len(relevant_questions),
        }
        if total > 0:
            report["landfill_fraction"] = landfill / total

        return report


def generate_all_reports(db, output_file="all_reports.json"):
    t0 = time.time()

    reports = []
    stats = {}

    customers = db[METADATA_COLL].find_one({KEY: "authorities"})[VAL]

    for customer in customers:
        for year in [2012, 2013]:
            for quarter in [f"Q{j}" for j in range(1, 5)]:
                if (
                    report := _generate_report(customer, year, quarter, db)
                ) is not None:
                    report.update(
                        {"customer": customer, "year": year, "quarter": quarter}
                    )
                    reports.append(report)

                    try:
                        stats[report["num_relevant_questions"]] += 1
                    except KeyError:
                        stats[report["num_relevant_questions"]] = 1

    reports.append(stats)
    with open(DATA_FOLDER / output_file, "w") as f:
        json.dump(reports, f, indent=2)

    print("Generated all reports successfully")
    print(f"Total number of questionnaires: {sum(stats.values())}")
    thr = 4
    good_questionnaires = sum([val for key, val in stats.items() if key >= thr])
    print(
        f"Number of questionnaires with at least {thr} relevant fields: {good_questionnaires}"
    )
    print(f"Time elapsed: {int(time.time()-t0)} secs")


def generate_single_report(
    customer, year, quarter, db, output_file="single_report.json"
):
    t0 = time.time()

    if (report := _generate_report(customer, year, quarter, db)) is None:
        print("No data found")
    else:
        report.update({"customer": customer, "year": year, "quarter": quarter})

        with open(DATA_FOLDER / output_file, "w") as f:
            json.dump(report, f, indent=2)

        print("Generated single report successfully")
    print(f"Time elapsed: {int(time.time()-t0)} secs")


if __name__ == "__main__":
    db = get_db()
    generate_all_reports(db)
    generate_single_report("Birmingham City Council", 2012, "Q2", db)

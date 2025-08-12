import json
from tqdm import tqdm
import gzip
import os
from collections import defaultdict
from pprint import pp

# data paths
S3_FOLDER = "mock_s3"
OL_DATA = "data/openlibrary/2025-06-30/2025-06-30.txt.gz"

# data pipeline parameters
SAMPLE_SIZE = 1000000
WORK_ID_FIRST_N = 4
BATCH_SIZE = 10000

"""
OL Notes
- Some editions have "genres" as a field: can choose one that has it to aggregate to works
- Some editions have different subjects, depending on language maybe? work has complete list of subjects
- Work has description
"""


def _remove_folder(folder_path, recursed=False):
    """Recursively delete a folder."""
    names = os.listdir(folder_path)
    for name in names:
        path = folder_path + f"/{name}"
        if os.path.isfile(path):
            os.remove(path)
        else:
            _remove_folder(path, recursed=True)
            os.rmdir(path)

    if not recursed:
        os.rmdir(folder_path)


def _check_ids(raw_json) -> dict:
    """Extract matchable external IDs from edition, returning None if no IDs exist"""
    book = dict()

    # check if json has goodreads and amazon id
    external_ids = raw_json.get("identifiers")
    if external_ids:
        for id in ["goodreads", "amazon"]:
            id_val = external_ids.get(id)
            if id_val:
                book[id] = id_val[0]

    # check if json has isbn identifiers
    for isbn in ["isbn_10", "isbn_13"]:
        isbn_val = raw_json.get(isbn)
        if isbn_val:
            book[isbn] = isbn_val[0]

    # return json w/ ids if it has them, otherwise return None
    return book if book else None


def _parse_edition(line) -> tuple[str, dict]:
    """Parse a single line into edition key and data"""

    # preprocess line into dictionary
    line = line.decode("utf-8").split("\t")
    edition_key = line[1].split("/")[-1]
    raw_json = json.loads("".join(line[4:]))

    # only include English books
    languages = raw_json.get("languages")
    if not languages or languages[0].get("key") != "/languages/eng":
        return None, None

    # only include books that have works
    works = raw_json.get("works")
    if not works:
        return None, None

    # check if book has ids, otherwise return None
    book = _check_ids(raw_json)
    if not book:
        return None, None

    # add work id
    book["work_id"] = works[0]["key"].split("/")[-1]

    # check if all these fields are in them, include them if they exist
    fields = [
        "title",
        "subjects",
        "number_of_pages",
        "publish_date",
        "genres",
        "covers",
    ]
    for field in fields:
        value = raw_json.get(field)
        if value:
            book[field] = value

    return edition_key, book


def _save_batch(
    batch_works, batch_work_ids, batch_isbn10, batch_isbn13, batch_count, n
):
    """Save one batch's works (grouped into files by first n characters) and work ids"""
    # make temporary directories if they don't exist
    os.makedirs(f"{S3_FOLDER}/temp_batches", exist_ok=True)
    os.makedirs(f"{S3_FOLDER}/temp_batches/works", exist_ok=True)
    os.makedirs(f"{S3_FOLDER}/temp_batches/work_ids", exist_ok=True)
    os.makedirs(f"{S3_FOLDER}/temp_batches/isbn_10", exist_ok=True)
    os.makedirs(f"{S3_FOLDER}/temp_batches/isbn_13", exist_ok=True)

    # group works by first n characters of id
    works_n = defaultdict(dict)
    for work_id, work_data in batch_works.items():
        works_n[work_id[:n]][work_id] = work_data

    # save batch of works to directory by their id's first n characters
    for work_n_id, work_n_data in works_n.items():
        work_n_dir = f"{S3_FOLDER}/temp_batches/works/{work_n_id}"
        os.makedirs(work_n_dir, exist_ok=True)
        with open(work_n_dir + f"/batch_{batch_count}.json", "w") as f:
            json.dump(work_n_data, f)

    # save work_ids by batch
    with open(f"{S3_FOLDER}/temp_batches/work_ids/batch_{batch_count}.json", "w") as f:
        json.dump(batch_work_ids, f)

    # save isbn map by batch
    with open(f"{S3_FOLDER}/temp_batches/isbn_10/batch_{batch_count}.json", "w") as f:
        json.dump(batch_isbn10, f)
    with open(f"{S3_FOLDER}/temp_batches/isbn_13/batch_{batch_count}.json", "w") as f:
        json.dump(batch_isbn13, f)


def _aggregate_batch(batch_editions, batch_work_ids) -> dict:
    """Aggregate together the editions in one batch into a dictionary of works"""
    works = dict()

    for work_id, edition_ids in batch_work_ids.items():
        # if there's only one edition, make the work that edition's dict
        if len(edition_ids) == 1:
            works[work_id] = batch_editions[edition_ids[0]]
            continue

        work = dict()
        work_subjects = set()

        # take first instance of each work's value, except "Subjects" which is added to
        for edition_id in edition_ids:
            for key, value in batch_editions[edition_id].items():
                if key == "subjects":
                    work_subjects.update(value)
                elif key not in work.keys():
                    work[key] = value

        # make subjects a list, add work to aggregated works dictionary
        work["subjects"] = list(work_subjects)
        works[work_id] = work

    return works


def _aggregate_batches():
    """Aggregate temporary batches into corresponding folders"""

    # aggregate work ids and save
    work_ids = defaultdict(list)
    for batch_filename in tqdm(
        os.listdir(f"{S3_FOLDER}/temp_batches/work_ids"), desc="Aggregating work IDs"
    ):
        with open(f"{S3_FOLDER}/temp_batches/work_ids/{batch_filename}", "r") as f:
            batch = json.load(f)
            for work_id, editions_lst in batch.items():
                work_ids[work_id] += editions_lst
    with open(f"{S3_FOLDER}/work_ids.json", "w") as f:
        json.dump(work_ids, f)

    # aggregate isbn 10 batches and save
    isbn_10s = dict()
    for batch_filename in tqdm(
        os.listdir(f"{S3_FOLDER}/temp_batches/isbn_10"), desc="Aggregating ISBN 10"
    ):
        with open(f"{S3_FOLDER}/temp_batches/isbn_10/{batch_filename}", "r") as f:
            batch = json.load(f)
            for isbn_10, work_id in batch.items():
                isbn_10s[isbn_10] = work_id
    with open(f"{S3_FOLDER}/isbn_10s.json", "w") as f:
        json.dump(isbn_10s, f)

    # aggregate isbn 13 batches and save
    isbn_13s = dict()
    for batch_filename in tqdm(
        os.listdir(f"{S3_FOLDER}/temp_batches/isbn_13"), desc="Aggregating ISBN 13"
    ):
        with open(f"{S3_FOLDER}/temp_batches/isbn_13/{batch_filename}", "r") as f:
            batch = json.load(f)
            for isbn_13, work_id in batch.items():
                isbn_13s[isbn_13] = work_id
    with open(f"{S3_FOLDER}/isbn_13s.json", "w") as f:
        json.dump(isbn_13s, f)

    # aggregate works
    for first_n_id in tqdm(
        os.listdir(f"{S3_FOLDER}/temp_batches/works"), desc="Aggregating works"
    ):
        works_group = dict()
        for batch_group_filename in os.listdir(
            f"{S3_FOLDER}/temp_batches/works/{first_n_id}"
        ):
            with open(
                f"{S3_FOLDER}/temp_batches/works/{first_n_id}/{batch_group_filename}",
                "r",
            ) as f:
                batch_group = json.load(f)
                for work_id, work_data in batch_group.items():
                    if work_id not in works_group.keys():
                        work_data["subjects"] = set(
                            work_data.get("subjects", [])
                        )  # convert subjects to set, empty set if doesn't exist
                        works_group[work_id] = work_data

                    else:
                        work_keys = works_group[work_id].keys()
                        for key, value in work_data.items():
                            if key == "subjects":
                                works_group[work_id]["subjects"].update(value)
                            elif key not in work_keys:
                                works_group[work_id][key] = value

        # export group to json, converting subjects from set to list
        works_group = {
            work_id: {**data, "subjects": list(data.get("subjects", set()))}
            for work_id, data in works_group.items()
        }
        os.makedirs(f"{S3_FOLDER}/works", exist_ok=True)
        with open(f"{S3_FOLDER}/works/{first_n_id}.json", "w") as f:
            json.dump(works_group, f)

    # clear temporary batches
    _remove_folder(f"{S3_FOLDER}/temp_batches")


def process_in_batches(
    data_path=OL_DATA, batch_size=BATCH_SIZE, sample_size=SAMPLE_SIZE
):
    """Process OpenLibrary data in batches, main function"""

    # define variables for batch
    batch_editions = dict()
    batch_work_ids = defaultdict(list)
    batch_isbn10 = dict()
    batch_isbn13 = dict()
    batch_count = 0

    # define variable for tracking number of samples collected
    total_processed = 0

    with gzip.open(data_path, "rb") as f:
        for line in tqdm(f, desc="Processing editions"):
            key, edition = _parse_edition(line)

            if key and edition:
                work_id = edition.pop("work_id")  # get and remove work id from edition
                batch_editions[key] = edition
                batch_work_ids[work_id].append(key)
                isbn_10 = edition.get("isbn_10")
                isbn_13 = edition.get("isbn_10")
                if isbn_10:
                    batch_isbn10[isbn_10] = work_id
                if isbn_13:
                    batch_isbn13[isbn_13] = work_id
                total_processed += 1

                if len(batch_editions) >= batch_size or total_processed == sample_size:
                    # aggregate into works and save
                    batch_works = _aggregate_batch(batch_editions, batch_work_ids)
                    _save_batch(
                        batch_works,
                        batch_work_ids,
                        batch_isbn10,
                        batch_isbn13,
                        batch_count,
                        WORK_ID_FIRST_N,
                    )

                    # reset batch
                    batch_editions.clear()
                    batch_work_ids.clear()
                    batch_works.clear()
                    batch_isbn10.clear()
                    batch_isbn13.clear()
                    batch_count += 1

                # if processed the number we want, break
                if total_processed == sample_size:
                    print(
                        f"\nProcessed {total_processed} books in {batch_count} batches\n"
                    )
                    _aggregate_batches()
                    break


if __name__ == "__main__":
    process_in_batches()

import json
from tqdm import tqdm
import gzip
import os
from collections import defaultdict
from pprint import pp

# data paths
S3_FOLDER = "mock_s3"
OL_DATA = "data/openlibrary/2025-06-30/2025-06-30.txt.gz"
AMAZON_BOOK_DATA = "Books.jsonl"
AMAZON_META_DATA = "meta_Books.jsonl"

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


def _save_batch(batch_works, batch_work_ids, batch_count, n):
    """Save one batch's works (grouped into files by first n characters) and work ids"""
    # make temporary directories if they don't exist
    os.makedirs(f"{S3_FOLDER}/temp_batches", exist_ok=True)
    os.makedirs(f"{S3_FOLDER}/temp_batches/works", exist_ok=True)
    os.makedirs(f"{S3_FOLDER}/temp_batches/work_ids", exist_ok=True)

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


def _aggregate_batch(batch_editions, batch_work_ids):
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
                total_processed += 1

                if len(batch_editions) >= batch_size or total_processed == sample_size:
                    # aggregate into works and save
                    batch_works = _aggregate_batch(batch_editions, batch_work_ids)
                    _save_batch(
                        batch_works, batch_work_ids, batch_count, WORK_ID_FIRST_N
                    )

                    # reset batch
                    batch_editions.clear()
                    batch_work_ids.clear()
                    batch_works.clear()
                    batch_count += 1

                # if processed the number we want, break
                if total_processed == sample_size:
                    print(
                        f"\nProcessed {total_processed} books in {batch_count} batches\n"
                    )
                    _aggregate_batches()
                    break


def read_ol_data(self) -> tuple[dict, dict[list]]:
    """
    Read data from Open Library into (1) a dictionary of editions and (2) a dictionary mapping work ids to edition ids.
    """

    # initialize editions dictionary, before aggregating
    self.editions = dict()
    self.work_ids = dict()

    with gzip.open(self.data_path, "rb") as f:
        count = 0
        for line in tqdm(f):

            # check edition eligibility
            key, edition = _parse_edition(line)
            if key and edition:

                # add full json to editions
                self.editions[key] = edition

                # add edition id to work id
                self.work_ids[edition["work_id"]] = self.work_ids.get(
                    edition["work_id"], list()
                ) + [key]

                # break if sample size reached
                count += 1
                if self.sample_size and count > self.sample_size:
                    return self.editions, self.work_ids

        return self.editions, self.work_ids


def aggregate_ol_works(self):
    self.read_ol_data()
    self.works = dict()

    # go through every work and aggregate by editions
    for work_key, edition_ids in tqdm(self.work_ids.items()):
        work = dict()

        # define subjects (keywords) as set for easy updating
        work_subjects = set()

        # take first instance of each value, except subjects which is updated
        for edition_id in edition_ids:
            del self.editions[edition_id][
                "work_id"
            ]  # remove work ID since it'll be our key
            for key, value in self.editions[edition_id].items():
                if key == "subjects":
                    work_subjects.update(value)
                elif key not in work.keys():
                    work[key] = value

        # add subjects to work, add work to works
        work["subjects"] = list(work_subjects)
        self.works[work_key] = work


def to_json(self, first_n=WORK_ID_FIRST_N):
    """Outputs edition ids and works into Mock S3, and sorts works into folders by first n characters of work id."""

    # export works
    if first_n > 1:
        work_n = dict()

        # make dictionary where keys are first n characters of work id, values are works
        for work_id, work_data in self.works.items():
            if work_id[:first_n] not in work_n.keys():
                work_n[work_id[:first_n]] = dict()
            work_n[work_id[:first_n]][work_id] = work_data

        # export to json
        for first_n, data in work_n.items():
            with open(f"{S3_FOLDER}/books/{first_n}.json", "w") as fp:
                json.dump(data, fp)
    else:
        with open(f"{S3_FOLDER}/books/works.json", "w") as fp:
            json.dump(self.works, fp)

    # export work ids
    with open(f"{S3_FOLDER}/book_ids/edition_ids.json", "w") as fp:
        json.dump(self.work_ids, fp)


def amazon_ids(raw_json):
    pass


def amazon_book_json(line):
    pass


def read_amazon_books(books=AMAZON_BOOK_DATA):
    # only read in books whose ISBNs are in Open Library dataset
    pass


def read_amazon_meta(meta=AMAZON_META_DATA):
    pass


def read_amazon_data(books=AMAZON_BOOK_DATA, meta=AMAZON_META_DATA):
    books = read_amazon_books(books)
    isbns = set(books.keys())

    pass


if __name__ == "__main__":
    process_in_batches()
    # _aggregate_batches(0)

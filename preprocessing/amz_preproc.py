from tqdm import tqdm
import json
import gzip
from pprint import pp
import os
from collections import defaultdict


BOOK_PATH = "data/amazon/meta_Books.jsonl.gz"
REVIEWS_PATH = "data/amazon/Books.jsonl.gz"
S3_FOLDER = "mock_s3"

BOOK_SAMPLE_SIZE = 5000
REVIEW_SAMPLE_SIZE = 50000
BOOK_ID_FIRST_N = 2
REVIEW_ID_FIRST_N = 3
BATCH_SIZE = 100


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


def _parse_book(line, ol_isbn_10s, ol_isbn_13s) -> tuple[str, dict]:
    """Parse a single line into edition key and data"""

    # preprocess line into dictionary
    raw_json = json.loads(line)
    book = dict()

    # only include English books
    language = raw_json["details"].get("Language")
    if not language or language != "English":
        return None, None

    # check if book has ISBNs, otherwise return None
    isbn_10 = raw_json["details"].get("ISBN 10")
    isbn_13 = raw_json["details"].get("ISBN 13")
    if not isbn_10 and not isbn_13:
        return None, None
    if isbn_10:
        book["isbn_10"] = isbn_10
    if isbn_13:
        book["isbn_13"] = isbn_13

    # check if one of the ISBNs is in the open library dataset, otherwise return None
    # TODO watch out for this! could cause a very small sample

    if isbn_10 not in ol_isbn_10s and isbn_13 not in ol_isbn_13s:
        return None, None

    # only include books w/ more than one rating
    num_ratings = int(raw_json.get("rating_number"))
    if num_ratings > 1:
        book["num_ratings"] = num_ratings
    else:
        return None, None

    # get average rating
    avg_rating = raw_json.get("average_rating")
    if avg_rating:
        book["avg_rating"] = avg_rating

    # get genre
    genre = raw_json.get("categories")
    if genre and len(genre) >= 2:
        book["genre"] = genre[1]
    else:
        return None, None

    # get number of pages
    hard_pages = raw_json["details"].get("Hardcover")
    soft_pages = raw_json["details"].get("Paperback")
    if hard_pages:
        book["num_pages"] = int(hard_pages.split()[0])
    elif soft_pages:
        book["num_pages"] = int(soft_pages.split()[0])
    else:
        return None, None

    # get publication year
    pub_year = raw_json["details"].get("Publisher")
    if pub_year:
        try:
            pub_year = int(pub_year[-5:-1])
            book["publication_year"] = pub_year
        except ValueError:
            return None, None

    asin = raw_json["parent_asin"]

    return asin, book


def _save_book_batch(batch_books, batch_isbn10, batch_isbn13, batch_count, n):
    """Save one batch's works (grouped into files by first n characters) and work ids"""
    # make temporary directories if they don't exist
    os.makedirs(f"{S3_FOLDER}/temp_batches", exist_ok=True)
    os.makedirs(f"{S3_FOLDER}/temp_batches/amz_books", exist_ok=True)
    os.makedirs(f"{S3_FOLDER}/temp_batches/amz_isbn10", exist_ok=True)
    os.makedirs(f"{S3_FOLDER}/temp_batches/amz_isbn13", exist_ok=True)

    # group books by first n characters of id
    books_n = defaultdict(dict)
    for book_id, book_data in batch_books.items():
        books_n[book_id[:n]][book_id] = book_data

    # save batch of works to directory by their id's first n characters
    for book_n_id, book_n_data in books_n.items():
        book_n_dir = f"{S3_FOLDER}/temp_batches/amz_books/{book_n_id}"
        os.makedirs(book_n_dir, exist_ok=True)
        with open(book_n_dir + f"/batch_{batch_count}.json", "w") as f:
            json.dump(book_n_data, f)

    # save isbn10s by batch
    with open(
        f"{S3_FOLDER}/temp_batches/amz_isbn10/batch_{batch_count}.json", "w"
    ) as f:
        json.dump(batch_isbn10, f)

    # save isbn13s by batch
    with open(
        f"{S3_FOLDER}/temp_batches/amz_isbn13/batch_{batch_count}.json", "w"
    ) as f:
        json.dump(batch_isbn13, f)


def _aggregate_book_batches():
    """Aggregate temporary batches into corresponding folders"""

    # aggregate ISBN 10s and save
    isbn_10s = defaultdict(list)
    for batch_filename in tqdm(
        os.listdir(f"{S3_FOLDER}/temp_batches/amz_isbn10"), desc="Aggregating ISBN 10s"
    ):
        with open(f"{S3_FOLDER}/temp_batches/amz_isbn10/{batch_filename}", "r") as f:
            batch = json.load(f)
            for asin, isbn10 in batch.items():
                isbn_10s[asin] += isbn10
    with open(f"{S3_FOLDER}/amz_isbn10s.json", "w") as f:
        json.dump(isbn_10s, f)

    # aggregate ISBN 13s and save
    isbn_13s = defaultdict(list)
    for batch_filename in tqdm(
        os.listdir(f"{S3_FOLDER}/temp_batches/amz_isbn13"), desc="Aggregating ISBN 13s"
    ):
        with open(f"{S3_FOLDER}/temp_batches/amz_isbn13/{batch_filename}", "r") as f:
            batch = json.load(f)
            for asin, isbn13 in batch.items():
                isbn_13s[asin] += isbn13
    with open(f"{S3_FOLDER}/amz_isbn13s.json", "w") as f:
        json.dump(isbn_13s, f)

    # aggregate books
    for first_n_id in tqdm(
        os.listdir(f"{S3_FOLDER}/temp_batches/amz_books"), desc="Aggregating books"
    ):
        books_group = dict()
        for batch_group_filename in os.listdir(
            f"{S3_FOLDER}/temp_batches/amz_books/{first_n_id}"
        ):
            with open(
                f"{S3_FOLDER}/temp_batches/amz_books/{first_n_id}/{batch_group_filename}",
                "r",
            ) as f:
                batch_group = json.load(f)
                for asin, book_data in batch_group.items():
                    if asin not in books_group.keys():
                        books_group[asin] = book_data

                    else:
                        book_keys = books_group[asin].keys()
                        for key, value in book_data.items():
                            if key not in book_keys:
                                books_group[asin][key] = value

        # export group to json, converting subjects from set to list
        os.makedirs(f"{S3_FOLDER}/amz_books", exist_ok=True)
        with open(f"{S3_FOLDER}/amz_books/{first_n_id}.json", "w") as f:
            json.dump(books_group, f)

    # clear temporary batches
    _remove_folder(f"{S3_FOLDER}/temp_batches")


def process_book_batches(
    book_path=BOOK_PATH,
    batch_size=BATCH_SIZE,
    book_sample_size=BOOK_SAMPLE_SIZE,
):
    """Process Amazon book data in batches"""

    # define variables for batch
    batch_books = dict()
    batch_isbn10 = defaultdict(list)
    batch_isbn13 = defaultdict(list)
    batch_count = 0

    # variable for whether or not books have been aggregated
    aggregated = False

    # read in open library isbns before iteration for efficiency
    with open(f"{S3_FOLDER}/isbn_10s.json", "r") as f:
        ol_isbn10s = json.load(f).keys()
    with open(f"{S3_FOLDER}/isbn_13s.json", "r") as f:
        ol_isbn13s = json.load(f).keys()

    # define variable for tracking number of samples collected
    total_processed = 0

    with gzip.open(book_path, "rb") as f:
        with tqdm(
            f,
            desc="Processing books",
        ) as t:
            for line in t:
                key, edition = _parse_book(line, ol_isbn10s, ol_isbn13s)
                if total_processed % 1000 == 0:
                    t.set_postfix(total_processed=total_processed)

                if key and edition:
                    batch_books[key] = edition
                    isbn10 = edition.get("isbn_10")
                    isbn13 = edition.get("isbn_13")
                    if isbn10:
                        batch_isbn10[key].append(isbn10)
                    if isbn13:
                        batch_isbn13[key].append(isbn13)
                    total_processed += 1

                    if (
                        len(batch_books) >= batch_size
                        or total_processed == book_sample_size
                    ):
                        # aggregate into works and save
                        _save_book_batch(
                            batch_books,
                            batch_isbn10,
                            batch_isbn13,
                            batch_count,
                            BOOK_ID_FIRST_N,
                        )

                        # reset batch
                        batch_books.clear()
                        batch_isbn10.clear()
                        batch_isbn13.clear()
                        batch_count += 1

                    # if processed the number we want, break
                    if total_processed == book_sample_size:
                        print(
                            f"\nProcessed {total_processed} books in {batch_count} batches\n"
                        )
                        _aggregate_book_batches()
                        aggregated = False
                        break

    # aggregate batches in case sample size isn't reached
    if not aggregated:
        _aggregate_book_batches()


def _parse_review(line, asins) -> tuple[str, dict]:
    """Parse a single line into edition key and data"""

    # preprocess line into dictionary
    raw_json = json.loads(line)

    # only include books in our ASINs
    asin = raw_json.get("asin")
    if not asin or not asin in asins:
        return None, None

    # only include verified purchases
    if not raw_json["verified_purchase"]:
        return None, None

    # get user id and rating
    user_id = raw_json["user_id"]
    rating = raw_json["rating"]

    return user_id, {asin: rating}


def _save_review_batch(batch_reviews, batch_count, n):
    """Save one batch's works (grouped into files by first n characters) and work ids"""
    # make temporary directories if they don't exist
    os.makedirs(f"{S3_FOLDER}/temp_batches", exist_ok=True)
    os.makedirs(f"{S3_FOLDER}/temp_batches/reviews", exist_ok=True)

    # group books by first n characters of id
    reviews_n = defaultdict(dict)
    for user_id, review in batch_reviews.items():
        reviews_n[user_id[:n]][user_id] = review

    # save batch of works to directory by their id's first n characters
    for user_n_id, user_n_data in reviews_n.items():
        user_n_dir = f"{S3_FOLDER}/temp_batches/reviews/{user_n_id}"
        os.makedirs(user_n_dir, exist_ok=True)
        with open(user_n_dir + f"/batch_{batch_count}.json", "w") as f:
            json.dump(user_n_data, f)


def _aggregate_review_batches():
    """Aggregate temporary batches into corresponding folders"""
    # aggregate reviews
    for first_n_id in tqdm(
        os.listdir(f"{S3_FOLDER}/temp_batches/reviews"), desc="Aggregating reviews"
    ):
        reviews_group = defaultdict(list)
        for batch_group_filename in os.listdir(
            f"{S3_FOLDER}/temp_batches/reviews/{first_n_id}"
        ):
            with open(
                f"{S3_FOLDER}/temp_batches/reviews/{first_n_id}/{batch_group_filename}",
                "r",
            ) as f:
                batch_group = json.load(f)
                for user_id, review in batch_group.items():
                    reviews_group[user_id] += review

        # export group to json, converting subjects from set to list
        os.makedirs(f"{S3_FOLDER}/reviews", exist_ok=True)
        with open(f"{S3_FOLDER}/reviews/{first_n_id}.json", "w") as f:
            json.dump(reviews_group, f)

    # clear temporary batches
    _remove_folder(f"{S3_FOLDER}/temp_batches")


def process_review_batches(
    review_path=REVIEWS_PATH,
    batch_size=BATCH_SIZE,
    review_sample_size=REVIEW_SAMPLE_SIZE,
):
    """Process Amazon book data in batches"""

    # define variables for batch
    batch_reviews = defaultdict(list)
    batch_count = 0

    # variable for whether or not reviews have been aggregated
    aggregated = False

    # read in asins before iteration for efficiency
    with open(f"{S3_FOLDER}/amz_isbn10s.json", "r") as f:
        asins = set(json.load(f).keys())
    with open(f"{S3_FOLDER}/amz_isbn13s.json", "r") as f:
        asins.update(json.load(f).keys())

    # define variable for tracking number of samples collected
    total_processed = 0

    with gzip.open(review_path, "rb") as f:
        with tqdm(
            f,
            desc="Processing reviews",
        ) as t:
            for line in t:
                user_id, review = _parse_review(line, asins)

                if user_id and review:
                    batch_reviews[user_id].append(review)
                    total_processed += 1
                    if total_processed % 1000 == 0:
                        t.set_postfix(total_processed=total_processed)

                    if (
                        len(batch_reviews) >= batch_size
                        or total_processed == review_sample_size
                    ):
                        # aggregate into works and save
                        _save_review_batch(
                            batch_reviews,
                            batch_count,
                            REVIEW_ID_FIRST_N,
                        )

                        # reset batch
                        batch_reviews.clear()
                        batch_count += 1

                    # if processed the number we want, break
                    if total_processed == review_sample_size:
                        print(
                            f"\nProcessed {total_processed} reviews in {batch_count} batches\n"
                        )
                        _aggregate_review_batches()
                        aggregated = True
                        break

    # aggregate reviews in case sample size isn't reached
    if not aggregated:
        _aggregate_review_batches()


if __name__ == "__main__":
    process_book_batches()
    process_review_batches()

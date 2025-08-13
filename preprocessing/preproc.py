from amz_preproc import process_book_batches, process_review_batches
from ol_preproc import process_in_batches

BOOK_PATH = "data/amazon/meta_Books.jsonl.gz"
REVIEWS_PATH = "data/amazon/Books.jsonl.gz"
OL_DATA = "data/openlibrary/2025-06-30/2025-06-30.txt.gz"
S3_FOLDER = "mock_s3"

OL_BOOKS = 10000000
AMZ_BOOKS = 10000000
REVIEWS = 10000000

BATCH_SIZE = 50000
BOOK_ID_FIRST_N = 2
REVIEW_ID_FIRST_N = 3
WORK_ID_FIRST_N = 4

if __name__ == "__main__":
    print("PROCESSING OPEN LIBRARY BOOKS")
    process_in_batches(OL_DATA, BATCH_SIZE, OL_BOOKS)
    print(f"***********************************************\nPROCESSING AMAZON BOOKS")
    process_book_batches(BOOK_PATH, BATCH_SIZE, AMZ_BOOKS)
    print(f"***********************************************\nPROCESSING AMAZON REVIEWS")
    process_review_batches(REVIEWS_PATH, BATCH_SIZE, REVIEWS)

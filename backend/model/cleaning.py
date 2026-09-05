from pathlib import Path

import polars as pl

BASE_DIR = Path(__file__).parent
BOOKS_PATH = BASE_DIR / "data/goodreads/goodreads_books.csv"
WORKS_PATH = BASE_DIR / "data/goodreads/goodreads_works.csv"


def books_to_works(
    int_path: Path | str = BASE_DIR / "data/goodreads/goodreads_interactions.csv",
    books_path: Path | str = BOOKS_PATH,
):
    """
    Function to run to convert Goodreads books training datasets into works.
    """
    int_df = pl.read_csv(int_path)
    int_df = int_df.filter(pl.col("is_read") == 1)
    books_df = pl.read_csv(books_path)
    isbn_map = books_df.select(["work_id", "isbn13"]).unique("isbn13")
    isbn_map.write_parquet(BASE_DIR / "data/train/isbn_work_map.parquet")

    # convert empty to null, drop books with no isbn13
    books_df = books_df.with_columns(
        pl.when(pl.col(pl.String).str.len_chars() == 0)
        .then(None)
        .otherwise(pl.col(pl.String))
        .name.keep()
    )
    books_df = books_df.filter(pl.col("isbn13").is_not_null())

    # create a df that's grouped by work id and sums ratings count, takes most common value of others
    works_df = books_df.group_by("work_id").agg(
        pl.col("title").mode().first(),
        pl.col("ratings_count").sum(),
        pl.col("image_url").mode().first(),
    )

    # join with books df to get work ids, then drop isbn13 and null work ids
    int_df = int_df.join(
        books_df.select(("book_id", "work_id")), on="book_id", how="left"
    )
    int_df = int_df.drop_nulls("work_id")

    # write work dfs
    works_df.write_csv(BASE_DIR / "data/goodreads/goodreads_works.csv")
    int_df.write_csv(BASE_DIR / "data/goodreads/goodreads_work_interactions.csv")


def sample_books(
    n_samples: int,
    min_reviews: int,
    int_path: Path | str = BASE_DIR / "data/goodreads/goodreads_interactions.csv",
    books_path: Path | str = BOOKS_PATH,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    int_df = pl.read_csv(int_path)
    int_df = int_df.filter((pl.col("is_read") == 1) & (pl.col("rating") >= 4))
    books_df = pl.read_csv(books_path)
    isbn_book_map = books_df.select(["work_id", "isbn13"]).unique("isbn13")
    isbn_book_map.write_parquet(BASE_DIR / "data/train/isbn_book_map.parquet")

    # convert empty to null, drop books with no isbn13
    books_df = books_df.with_columns(
        pl.when(pl.col(pl.String).str.len_chars() == 0)
        .then(None)
        .otherwise(pl.col(pl.String))
        .name.keep()
    )
    books_df = books_df.filter(pl.col("isbn13").is_not_null())

    # create a df that's grouped by work id and sums ratings count, takes most common value of others
    grouped_df = books_df.group_by("work_id").agg(
        pl.col("title").mode().first(),
        pl.col("ratings_count").sum(),
        pl.col("image_url").mode().first(),
    )

    # filter grouped df to include only books with number of reviews
    grouped_df = grouped_df.filter(pl.col("ratings_count") > min_reviews)

    # filter interactions df to include only books in grouped df
    books_df = books_df.filter(pl.col("work_id").is_in(grouped_df["work_id"]))
    int_df = int_df.filter(pl.col("book_id").is_in(books_df["book_id"]))

    # sample interactions df
    int_df = int_df.sample(n_samples)

    # filter grouped df to include only books in interactions df
    books_df = books_df.filter(pl.col("book_id").is_in(int_df["book_id"].unique()))
    grouped_df = grouped_df.filter(
        pl.col("work_id").is_in(books_df["work_id"].unique())
    )
    int_df = int_df.with_columns(
        work_id=pl.col("book_id").replace(
            old=books_df["book_id"], new=books_df["work_id"]
        )
    )

    return (books_df, int_df)


def sample_works(
    n_samples: int | None = None,
    min_reviews: int | None = None,
    int_path: Path | str = BASE_DIR / "data/goodreads/goodreads_work_interactions.csv",
    works_path: Path | str = WORKS_PATH,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    # get interactions and works, filter to only books with >n interactions
    int_df = pl.read_csv(int_path)
    int_df = int_df.filter((pl.col("is_read") == 1) & (pl.col("rating") >= 4))
    works_df = pl.read_csv(works_path)
    if min_reviews:
        int_df = int_df.filter(pl.len().over("work_id") >= min_reviews)

    if n_samples:
        # sample interactions df
        int_df = int_df.sample(n_samples)

    # filter works df to include only books in interactions df
    works_df = works_df.filter(pl.col("work_id").is_in(int_df["work_id"].unique()))

    return (works_df, int_df)


if __name__ == "__main__":
    # sample_books(300000, 2000)
    books_to_works()

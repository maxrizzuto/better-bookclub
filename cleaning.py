import polars as pl

BOOKS_PATH = "data/goodreads/goodreads_books.csv"


def sample_books(
    n_samples: int,
    min_reviews: int,
    max_reviews: int = 1000000,
    int_path: str = "data/goodreads/goodreads_interactions.csv",
    books_path: str = BOOKS_PATH,
):
    int_df = pl.read_csv(int_path)
    int_df = int_df.filter((pl.col("is_read") == 1) & (pl.col("rating") >= 4))
    books_df = pl.read_csv(books_path)

    # convert empty to null, drop books with no isbn13
    books_df = books_df.with_columns(
        pl.when(pl.col(pl.String).str.len_chars() == 0)
        .then(None)
        .otherwise(pl.col(pl.String))
        .name.keep()
    )
    books_df = books_df.filter(pl.col("isbn13").is_not_null())
    books_df = books_df.filter(
        (pl.col("ratings_count") > min_reviews)
        & (pl.col("ratings_count") < max_reviews)
    )
    books_df.write_csv(f"data/goodreads/goodreads_books_{n_samples}_{min_reviews}.csv")

    books_df = books_df.select(["book_id", "isbn13"])

    int_df = int_df.join(books_df, on="book_id", how="right")
    int_df = int_df.sample(n_samples)
    int_df.write_csv(
        f"data/goodreads/goodreads_interactions_{n_samples}_{min_reviews}.csv"
    )


sample_books(300000, 200)

"""
TODOs:
- Recommendations on trained model return incorrect ratings count number for given books: it's just one
of the isbns, not the work aggregated together. It's not working at some point: DEBUG.

- Put predictions for users into folders based on the model parameters (e.g. there shouldn't be one
Max prediction file, it should be model specific)
"""

import logging
import os
import sys
import warnings
from pathlib import Path

import numpy as np
import polars as pl
import torch

from .cleaning import sample_works
from .scrapers.storygraph import Storygraph

warnings.filterwarnings("ignore", ".*Sparse CSR tensor support is in beta state.*")

# users who reviewed >= 20 books, all of which have at least 50 reviews
BASE_DIR = Path(__file__).parent
TRAINED = False
L2_LAMBDA = 500
NUM_SAMPLES = 20000000
MIN_REVIEWS = 200
GOODREADS_PATH = BASE_DIR / "storygraph.csv"
B_PATH = BASE_DIR / f"data/train/{NUM_SAMPLES}_{MIN_REVIEWS}/B{NUM_SAMPLES}.npy"
# EXPORT_ID_COL = "ISBN/UID"
UNAMES = ["mrizzuto", "itsroryo"]

# ITEM_COL = "isbn13"
ITEM_COL = "work_id"
USER_COL = "user_id"

PRED = True


# [TODO] FILTER BOOKS BY WORK_ID INITIALLY SINCE THERE ARE MULTIPLE ISBNS
class TorchEASE:
    # ALL GOOD WITH WORK ID
    def __init__(
        self,
        trained: bool = TRAINED,
        num_samples: int = NUM_SAMPLES,
        min_reviews: int = MIN_REVIEWS,
        item_col: str = ITEM_COL,
        user_col: str = USER_COL,
        l2_reg: int = L2_LAMBDA,
        score_col: str | None = None,
    ):
        """
        Class for EASE models built in PyTorch.

        trained: bool
            Whether or not the specific model has been trained. If True, load data. If False, prepare to train.
        num_samples: int
            Number of samples we want to include in our training. Used for training and file lookup.
        min_reviews: int
            Minimum number of reviews a book must receive to be in our training. Used for training and file lookup.
        item_col: str
            The column name containing our items in the training dataframe. These should be work ids from Goodreads.
        user_col: str
            The column name containing our users in the training dataframe.

        **kwargs
        l2_reg: int
            L2 regularizer hyperparameter. Mandatory if training = False.
        score_col: str
            Optional. Column with ratings in training dataframe, if you want to use scoring.

        """

        logging.basicConfig(
            format="%(asctime)s [%(levelname)s] %(message)s",
            level=logging.INFO,
            datefmt="%Y-%m-%d %H:%M:%S",
            stream=sys.stdout,
        )
        self.logger = logging.getLogger()
        self.logger.info("Building user + item lookup")

        self.num_samples = num_samples
        self.min_reviews = min_reviews
        self.item_col = item_col
        self.user_col = user_col
        self.item_id_col = self.item_col + "_id"
        self.user_id_col = self.user_col + "_id"
        self.path = BASE_DIR / f"data/train/{self.num_samples}_{self.min_reviews}/"
        self.works_path = (
            self.path / f"goodreads_works_{self.num_samples}_{self.min_reviews}.parquet"
        )
        self.l2_reg = l2_reg
        self.score_col = score_col

        if trained:
            try:
                self.logger.info("Loading files")
                self.user_lookup = pl.read_parquet(self.path / "user_lookup.parquet")
                self.item_lookup = pl.read_parquet(self.path / "item_lookup.parquet")
                self.isbn_map = pl.read_parquet(
                    self.path.parent / "isbn_work_map.parquet"
                )
                self.indices = torch.load(self.path / "indices.pt")
                self.values = torch.load(self.path / "values.pt")
                self.logger.info("Files loaded")
                self.sparse = torch.sparse_coo_tensor(self.indices.t(), self.values)
            except FileNotFoundError:
                self.logger.error("File couldn't be found: check training")
                raise

        # [TODO] check first make sure work id is properly used in training, then saved,,,,
        else:
            os.makedirs(self.path, exist_ok=True)

            try:
                train_df = pl.read_parquet(
                    self.path
                    / f"goodreads_work_interactions_{self.num_samples}_{self.min_reviews}.parquet"
                )
                self.isbn_map = pl.read_parquet(
                    self.path.parent / "isbn_work_map.parquet"
                )

                if not os.path.isfile(self.works_path):
                    raise FileNotFoundError

            except FileNotFoundError:
                self.logger.info("Training dataframe not found, sampling dataframe.")
                # isbn map is work ids to isbns
                works_df, train_df = sample_works(self.num_samples, self.min_reviews)
                train_df.write_parquet(
                    self.path
                    / f"goodreads_work_interactions_{self.num_samples}_{self.min_reviews}.parquet"
                )
                self.isbn_map = pl.read_parquet(self.path / "../isbn_work_map.parquet")
                works_df.write_parquet(self.works_path)
                del works_df

                self.logger.info("Training dataframe created and saved.")

            self.user_lookup = self._generate_labels(train_df, self.user_col)
            self.item_lookup = self._generate_labels(train_df, self.item_col)

            self.item_map = {}
            self.logger.info("Building item hashmap")
            for row in self.item_lookup.rows():
                _item, _item_id = row
                self.item_map[_item_id] = _item

            train_df = train_df.join(self.user_lookup, on=self.user_col)
            train_df = train_df.join(self.item_lookup, on=self.item_col)
            self.logger.info("User + item lookup complete")
            self.indices = torch.LongTensor(
                train_df[[self.user_id_col, self.item_id_col]].rows()
            )

            if self.score_col:
                self.values = torch.FloatTensor(train_df[self.score_col])

            else:
                # implicit values only
                self.values = torch.ones(self.indices.shape[0])

            del train_df

            self.sparse = torch.sparse_coo_tensor(self.indices.t(), self.values)
            self.logger.info("Sparse data built")

            # save all relevant data
            self.user_lookup.write_parquet(self.path / "user_lookup.parquet")
            self.item_lookup.write_parquet(self.path / "item_lookup.parquet")
            torch.save(self.indices, self.path / "indices.pt")
            torch.save(self.values, self.path / "values.pt")
            self.logger.info("Data saved")
            self.fit()

    def _generate_labels(self, df, col):
        dist_labels = df.unique([col], maintain_order=True)[[col]]
        dist_labels = dist_labels.with_columns(
            pl.col(col).unique(maintain_order=True)
        ).with_row_index(name=col + "_id")

        return dist_labels

    # [TODO] work id should work now: check
    def fit(self, export: bool = True):
        self.logger.info("Building G Matrix")
        G = torch.sparse.mm(self.sparse.T, self.sparse)
        dense = torch.eye(G.shape[0]) * self.l2_reg

        G = dense.add(G)

        self.logger.info("Taking inverse")
        P = G.inverse()

        self.logger.info("Building B Matrix")
        B = P / (-1 * P.diag())
        B = B.fill_diagonal_(0)

        if export:
            np.save(f"{self.path}/B{self.num_samples}.npy", B.numpy())

    # [TODO] work id should work now: check
    def get_user_works(self, uname) -> pl.DataFrame:
        user_works_path = BASE_DIR / f"preds/users/{uname}_works.parquet"
        user_book_path = BASE_DIR / f"preds/users/{uname}.parquet"
        if os.path.exists(user_works_path):
            return pl.read_parquet(user_works_path)
        elif os.path.exists(user_book_path):
            user_df = pl.read_parquet(user_book_path)
            if "work_id" in user_df.columns:
                user_df.write_parquet(user_works_path)
                return user_df
            elif "isbn13" in user_df.columns:
                user_df = (
                    user_df.join(self.isbn_map, on="isbn13", how="left")
                    .drop_nulls("work_id")
                    .drop("isbn")
                )
                user_df.write_parquet(user_works_path)
                return user_df
            elif "isbn" in user_df.columns:
                user_df = (
                    user_df.join(
                        self.isbn_map, left_on="isbn", right_on="isbn13", how="left"
                    )
                    .drop_nulls("work_id")
                    .drop("isbn")
                )
                user_df.write_parquet(user_works_path)
                return user_df
            else:
                self.logger.error("Error with user dataframes.")
                raise FileNotFoundError

        else:
            self.logger.info("Fetching currently reading...")
            current = Storygraph.currently_reading(uname)
            self.logger.info("Fetching to read...")
            to_read = Storygraph.to_read(uname)
            self.logger.info("Fetching books read...")
            read = Storygraph.books_read(uname)

            books = current + to_read + read

            user_df = pl.from_records(books).unique("isbn")
            user_df = (
                user_df.join(
                    self.isbn_map, left_on="isbn", right_on="isbn13", how="left"
                )
                .drop_nulls("work_id")
                .drop("isbn")
            )
            user_df.write_parquet(BASE_DIR / f"preds/users/{uname}_works.parquet")
            return user_df

    # [TODO] work id should work now: check
    def pred_df_from_uname(self, uname):
        preds_path = BASE_DIR / f"preds/users/{uname}_preds.parquet"
        if os.path.exists(preds_path):
            return pl.read_parquet(preds_path)
        else:
            user_df = self.get_user_works(uname)
            pred_df = model.pred(user_df, "work_id", n=None)
            pred_df.write_parquet(preds_path)
            return pred_df

    # [TODO] check that work id works
    def pred(
        self,
        pred_df: pl.DataFrame,
        id_col: str,
        n: None | int = 20,
    ) -> pl.DataFrame:
        """
        Take in goodreads dataframe with interacted books, return top 20

        pred_df : pl.DataFrame
            DataFrame of user's books to make predictions on.
        id_col : str
            The id column in the user's dataframe.
        n : None | int = 20
            The number of books to return.
        """
        # convert isbns to work ids
        if "isbn" in id_col:
            pred_df = pred_df.join(
                self.isbn_map, how="inner", left_on=id_col, right_on="isbn13"
            )
        works_df = pl.read_parquet(self.works_path)

        interacted_books = (
            pred_df.with_columns(
                pl.col(id_col).cast(pl.String).str.strip_chars('"="').cast(pl.Int64)
            )
            .join(self.item_lookup, left_on=id_col, right_on=self.item_col, how="left")[
                self.item_id_col
            ]
            .drop_nulls()
            .to_numpy()
        )

        # create one-hot vector
        gr_vector = np.zeros((1, len(self.item_lookup)))
        gr_vector[:, interacted_books] = 1
        shape = len(self.item_lookup)

        self.logger.info("Generating predictions")
        try:
            B = np.memmap(
                self.path / f"B{self.num_samples}.npy",
                dtype="float32",
                mode="r",
                shape=(shape, shape),
                offset=128,
            )
            preds = np.zeros((1, shape))
            for i in range(0, B.shape[0], 1000):
                chunk = B[:, i : i + 1000]
                result = np.dot(gr_vector, chunk)
                preds[:, i : i + 1000] = result

        except AttributeError:
            self.logger.exception("B matrix not found: fit model or load matrix")
            raise

        preds = torch.from_numpy(preds)
        top_n_idx = torch.argsort(preds, descending=True)[0]
        if n:
            top_n_idx = top_n_idx[:n]
        pred_vals = preds[:, top_n_idx]
        top_n_ids = [
            model.item_lookup.filter(pl.col(self.item_id_col) == x.item())[
                self.item_col
            ].item()
            for x in top_n_idx
        ]

        works_df = works_df.filter(pl.col(self.item_col).is_in(top_n_ids))
        works_df = works_df.sort(
            by=pl.col(self.item_col)
            .cast(pl.String)
            .cast(pl.Enum(list(map(str, top_n_ids))))
        )
        works_df = works_df.with_columns(pl.Series("preds", pred_vals[0]))
        self.logger.info("Predictions complete")

        return works_df

    def group_preds(self, unames: list[str]) -> pl.DataFrame:
        pred_df = pl.DataFrame()
        unames.sort()
        preds_path = BASE_DIR / f"preds/groups/{'_'.join(unames)}_preds.parquet"
        if os.path.exists(preds_path):
            return pl.read_parquet(preds_path)
        for uname in unames:
            self.logger.info(f"Getting {uname} predictions.")
            if os.path.exists(BASE_DIR / f"preds/users/{uname}_preds.parquet"):
                user_df = pl.read_parquet(
                    BASE_DIR / f"preds/users/{uname}_preds.parquet"
                )
            elif os.path.exists(BASE_DIR / f"preds/users/{uname}_works.parquet"):
                user_df = pl.read_parquet(
                    BASE_DIR / f"preds/users/{uname}_works.parquet"
                )
                user_df = self.pred(user_df, "work_id", n=None)
                user_df.write_parquet(BASE_DIR / f"preds/users/{uname}_preds.parquet")
            else:
                user_df = self.pred_df_from_uname(uname)

            if len(pred_df) > 0:
                other_cols = [
                    col for col in user_df.columns if col not in ("preds", "work_id")
                ]
                pred_df = (
                    pl.concat([pred_df, user_df], how="diagonal_relaxed")
                    .group_by("work_id")
                    .agg(
                        preds=pl.col("preds").mean(),
                        *[pl.col(c).first() for c in other_cols],
                    )
                )
            else:
                pred_df = user_df

        pred_df = pred_df.sort("preds", descending=True)
        pred_df.write_parquet(preds_path)

        return pred_df


if __name__ == "__main__":
    model = TorchEASE(
        trained=TRAINED,
        num_samples=NUM_SAMPLES,
        min_reviews=MIN_REVIEWS,
        item_col=ITEM_COL,
        user_col=USER_COL,
        l2_reg=L2_LAMBDA,
    )

    if PRED:
        model.group_preds(UNAMES)
        max_df = model.pred_df_from_uname("mrizzuto")
        print(max_df)
        max_df.write_csv(f"preds/preds_{L2_LAMBDA}l_{NUM_SAMPLES}.csv")

# try to make sparse matrix lmao
import polars as pl
import torch
import logging
import sys
import os
import warnings
import numpy as np
from cleaning import sample_books

warnings.filterwarnings("ignore", ".*Sparse CSR tensor support is in beta state.*")

# users who reviewed >= 20 books, all of which have at least 50 reviews
TRAINED = True
L2_LAMBDA = 1000
NUM_SAMPLES = 300000
MIN_REVIEWS = 200
GOODREADS_PATH = "storygraph.csv"
BOOKS_PATH = f"data/train/{NUM_SAMPLES}_{MIN_REVIEWS}/goodreads_books_{NUM_SAMPLES}_{MIN_REVIEWS}.parquet"
B_PATH = f"data/train/{NUM_SAMPLES}_{MIN_REVIEWS}/B{NUM_SAMPLES}.npy"
EXPORT_ID_COL = "ISBN/UID"


class TorchEASE:
    def __init__(
        self,
        trained: bool,
        num_samples: int,
        min_reviews: int,
        item_col: str,
        user_col: str,
        **kwargs,
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
            The column name containing our items in the training dataframe. These should be ISBNs.
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
        self.path = f"data/train/{self.num_samples}_{self.min_reviews}/"

        if trained:
            try:
                self.logger.info("Loading files")
                self.user_lookup = pl.read_parquet(self.path + "user_lookup.parquet")
                self.item_lookup = pl.read_parquet(self.path + "item_lookup.parquet")
                self.indices = torch.load(self.path + "indices.pt")
                self.values = torch.load(self.path + "values.pt")
                self.logger.info("Files loaded")
                self.sparse = torch.sparse_coo_tensor(self.indices.t(), self.values)
            except FileNotFoundError:
                self.logger.error("File couldn't be found: check training")
                raise

        else:
            os.makedirs(
                f"data/train/{self.num_samples}_{self.min_reviews}", exist_ok=True
            )
            try:
                self.l2_reg = kwargs.get("l2_reg")
                if not self.l2_reg:
                    raise ValueError(
                        "Kwarg missing: L2 hyperparameter. Pass in with name l2_reg."
                    )

            except ValueError as e:
                self.logger.error(e)
                raise

            try:
                train_df = pl.read_parquet(
                    self.path
                    + f"goodreads_interactions_{self.num_samples}_{self.min_reviews}.parquet"
                )

                if not os.path.isfile(
                    self.path
                    + f"goodreads_books_{self.num_samples}_{self.min_reviews}.parquet"
                ):
                    raise FileNotFoundError

            except FileNotFoundError:
                self.logger.error("Training dataframe not found, sampling dataframe.")
                books_df, train_df = sample_books(self.num_samples, self.min_reviews)
                train_df.write_parquet(
                    self.path
                    + f"goodreads_interactions_{self.num_samples}_{self.min_reviews}.parquet"
                )
                books_df.write_parquet(
                    self.path
                    + f"goodreads_books_{self.num_samples}_{self.min_reviews}.parquet"
                )
                del books_df

                self.logger.info("Training dataframe created and saved.")

            self.user_lookup = self.generate_labels(train_df, self.user_col)
            self.item_lookup = self.generate_labels(train_df, self.item_col)

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

            self.score_col = kwargs.get("score_col")
            if self.score_col:
                self.values = torch.FloatTensor(train_df[self.score_col])

            else:
                # implicit values only
                self.values = torch.ones(self.indices.shape[0])

            del train_df

            self.sparse = torch.sparse_coo_tensor(self.indices.t(), self.values)
            self.logger.info("Sparse data built")

            # save all relevant data
            self.user_lookup.write_parquet(self.path + "user_lookup.parquet")
            self.item_lookup.write_parquet(self.path + "item_lookup.parquet")
            torch.save(self.indices, self.path + "indices.pt")
            torch.save(self.values, self.path + "values.pt")
            self.logger.info("Data saved")

    def generate_labels(self, df, col):
        dist_labels = df.unique([col], maintain_order=True)[[col]]
        dist_labels = dist_labels.with_columns(
            pl.col(col).unique(maintain_order=True)
        ).with_row_index(name=col + "_id")

        return dist_labels

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

        return

    def pred(
        self,
        pred_df: pl.DataFrame,
        books_df: pl.DataFrame,
        id_col: str,
        n: int = 20,
    ):
        """
        Take in goodreads dataframe with interacted books, return top 20
        """
        interacted_books = (
            pred_df.with_columns(pl.col(id_col).str.strip_chars('"="'))
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
                self.path + f"B{self.num_samples}.npy",
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
            self.logger.error("B matrix not found: fit model or load matrix")
            return

        preds = torch.from_numpy(preds)
        top_n_idx = torch.argsort(preds, descending=True)[0][:n]
        pred_vals = preds[:, top_n_idx]
        top_n_ids = [
            model.item_lookup.filter(pl.col(self.item_id_col) == x.item())[
                self.item_col
            ].item()
            for x in top_n_idx
        ]

        books_df = books_df.filter(pl.col(self.item_col).is_in(top_n_ids))
        books_df = books_df.sort(
            by=pl.col(self.item_col)
            .cast(pl.String)
            .cast(pl.Enum(list(map(str, top_n_ids))))
        )
        books_df = books_df.with_columns(pl.Series("preds", pred_vals[0]))
        self.logger.info("Predictions complete")

        return books_df


if __name__ == "__main__":

    model = TorchEASE(
        trained=TRAINED,
        num_samples=NUM_SAMPLES,
        min_reviews=MIN_REVIEWS,
        item_col="isbn13",
        user_col="user_id",
        l2_reg=L2_LAMBDA,
    )
    if not TRAINED:
        model.fit()

    # predict
    gr_df = pl.read_csv(GOODREADS_PATH)
    books_df = pl.read_parquet(BOOKS_PATH)
    preds_df = model.pred(gr_df, books_df, id_col=EXPORT_ID_COL)
    print(preds_df)
    preds_df.write_csv(f"preds/preds_{L2_LAMBDA}l_{NUM_SAMPLES}.csv")

# try to make sparse matrix lmao
import polars as pl
import torch
import logging
import sys
import os

import warnings

warnings.filterwarnings("ignore", ".*Sparse CSR tensor support is in beta state.*")

# users who reviewed >= 20 books, all of which have at least 50 reviews
L2_LAMBDA = 1000
GOODREADS_PATH = "goodreads_export.csv"
TRAIN_PATH = f"data/train/goodreads_interactions_300000_200.csv"
BOOKS_PATH = "data/train/goodreads_books_300000_200.csv"
B_PATH = "data/train/B300k.pt"


class TorchEASE:
    def __init__(
        self,
        train_df: pl.DataFrame,
        user_col: str = "user_id",
        item_col: str = "item_id",
        score_col: str | None = None,
        l2_reg: int = L2_LAMBDA,
        B_path: str = B_PATH,
    ):
        """
        Docstring for __init__

        :param train_df: Dataframe containing user-item interactions used for training.
        :param user_col: Name of column containing user ids
        :param item_col: Name of column containing item ids
        :param score_col: Name of column containing scores/ratings (optional)
        :param l2_reg: Lambda hyperparameter value
        """

        logging.basicConfig(
            format="%(asctime)s [%(levelname)s] %(message)s",
            level=logging.INFO,
            datefmt="%Y-%m-%d %H:%M:%S",
            stream=sys.stdout,
        )
        self.logger = logging.getLogger()
        self.logger.info("Building user + item lookup")

        self.l2_reg = l2_reg
        self.user_col = user_col
        self.item_col = item_col
        self.score_col = score_col
        self.B_path = B_path
        if os.path.isfile(self.B_path):
            self._load_B()

        self.user_id_col = user_col + "_id"
        self.item_id_col = item_col + "_id"

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

        if not score_col:
            # Implicit values only
            self.values = torch.ones(self.indices.shape[0])
        else:
            self.values = torch.FloatTensor(train_df[score_col])

        self.sparse = torch.sparse_coo_tensor(self.indices.t(), self.values)
        self.logger.info("Sparse data built")

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
        self.B = B

        if export:
            torch.save(B, self.B_path)

        return

    def _load_B(self):
        self.B = torch.load(self.B_path)
        return

    def pred(self, pred_df: pl.DataFrame, books_df: pl.DataFrame, n: int = 20):
        """
        Take in goodreads dataframe with interacted books, return top 20
        """
        interacted_books = (
            pred_df.with_columns(pl.col("ISBN13").str.strip_chars('"="'))
            .join(
                self.item_lookup, left_on="ISBN13", right_on=self.item_col, how="left"
            )[self.item_id_col]
            .drop_nulls()
            .to_numpy()
        )

        # create one-hot vector
        gr_vector = torch.zeros((1, len(self.item_lookup)))
        gr_vector[:, interacted_books] = 1

        try:
            preds = torch.mm(gr_vector, self.B)
        except AttributeError:
            self.logger.error("B matrix not found: fit model or load matrix")
            return

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

        return books_df


# fit model
train_df = pl.read_csv(TRAIN_PATH, schema_overrides={"isbn13": pl.String})
model = TorchEASE(train_df, "user_id", "isbn13", l2_reg=L2_LAMBDA, B_path=B_PATH)
del train_df
model.fit()


# predict
gr_df = pl.read_csv(GOODREADS_PATH)
books_df = pl.read_csv(BOOKS_PATH, schema_overrides={"isbn13": pl.String})
preds_df = model.pred(gr_df, books_df)
print(preds_df)
preds_df.write_csv(f"preds_{L2_LAMBDA}l_300k.csv")

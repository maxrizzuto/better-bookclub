from pathlib import Path
from typing import Annotated

import polars as pl
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from model.scrapers.storygraph import Storygraph
from model.torchEASE import TorchEASE

BASE_DIR = Path(__file__).parent

app = FastAPI()

# [TODO] look into middleware
origins = ["http://localhost:5173"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def root():
    return {"message": "Hello World."}


@app.get("/users")
async def books(
    user: Annotated[
        list[str] | None,
        Query(description="List of users to get books for from Storygraph."),
    ],
):
    if user:
        users = list()
        for username in user:
            try:
                user_path = BASE_DIR / f"model/data/users/{username}_works.parquet"
                df = pl.read_parquet(user_path)
                if "last date read" in df.columns:
                    df = df.sort(
                        ["rating", "last date read"],
                        descending=[True, True],
                        nulls_last=True,
                    )
                else:
                    df = df.sort("rating", descending=True, nulls_last=True)

                shelf_counts = df["shelf"].value_counts()
                shelf_counts = {
                    k: v
                    for k, v in dict(
                        zip(shelf_counts["shelf"], shelf_counts["count"])
                    ).items()
                    if k in ["Read", "To read", "Currently reading"]
                }

                # do data for graph here later, seems too complicated to figure out now

                books_list = (
                    df[:12]
                    .select("title", "work_id", "isbn13", "rating", "shelf")
                    .to_dicts()
                )
                users.append(
                    {"username": username, "books": books_list, "shelves": shelf_counts}
                )

            except FileNotFoundError:
                return {"username": username, "books": [], "shelves": {}}
        return users


@app.get("/recommendations")
async def recs(
    user: Annotated[
        list[str] | None, Query(description="List of users to get recommendations for.")
    ] = None,
):
    if user:
        model = TorchEASE()

        # [TODO] update to cloud storage url
        pred_df = model.group_preds(user)

        result = {}
        result["usernames"] = user
        group_results = (
            pred_df.with_columns(
                preds=pl.col("preds") / pl.col("preds").max()
            ).with_columns(preds=pl.col("preds").round(3))[:10]
        ).to_dicts()
        result["group_results"] = group_results
        result["user_results"] = {}
        for username in user:
            user_results = (
                model.pred_df_from_uname(username)
                .with_columns(preds=pl.col("preds") / pl.col("preds").max())
                .with_columns(preds=pl.col("preds").round(3))[:10]
            ).to_dicts()
            result["user_results"][username] = user_results[:10]
        return result
    else:
        return {"message": "no usernames"}


"""
[TODO]: Optimize this later so it's not always reading the map into memory.
"""


@app.get("/work_id")
async def work_id(
    isbn: Annotated[
        str | None, Query(description="ISBN13 to get the corresponding work id of.")
    ] = None,
):
    isbn_map = pl.read_parquet(BASE_DIR / "model/data/train/isbn_work_map.parquet")
    try:
        work_id = isbn_map.filter(pl.col("isbn13") == isbn)["work_id"].item()
        return work_id
    except ValueError:
        return None

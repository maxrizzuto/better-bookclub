import os
from pathlib import Path
from typing import Annotated

import polars as pl
from fastapi import BackgroundTasks, FastAPI, Query
from model.scrapers.storygraph import Storygraph
from model.torchEASE import TorchEASE

BASE_DIR = Path(__file__).parent


app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Hello World."}


def start_stream_books(username: str):
    Storygraph.stream_books(username)
    return


@app.get("/users")
async def books(
    background_tasks: BackgroundTasks,
    user: Annotated[
        list[str] | None,
        Query(description="List of users to get books for from Storygraph."),
    ] = None,
):
    if user:
        dct = dict()
        dct["status"] = "complete"
        dct["userBooks"] = {}
        for username in user:
            user_path = BASE_DIR / f"model/preds/users/{username}.parquet"
            temp_path = BASE_DIR / f"model/preds/users/{username}-temp.parquet"
            if os.path.exists(user_path):
                dct["userBooks"][username] = pl.read_parquet(user_path).to_dicts()
            elif os.path.exists(temp_path):
                dct["userBooks"][username] = pl.read_parquet(temp_path).to_dicts()
                dct["status"] = "in progress"
            else:
                background_tasks.add_task(start_stream_books, username)
                dct["status"] = "in progress"
                pl.DataFrame().write_parquet(temp_path)
        return dct

    else:
        return {"status": "No usernames input."}


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

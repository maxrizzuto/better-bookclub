from pathlib import Path
from typing import Annotated

import polars as pl
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.sse import EventSourceResponse
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


@app.get("/users", response_class=EventSourceResponse)
async def books(
    user: Annotated[
        list[str] | None,
        Query(description="List of users to get books for from Storygraph."),
    ],
):
    if user:
        for username in user:
            for books in Storygraph.stream_books(username):
                yield {"username": username, "books": books}

    yield {"data": "close"}


# def start_stream_books(username: str):
#     Storygraph.stream_books(username)
#     user_path = BASE_DIR / f"model/preds/users/{username}.parquet"
#     temp_path = BASE_DIR / f"model/preds/users/{username}-temp.parquet"
#     try:
#         user_df = pl.read_parquet(temp_path)
#         user_df = user_df.unique("isbn")
#         user_df.write_parquet(user_path)
#         os.remove(temp_path)
#     except FileNotFoundError:
#         return


# @app.get("/users")
# async def books(
#     background_tasks: BackgroundTasks,
#     user: Annotated[
#         list[str] | None,
#         Query(description="List of users to get books for from Storygraph."),
#     ] = None,
# ):
#     if user:
#         dct = dict()
#         dct["status"] = "complete"
#         dct["userBooks"] = {}
#         for username in user:
#             user_path = BASE_DIR / f"model/preds/users/{username}.parquet"
#             temp_path = BASE_DIR / f"model/preds/users/{username}-temp.parquet"
#             if os.path.exists(user_path):
#                 dct["userBooks"][username] = pl.read_parquet(user_path).to_dicts()
#             elif os.path.exists(temp_path):
#                 dct["userBooks"][username] = pl.read_parquet(temp_path).to_dicts()
#                 dct["status"] = "in progress"
#             else:
#                 background_tasks.add_task(start_stream_books, username)
#                 dct["status"] = "in progress"
#                 pl.DataFrame().write_parquet(temp_path)
#         print(dct["status"])
#         return dct

#     else:
#         return {"status": "No usernames input."}


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

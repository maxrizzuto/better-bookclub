from pathlib import Path
from typing import Annotated

import httpx
import polars as pl
import requests
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
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
                    .select("title", "work_id", "isbn13", "rating", "shelf", "olid")
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

        result = {"usernames": user, "user_results": {}, "group_results": []}
        group_works = []

        for username in user:
            user_works = list(model.get_user_works(username)["work_id"])
            group_works += user_works
            user_results = (
                model.pred_df_from_uname(username)
                .filter(~pl.col("work_id").is_in(user_works))
                .with_columns(preds=pl.col("preds") / pl.col("preds").max())
                .with_columns(preds=pl.col("preds").round(3))[:12]
            ).to_dicts()
            result["user_results"][username] = user_results[:12]

        group_works = set(group_works)

        # [TODO] update to cloud storage url
        pred_df = model.group_preds(user).filter(~pl.col("work_id").is_in(group_works))

        group_results = (
            pred_df.with_columns(
                preds=pl.col("preds") / pl.col("preds").max()
            ).with_columns(preds=pl.col("preds").round(3))[:12]
        ).to_dicts()
        result["group_results"] = group_results
        print(result)
        return result

    else:
        return {"message": "no usernames"}


@app.get("/image_url")
async def image_url(
    id: Annotated[
        str,
        Query(description="Either ISBN or OLID to attempt to find cover id for."),
    ],
):
    id_type = "isbn"
    if "OL" in id:
        id_type = "books"
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"https://openlibrary.org/{id_type}/{id}.json")
            data = response.json()
            response = await client.get(
                f"https://openlibrary.org{data['works'][0]['key']}.json"
            )

        data = response.json()
        cover_url = f"https://covers.openlibrary.org/b/id/{data['covers'][0]}-L.jpg"
        print(cover_url)
        return {"url": cover_url}
    except:
        return {"url": "https://covers.openlibrary.org/b/olid/null-S"}

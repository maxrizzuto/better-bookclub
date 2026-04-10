from typing import Annotated

from fastapi import FastAPI, Query
from model.scrapers.storygraph import Storygraph
from model.torchEASE import TorchEASE

app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Hello World."}


@app.get("/users")
async def books(
    user: Annotated[
        list[str] | None,
        Query(description="List of users to get books for from Storygraph."),
    ] = None,
):
    if user:
        dct = dict()
        for username in user:
            user_books = Storygraph.get_user_books(username)
            dct[username] = user_books
        return dct

    else:
        return {"message": "No usernames input."}


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

        return {
            "message": "the right endpoint!",
            "usernames": user,
            "num_users": len(user),
            "top_pred": pred_df["title"][0],
        }
    else:
        return {"message": "no usernames"}

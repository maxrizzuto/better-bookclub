import logging
import os
import random
import time
from pathlib import Path

import polars as pl
import undetected_chromedriver as uc
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

BASE_DIR = Path(__file__).parent.parent
load_dotenv(BASE_DIR / "scrapers/.env")
COOKIE = os.getenv("COOKIE")
UNAME = os.getenv("SG_UNAME")
PW = os.getenv("SG_PW")
LOGGER = logging.getLogger()


class StorygraphScraper:
    @staticmethod
    def fetch_url_stream(url, cookie=COOKIE):
        # options = uc.ChromeOptions()
        # options.add_argument("--window-size=1920,1080")
        service = ChromeService(ChromeDriverManager().install())
        driver = webdriver.Chrome(
            service=service,
        )  # pyright: ignore[reportCallIssue]

        driver.get(url)

        if cookie:
            driver.delete_all_cookies()
            driver.add_cookie({"name": "remember_user_token", "value": cookie})

        driver.get(url)

        # wait for Cloudflare to finish
        wait = WebDriverWait(driver, 2000)
        wait.until_not(EC.title_contains("Just a moment"))

        if EC.title_contains("Sign In"):
            username = driver.find_element("id", "user_email")
            password = driver.find_element("id", "user_password")

            if UNAME and PW:
                username.send_keys(UNAME)
                password.send_keys(PW)

            driver.find_element("id", "sign-in-btn").click()

        # driver.refresh()
        SCROLL_PAUSE_TIME = 5
        last_height = driver.execute_script("return document.body.scrollHeight")

        try:
            while True:
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(SCROLL_PAUSE_TIME + random.random() * 3)
                new_height = driver.execute_script("return document.body.scrollHeight")

                yield driver.page_source

                if new_height == last_height:
                    break
                last_height = new_height
        finally:
            driver.quit()


class Storygraph:
    @staticmethod
    def parse_html(html, shelf):
        soup = BeautifulSoup(html, "html.parser")
        books_list = list()
        rating_map = dict()

        if soup.find(string="Sorry, that page doesn't exist!"):
            raise ValueError("Username not found. Please try a different username.")

        p_elements = [
            x.parent
            for x in soup.select("div[class*='hidden edition-info mt-3'] p span")
            if "ISBN" in x.text and x.parent
        ]

        books = [
            x.parent.parent.select("div[class*='book-title-author-and-series'] > h3")[0]
            for x in p_elements
            if x.parent and x.parent.parent
        ]
        isbns = [x.text.split(" ")[-1].strip() for x in p_elements]

        if shelf == "Read":
            ratings = soup.select(
                "div[data-controller='remove-book']>div>div>div>div>span"
            )
            rating_isbns = list()
            for rating in ratings:
                try:
                    rating_isbns.append(
                        rating.parent.parent.parent.parent.parent.parent.parent.select(
                            "div[class*='hidden edition-info mt-3'] p"
                        )[0]
                        .get_text()
                        .split(" ")[-1]
                        .strip()
                    )
                except IndexError:
                    rating_isbns.append(None)
                rating_map = {
                    rating_isbns[idx]: ratings[idx].get_text()
                    for idx in range(len(rating_isbns))
                    if rating_isbns[idx]
                }

        for idx in range(len(books)):
            book = books[idx]
            isbn = isbns[idx].strip()
            title = book.find("a")
            if title:
                title = title.text.strip()

            storygraph_id = book.find("a")
            if storygraph_id:
                storygraph_id = str(storygraph_id["href"]).split("/")[-1]
            book = {
                "title": title,
                "storygraph_id": storygraph_id,
                "isbn": isbn,
                "shelf": shelf,
            }
            if shelf == "Read" and isbn in rating_map.keys():
                book["rating"] = rating_map[isbn]

            books_list.append(book)
        return books_list

    @staticmethod
    def stream_books(uname, cookie=COOKIE):
        user_book_path = BASE_DIR / f"preds/users/{uname}.parquet"
        temp_book_path = f"{''.join(str(user_book_path).split('.')[:-1])}-temp.parquet"
        if os.path.exists(user_book_path):
            yield pl.read_parquet(user_book_path).to_dicts()

        else:
            url_map = {
                "Currently reading": f"https://app.thestorygraph.com/currently-reading/{uname}",
                "To read": f"https://app.thestorygraph.com/to-read/{uname}",
                "Read": f"https://app.thestorygraph.com/books-read/{uname}",
            }

            books = []
            seen_ids = set()
            for shelf, url in url_map.items():
                LOGGER.info(f"Fetching {shelf}...")
                for html_snapshot in StorygraphScraper.fetch_url_stream(url, cookie):
                    shelf_books = Storygraph.parse_html(html_snapshot, shelf)
                    new_books = [
                        book
                        for book in shelf_books
                        if book["isbn"] and book["isbn"] not in seen_ids
                    ]
                    seen_ids.update(book["isbn"] for book in new_books)
                    if new_books:
                        books += new_books
                        pl.from_records(books).write_parquet(temp_book_path)

                    # trying to add SSE? does this convert it to a generator properly?
                    yield new_books

            if books:
                user_df = pl.from_records(books)
                user_df = user_df.unique("isbn")
                user_df.write_parquet(user_book_path)
                os.remove(temp_book_path)


if __name__ == "__main__":
    Storygraph.stream_books("mrizzuto")

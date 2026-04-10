import logging
import os
import time
from pathlib import Path

import polars as pl
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

COOKIE = os.getenv("COOKIE")
LOGGER = logging.getLogger()
BASE_DIR = Path(__file__).parent.parent


class StorygraphScraper:
    @staticmethod
    def fetch_url(url, cookie=COOKIE):
        options = Options()
        options.add_argument("--headless")
        driver = webdriver.Chrome(options=options)  # pyright: ignore[reportCallIssue]
        driver.get(url)

        if cookie:
            driver.add_cookie({"name": "remember_user_token", "value": cookie})
        driver.refresh()
        SCROLL_PAUSE_TIME = 5
        last_height = driver.execute_script("return document.body.scrollHeight")
        while True:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(SCROLL_PAUSE_TIME)
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height
        html_content = driver.page_source
        driver.quit()
        return html_content

    @staticmethod
    def currently_reading(uname, cookie=COOKIE):
        url = f"https://app.thestorygraph.com/currently-reading/{uname}"
        return StorygraphScraper.fetch_url(url, cookie)

    @staticmethod
    def to_read(uname, cookie=COOKIE):
        url = f"https://app.thestorygraph.com/to-read/{uname}"
        return StorygraphScraper.fetch_url(url, cookie)

    @staticmethod
    def books_read(uname, cookie=COOKIE):
        url = f"https://app.thestorygraph.com/books-read/{uname}"
        return StorygraphScraper.fetch_url(url, cookie)


class Storygraph:
    @staticmethod
    def parse_html(html, shelf):
        soup = BeautifulSoup(html, "html.parser")
        books_list = list()

        if soup.find(string="Sorry, that page doesn't exist!"):
            raise ValueError("Username not found. Please try a different username.")

        p_elements = [
            x.parent
            for x in soup.select("div[class*='hidden edition-info mt-3'] p span")
            if "ISBN" in x.text and x.parent
        ]

        books = [
            x.parent.parent.select("div[class*='book-title-author-and-series']")[0]
            for x in p_elements
            if x.parent and x.parent.parent
        ]
        isbns = [x.text.split(" ")[-1].strip() for x in p_elements]

        for idx in range(len(books)):
            book = books[idx]
            isbn = isbns[idx].strip()
            title = book.find("a")
            if title:
                title = title.text.strip()

            storygraph_id = book.find("a")
            if storygraph_id:
                storygraph_id = str(storygraph_id["href"]).split("/")[-1]
            books_list.append(
                {
                    "title": title,
                    "storygraph_id": storygraph_id,
                    "isbn": isbn,
                    "shelf": shelf,
                }
            )
        return books_list

    @staticmethod
    def currently_reading(uname, cookie=COOKIE):
        content = StorygraphScraper.currently_reading(uname, cookie)
        return Storygraph.parse_html(content, "Currently reading")

    @staticmethod
    def to_read(uname, cookie=COOKIE):
        content = StorygraphScraper.to_read(uname, cookie)
        return Storygraph.parse_html(content, "To read")

    @staticmethod
    def books_read(uname, cookie=COOKIE):
        content = StorygraphScraper.books_read(uname, cookie)
        return Storygraph.parse_html(content, "Read")

    @staticmethod
    def get_user_books(uname, cookie=COOKIE):
        user_book_path = BASE_DIR / f"preds/users/{uname}.parquet"
        print(user_book_path)
        if os.path.exists(user_book_path):
            user_df = pl.read_parquet(user_book_path)
            return user_df.to_dicts()

        LOGGER.info("Fetching currently reading...")
        current = Storygraph.currently_reading(uname, cookie)
        LOGGER.info("Fetching to read...")
        to_read = Storygraph.to_read(uname, cookie)
        LOGGER.info("Fetching books read...")
        read = Storygraph.books_read(uname, cookie)

        books = current + to_read + read
        pl.from_records(books).write_parquet(BASE_DIR / f"preds/users/{uname}.parquet")
        return books

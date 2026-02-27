from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import time
import re
import json


class StorygraphScraper:
    @staticmethod
    def fetch_url(url, cookie):
        options = Options()
        options.add_argument("--headless")
        driver = webdriver.Chrome(options=options)
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
    def currently_reading(uname, cookie):
        url = f"https://app.thestorygraph.com/currently-reading/{uname}"
        return StorygraphScraper.fetch_url(url, cookie)

    @staticmethod
    def to_read(uname, cookie):
        url = f"https://app.thestorygraph.com/to-read/{uname}"
        return StorygraphScraper.fetch_url(url, cookie)

    @staticmethod
    def books_read(uname, cookie):
        url = f"https://app.thestorygraph.com/books-read/{uname}"
        return StorygraphScraper.fetch_url(url, cookie)


class Storygraph:
    @staticmethod
    def parse_html(html):
        soup = BeautifulSoup(html, "html.parser")
        books_list = list()
        # books = soup.find_all("div", class_="book-title-author-and-series")
        p_elements = [
            x.parent
            for x in soup.select("div[class*='hidden edition-info mt-3'] p span")
            if "ISBN" in x.text
        ]
        books = [
            x.parent.parent.select("div[class*='book-title-author-and-series']")[0]
            for x in p_elements
        ]
        isbns = [x.text.split(" ")[-1].strip() for x in p_elements]

        for idx in range(len(books)):
            book = books[idx]
            isbn = isbns[idx].strip()
            title = book.find("a").text.strip()
            storygraph_id = book.find("a")["href"].split("/")[-1]
            books_list.append(
                {"title": title, "storygraph_id": storygraph_id, "isbn": isbn}
            )
        return books_list

    @staticmethod
    def currently_reading(uname, cookie):
        content = StorygraphScraper.currently_reading(uname, cookie)
        return Storygraph.parse_html(content)

    @staticmethod
    def to_read(uname, cookie):
        content = StorygraphScraper.to_read(uname, cookie)
        return Storygraph.parse_html(content)

    @staticmethod
    def books_read(uname, cookie):
        content = StorygraphScraper.books_read(uname, cookie)
        return Storygraph.parse_html(content)

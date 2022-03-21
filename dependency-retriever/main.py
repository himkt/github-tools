from dataclasses import dataclass
import locale
import logging
import random
import time
from typing import Any, Optional, Tuple
import sqlite3

import bs4
import requests


locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')
logging.basicConfig(level=logging.DEBUG)


logger = logging.getLogger(__file__)
conn = sqlite3.connect("sample.db")


@dataclass
class Repository:
    authority: str
    name: str
    num_stars: int
    num_forks: int

    def to_tuple(self) -> Tuple[str, str, int, int]:
        return (
            self.authority,
            self.name,
            self.num_stars,
            self.num_forks,
        )


class Storage:
    TB = "repositories"

    def __init__(self, path: str = ":memory:") -> None:
        self._conn = sqlite3.connect(path)
        self._cur = self._conn.cursor()
        self._cur.execute(f"create table {self.TB} (authority, name, num_stars, num_forks)")
        self._conn.commit()

    def store(self, repo: Repository) -> None:
        logger.debug(f"Store {repo}")
        self._cur.execute(
            f"insert into {self.TB} values (?, ?, ?, ?)",
            repo.to_tuple(),
        )
        self._conn.commit()

    def close(self):
        logger.info("Close Storage session")
        self._conn.close()


class Crawler:
    def __init__(self, url: str, interval: int, storage: Storage) -> None:
        self._url: Optional[str] = url
        self._interval = interval
        self._storage = storage

    def crawl(self):
        while self._crawlable():
            logger.info(f"GET {self._url}")
            self._parse(self._url)
            self._wait()

        self._storage.close()

    def _parse(self, url: str) -> None:
        response = requests.get(url)
        soup = bs4.BeautifulSoup(response.text, "html.parser")
        for row in soup.find_all(class_="Box-row"):
            meta = row.find(class_="f5 color-fg-muted")

            # authority: {organization,user}
            org_tag = meta.find("a", attrs={"data-hovercard-type": "organization"})
            user_tag = meta.find("a", attrs={"data-hovercard-type": "user"})
            authority = self._authority(org_tag, user_tag)

            # repository
            repo_name = meta.find("a", attrs={"data-hovercard-type": "repository"}).text

            # star/fork
            star_tag, fork_tag = row.find_all(class_="color-fg-muted text-bold pl-3")
            num_stars = locale.atoi(star_tag.text.strip())
            num_forks = locale.atoi(fork_tag.text.strip())

            data = Repository(
                authority=authority,
                name=repo_name,
                num_stars=num_stars,
                num_forks=num_forks,
            )
            self._storage.store(data)

        next_tag = soup.find("a", class_="btn btn-outline BtnGroup-item", string="Next")
        self._after_crawl(next_tag)

    def _crawlable(self):
        return isinstance(self._url, str)

    def _wait(self):
        interval = random.random() * self._interval
        logger.debug(f"Wait {interval}s")
        time.sleep(interval)

    def _authority(self, org_tag: Any, user_tag: Any) -> str:
        if org_tag is not None:
            return org_tag.text
        elif user_tag is not None:
            return user_tag.text
        else:
            assert False, "Should not reach"

    def _after_crawl(self, next_tag: Any) -> None:
        if next_tag is None or not next_tag.has_attr("href"):
            self._url = None
            logging.info("Reach at end of cursor")
            self._url = None

        else:
            next_url = next_tag["href"]
            logging.info(f"Next: {next_url}")
            self._url = next_url


if __name__ == "__main__":
    url: str = (
        'https://github.com/optuna/optuna'
        '/network/dependents?package_id=UGFja2FnZS0xOTY2MjQ5Njg%3D'
    )
    storage = Storage(path="sample.db")
    crawler = Crawler(url=url, interval=5, storage=storage)
    crawler.crawl()

import time

import click
from sslog import logger

from app.application import Application
from app.config import load_config
from app.mt import MTeamRequestError
from app.scrape import Scrape


@click.group()
def cli() -> None:
    pass


@cli.command()
def node() -> None:
    cfg = load_config()

    app = Application.new(cfg)

    app.start()


@cli.command()
def scrape() -> None:
    cfg = load_config()

    s = Scrape(cfg)

    while True:
        try:
            s.scrape(limit=2)
            time.sleep(60)
        except MTeamRequestError as e:
            if e.message == "請求過於頻繁":
                logger.info("rate limited, sleep for 1h")
                time.sleep(3601)
                continue
            raise

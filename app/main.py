import os
import time

import click
from sslog import logger

from app.application import Application
from app.config import load_config
from app.mt import MTeamRequestError, httpx_network_errors
from app.scrape import Scrape
from app.utils import parse_obj_as


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
            s.scrape(limit=parse_obj_as(int, os.environ.get("SCRAPE_LIMIT", 100)))
            time.sleep(60)
        except httpx_network_errors:
            time.sleep(60)
            continue
        except MTeamRequestError as e:
            if e.message == "請求過於頻繁":
                logger.info("operator {!r} get rate limited, sleep for 10m", e.op)
                time.sleep(360)
                continue
            raise

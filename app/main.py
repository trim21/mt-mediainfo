import click

from app.application import Application
from app.config import load_config
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

    s.scrape()

import click

from app.config import load_downloader_config, load_scrape_config
from app.downloader import Downloader
from app.scrape import Scrape


@click.group()
def cli() -> None:
    pass


@cli.command()
def downloader() -> None:
    cfg = load_downloader_config()

    app = Downloader.new(cfg)

    app.start()


@cli.command()
def scrape() -> None:
    cfg = load_scrape_config()

    s = Scrape(cfg)
    s.start()

import click

from app.bin.downloader import Downloader
from app.bin.scrape import Scrape
from app.config import load_downloader_config, load_scrape_config, prepare_pg_ssl_key


@click.group()
def cli() -> None:
    pass


@cli.command()
def downloader() -> None:
    cfg = load_downloader_config()
    cfg = prepare_pg_ssl_key(cfg)

    app = Downloader.new(cfg)

    app.start()


@cli.command()
def scrape() -> None:
    cfg = load_scrape_config()
    cfg = prepare_pg_ssl_key(cfg)

    s = Scrape(cfg)
    s.start()

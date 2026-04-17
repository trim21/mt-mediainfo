import click

from app.config import load_node_config, load_scrape_config
from app.node import Node
from app.scrape import Scrape


@click.group()
def cli() -> None:
    pass


@cli.command()
def node() -> None:
    cfg = load_node_config()

    app = Node.new(cfg)

    app.start()


@cli.command()
def scrape() -> None:
    cfg = load_scrape_config()

    s = Scrape(cfg)
    s.start()

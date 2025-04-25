import click

from app.application import Application
from app.config import load_config


@click.command()
def cli() -> None:
    cfg = load_config()

    app = Application.new(cfg)

    app.start()

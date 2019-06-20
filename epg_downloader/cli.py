# -*- coding: utf-8 -*-

"""Console script for epg_downloader."""
import sys
import click

from .app import settings
from .epg_downloader import download_from_epg, upload_to_s3


class EPGConfig(object):
    def __init__(self):
        self.epg_host = settings.EPG_HOST
        self.epg_user = settings.EPG_USER
        self.epg_proto = settings.EPG_PROTOCOL
        self.epg_pass = settings.EPG_PASSWORD
        self.directory = settings.DIRECTORY

    def set_values(self, **kwargs):
        for k, v in kwargs.items():
            if v:
                setattr(self, k, v)


pass_epg_config = click.make_pass_decorator(EPGConfig, ensure=True)


@click.group()
@click.option('--epg-host', '-h', help="Host for EPGStation (example.com)")
@click.option('--epg-user', '-u', help="Username for EPGStation")
@click.option('--epg-proto', '-proto', help="Protocol for EPGStation (http/https)")
@click.option('--epg-pass', '-p', help="Password for EPGStation")
@click.option('--directory', '-d', help="Directory to download files")
@pass_epg_config
def main(epg_config, directory, **kwargs):
    epg_config.set_values(**kwargs)
    return 0


@click.command()
@click.option('--epg-host', '-h', help="Host for EPGStation (example.com)")
@click.option('--epg-user', '-u', help="Username for EPGStation")
@click.option('--epg-proto', '-proto', help="Protocol for EPGStation (http/https)")
@click.option('--epg-pass', '-p', help="Password for EPGStation")
@click.option('--directory', '-d', help="Directory to download files")
@pass_epg_config
def auto(epg_config, **kwargs):
    """Download from EPGStation & Upload to S3"""
    epg_config.set_values(**kwargs)
    click.echo(f"Downloading from {epg_config.epg_proto}://{epg_config.epg_host} to {epg_config.directory}")
    download_from_epg()
    upload_to_s3()
    return 0


@click.command()
@click.option('--epg-host', '-h', help="Host for EPGStation (example.com)")
@click.option('--epg-user', '-u', help="Username for EPGStation")
@click.option('--epg-proto', '-proto', help="Protocol for EPGStation (http/https)")
@click.option('--epg-pass', '-p', help="Password for EPGStation")
@click.option('--directory', '-d', help="Directory to download files")
@pass_epg_config
def download(epg_config, **kwargs):
    """Download from EPGStation to local directory"""
    epg_config.set_values(**kwargs)
    click.echo(f"Downloading from {epg_config.epg_proto}://{epg_config.epg_host} to {epg_config.directory}")
    download_from_epg()
    return 0


@click.command()
@click.option('--database', '-D', help="Database to store information")
def init(database):
    """Initialize project"""
    return 0


main.add_command(auto)
main.add_command(download)
main.add_command(auto)


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover

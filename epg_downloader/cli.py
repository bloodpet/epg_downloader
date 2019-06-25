# -*- coding: utf-8 -*-

"""Console script for epg_downloader."""
from pprint import pformat
import sys

import click
from tabulate import tabulate

from .app import settings
from .epg_downloader import (
    delete_from_epg,
    delete_from_s3,
    delete_local,
    download_all_from_epg,
    get_db_entry,
    get_entries_to_upload,
    get_info,
    list_entries,
    migrate_data,
    upload_all_to_s3,
    upload_to_s3,
)


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
    download_all_from_epg()
    click.echo(f"Uploading to S3")
    upload_all_to_s3()
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
    download_all_from_epg()
    return 0


@click.command()
@click.option('--test', '-t', is_flag=True, default=True, help="Just show files to upload")
def upload(test):
    """Upload all non-uploaded items to S3"""
    click.echo(f"Uploading to S3")
    for entry in get_entries_to_upload():
        click.echo(f"Uploading {entry['id']} {entry['filename']}")
        if not test:
            upload_to_s3(entry)
    return 0


@click.command()
@click.option('--database', '-D', help="Database to store information")
def init(database):
    """Initialize project"""
    return 0


@click.command()
@click.option('--status', '-s', default='all', help="Filter the list based on status (all, downloading, downloaded, uploading, uploaded)")
@click.option('--fields', '--field', '-f', multiple=True, default=['name'], help="Show given fields")
@click.option('--show-status/--hide-status', '-ss/-hs', is_flag=True, default=True, help="Show/Hide status")
def ls(status, fields, show_status):
    """List all downloads/uploads"""
    click.echo(tabulate(list_entries(status, fields, show_status), headers='keys'))
    return 0


@click.command()
@click.argument('entry_id')
def info(entry_id):
    click.echo(pformat(get_info(entry_id)))


@click.command()
def migrate():
    click.echo('Migrating data')
    migrate_data()
    click.echo('Done')


@click.command()
@click.argument('entry_ids', nargs=-1)
@click.option('--force', '-f', default=False, is_flag=True, help="Force delete regardless of status")
@click.option('--epg', '--epgstation', '-e', default=False, is_flag=True, help="Delete on EPGStation")
@click.option('--s3', '--spaces', '-s', default=False, is_flag=True, help="Delete on AWS S3 / DO Spaces")
def delete(entry_ids, force, epg, s3):
    for entry_id in entry_ids:
        entry = get_db_entry(entry_id)
        filename = entry['filename']
        if force:
            confirm = True
        else:
            confirm = click.confirm(f"Do you really want to delete {filename} locally?")
        if confirm:
            click.echo(f"Deleting {filename}")
            delete_local(entry=entry)
        if epg and (force or entry['epg_status'] == 'downloaded'):
            if force:
                confirm = True
            else:
                confirm = click.confirm(f'Do you really want to delete {filename} from EPGStation?')
            if confirm:
                click.echo(f"Deleting {entry['id']} {filename} from EPGStation")
                delete_from_epg(entry=entry)
        if s3 and (force or entry['s3_status'] == 'uploaded'):
            if force:
                confirm = True
            else:
                confirm = click.confirm(f'Do you really want to delete {filename} from S3 / Spaces?')
            if confirm:
                click.echo(f"Deleting {entry['id']} {filename} from S3 / Spaces")
                delete_from_s3(entry=entry)


main.add_command(auto)
main.add_command(delete)
main.add_command(delete, name='del')
main.add_command(delete, name='rm')
main.add_command(download)
main.add_command(info)
main.add_command(ls)
main.add_command(ls, name='list')
main.add_command(migrate)
main.add_command(upload)


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover

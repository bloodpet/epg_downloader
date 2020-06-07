# -*- coding: utf-8 -*-

"""Console script for epg_downloader."""
from pathlib import Path
from pprint import pformat
import sys

import click
from tabulate import tabulate

from .app import settings
from .clients import S3
from .epg_downloader import (
    check_dl,
    check_ul,
    delete_from_epg,
    delete_from_s3,
    delete_local,
    download_all_from_epg,
    download_one_from_epg,
    epg_to_s3_all,
    gen_html,
    get_db_entry,
    get_entries_to_download,
    get_entries_to_upload,
    get_entry,
    get_free_space,
    get_info,
    list_entries,
    migrate_data,
    update_from_epg,
    upload_all_to_s3,
    upload_one,
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
@click.option("--epg-host", "-h", help="Host for EPGStation (example.com)")
@click.option("--epg-user", "-u", help="Username for EPGStation")
@click.option("--epg-proto", "-proto", help="Protocol for EPGStation (http/https)")
@click.option("--epg-pass", "-p", help="Password for EPGStation")
@click.option("--directory", "-d", help="Directory to download files")
@pass_epg_config
def main(epg_config, directory, **kwargs):
    epg_config.set_values(**kwargs)
    return 0


@click.command()
@click.option("--epg-host", "-h", help="Host for EPGStation (example.com)")
@click.option("--epg-user", "-u", help="Username for EPGStation")
@click.option("--epg-proto", "-proto", help="Protocol for EPGStation (http/https)")
@click.option("--epg-pass", "-p", help="Password for EPGStation")
@click.option("--directory", "-d", help="Directory to download files")
@pass_epg_config
def auto_old(epg_config, **kwargs):
    """Download from EPGStation & Upload to S3"""
    epg_config.set_values(**kwargs)
    click.echo(
        f"Downloading from {epg_config.epg_proto}://{epg_config.epg_host} to {epg_config.directory}"
    )
    download_all_from_epg()
    click.echo(f"Uploading to S3")
    upload_all_to_s3()
    return 0


@click.command()
@click.argument("entry_ids", nargs=-1)
@click.option(
    "--dl",
    "--epg",
    "--epgstation",
    "-e",
    default=False,
    is_flag=True,
    help="Check download from EPGStation",
)
@click.option(
    "--ul",
    "--s3",
    "--spaces",
    "-s",
    default=False,
    is_flag=True,
    help="Check upload to AWS S3 / DO Spaces",
)
def check(entry_ids, dl, ul):
    if not dl and not ul:
        dl = True
    for entry_id in entry_ids:
        if dl:
            msg = "Good" if check_dl(entry_id) else "Bad"
            click.echo(f"{entry_id} dl: {msg}")
        if ul:
            msg = "Good" if check_ul(entry_id) else "Bad"
            click.echo(f"{entry_id} ul: {msg}")


@click.command()
@click.argument("entry_ids", nargs=-1)
@pass_epg_config
def download(epg_config, entry_ids, **kwargs):
    for entry_id in entry_ids:
        download_one_from_epg(entry_id)


@click.command()
@click.option("--epg-host", "-h", help="Host for EPGStation (example.com)")
@click.option("--epg-user", "-u", help="Username for EPGStation")
@click.option("--epg-proto", "-proto", help="Protocol for EPGStation (http/https)")
@click.option("--epg-pass", "-p", help="Password for EPGStation")
@click.option("--directory", "-d", help="Directory to download files")
@pass_epg_config
def download_all(epg_config, **kwargs):
    """Download from EPGStation to local directory"""
    epg_config.set_values(**kwargs)
    click.echo(
        f"Downloading from {epg_config.epg_proto}://{epg_config.epg_host} to {epg_config.directory}"
    )
    download_all_from_epg()
    return 0


@click.command()
@pass_epg_config
def pending(epg_config):
    """Get urls for download"""
    update_from_epg()
    for data in get_entries_to_download():
        click.echo(data)


@click.command()
@click.option(
    "--test", "-t", is_flag=True, default=False, help="Just show files to upload"
)
@click.argument("entry_ids", nargs=-1)
def upload(test, entry_ids):
    """Upload all non-uploaded items to S3"""
    click.echo(f"Uploading to S3")
    for entry_id in entry_ids:
        entry = get_entry(entry_id)
        click.echo(f"Uploading {entry['id']} {entry['filename']}")
        if not test:
            upload_to_s3(entry)
    return 0


@click.command()
@click.option(
    "--test", "-t", is_flag=True, default=False, help="Just show files to upload"
)
def upload_all(test):
    """Upload all non-uploaded items to S3"""
    click.echo(f"Uploading to S3")
    for entry in get_entries_to_upload():
        click.echo(f"Uploading {entry['id']} {entry['filename']}")
        if not test:
            upload_to_s3(entry)
    return 0


@click.command()
@click.option("--database", "-D", help="Database to store information")
def init(database):
    """Initialize project"""
    return 0


@click.command()
@click.option(
    "--status",
    "-s",
    default="all",
    help="Filter the list based on status (all, downloading, downloaded, uploading, uploaded)",
)
@click.option(
    "--fields",
    "--field",
    "-f",
    multiple=True,
    default=["name"],
    help="Show given fields",
)
@click.option(
    "--show-status/--hide-status",
    "-ss/-hs",
    is_flag=True,
    default=True,
    help="Show/Hide status",
)
def ls(status, fields, show_status):
    """List all downloads/uploads"""
    click.echo(tabulate(list_entries(status, fields, show_status), headers="keys"))
    return 0


@click.command()
def generate_html():
    gen_html()


@click.command()
@click.argument("entry_ids", nargs=-1)
def info(entry_ids):
    for entry_id in entry_ids:
        click.echo(pformat(get_info(entry_id)))


@click.command()
def migrate():
    click.echo("Migrating data")
    migrate_data()
    click.echo("Done")


@click.command()
@click.argument("entry_ids", nargs=-1)
@click.option(
    "--force",
    "-f",
    default=False,
    is_flag=True,
    help="Force delete regardless of status",
)
@click.option(
    "--epg",
    "--epgstation",
    "-e",
    default=False,
    is_flag=True,
    help="Delete on EPGStation",
)
@click.option(
    "--s3",
    "--spaces",
    "-s",
    default=False,
    is_flag=True,
    help="Delete on AWS S3 / DO Spaces",
)
def delete(entry_ids, force, epg, s3):
    for entry_id in entry_ids:
        entry = get_db_entry(entry_id)
        filename = entry["filename"]
        if force:
            confirm = True
        else:
            confirm = click.confirm(f"Do you really want to delete {filename} locally?")
        if confirm:
            click.echo(f"Deleting {filename}")
            delete_local(entry=entry)
        if epg and (force or entry["epg_status"] == "downloaded"):
            if force:
                confirm = True
            else:
                confirm = click.confirm(
                    f"Do you really want to delete {filename} from EPGStation?"
                )
            if confirm:
                click.echo(f"Deleting {entry['id']} {filename} from EPGStation")
                delete_from_epg(entry=entry, force=force)
        if s3 and (force or entry["s3_status"] == "uploaded"):
            if force:
                confirm = True
            else:
                confirm = click.confirm(
                    f"Do you really want to delete {filename} from S3 / Spaces?"
                )
            if confirm:
                click.echo(f"Deleting {entry['id']} {filename} from S3 / Spaces")
                delete_from_s3(entry=entry)


@click.command()
def show_free():
    click.echo(get_free_space())


@click.command()
@click.argument("entry_ids", nargs=-1)
@click.option(
    "--force",
    "-f",
    default=False,
    is_flag=True,
    help="Force delete regardless of status",
)
def pipeline(entry_ids, force):
    for entry_id in entry_ids:
        click.echo(f"Downloading {entry_id}")
        download_one_from_epg(entry_id)
        click.echo(f"Download verified {entry_id}")
        click.echo(f"Uploading {entry_id}")
        upload_one(entry_id)
        click.echo(f"Upload verified {entry_id}")
        if force:
            confirm = True
        else:
            confirm = click.confirm(f"Do you really want to delete {entry_id} locally?")
        if confirm:
            click.echo(f"Deleting {entry_id}")
            delete_local(entry_id=entry_id)
        if force:
            confirm = True
        else:
            confirm = click.confirm(
                f"Do you really want to delete {entry_id} from EPGStation?"
            )
        if confirm:
            click.echo(f"Deleting {entry_id} from EPGStation")
            delete_from_epg(entry_id=entry_id)


@click.command()
def upload_json():
    s3 = S3()
    #for path in Path(".").glob("*.json"):
    #    s3.upload(path.name, {"ContentType": "application/json"})
    for path in Path(".").glob("*.log"):
        s3.upload(path.name, {"ContentType": "text/plain; charset=utf-8"})


@click.command()
@click.argument("entry_ids", nargs=-1)
def get_crc(entry_ids):
    import os
    import zlib
    from .utils import calculate_multipart_etag

    s3 = S3()
    for entry_id in entry_ids:
        entry = get_info(entry_id)
        filename = entry["filename"]
        log_file = f"{filename}.log"
        if Path(log_file).is_file():
            click.echo(f"Skip {entry_id}: log exists")
            continue
        click.echo(f"Downloading {entry_id}")
        resp = s3.download(filename)
        click.echo(f"Check etag for {entry_id}")
        etag = calculate_multipart_etag(filename)
        if etag not in resp["ETag"]:
            click.echo(f"Error downloading {entry_id}")
            continue
        click.echo(f"Generate CRC for {entry_id}")
        with open(filename, "rb") as fp:
            crc32 = zlib.crc32(fp.read())
        crc_str = hex(crc32)[2:]
        with open(log_file, "w") as fp:
            fp.write(f"crc32: {crc_str}")
        s3.upload(log_file, {"ContentType": "text/plain; charset=utf-8"})
        # Delete file
        click.echo(f"Deleting {filename}")
        os.unlink(filename)

    click.echo("Done")


@click.command()
@click.argument("entry_ids", nargs=-1)
def auto_all(entry_ids):
    epg_to_s3_all()


main.add_command(get_crc, name="get-crc")
main.add_command(auto_all, name="auto")
main.add_command(check)
main.add_command(delete)
main.add_command(delete, name="del")
main.add_command(delete, name="rm")
main.add_command(download)
main.add_command(download, name="dl")
main.add_command(download_all)
main.add_command(generate_html, name="generate")
main.add_command(pending)
main.add_command(pipeline)
main.add_command(info)
main.add_command(ls)
main.add_command(ls, name="list")
main.add_command(migrate)
main.add_command(upload)
main.add_command(upload_all)
main.add_command(upload_json)
main.add_command(upload_json, name="upload-json")
main.add_command(show_free)
main.add_command(show_free, name="free")


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover

import json
from logzero import logger as log
import os
from pathlib import Path
from pymediainfo import MediaInfo

from .app import kv_store
from .clients import S3
from .utils import (
    check_crc,
    check_etag,
    epg_request,
    epg_retrieve,
    get_cdn_url,
    get_datetime,
    get_db_entries,
    get_db_entry,
    get_db_key,
    get_epg_entries,
    get_epg_file_url,
    get_epg_index_url,
    get_epg_info_url,
    get_epg_list_url,
    get_epg_free,
    get_s3_origin_url,
    download_file,
)

Path


def check_dl(identifier):
    entry = get_entry(identifier)
    log.info(entry['filename'], entry["id"])
    is_valid = check_crc(entry['filename'], entry["id"])
    if is_valid:
        entry["epg_status"] = "downloaded"
        entry["local_status"] = "downloaded"
    else:
        entry["epg_status"] = "downloading_error"
    kv_store[entry["db_key"]] = entry
    return is_valid


def check_ul(identifier):
    entry = get_entry(identifier)
    is_valid = check_etag(entry['filename'], entry["web_origin_url"])
    if is_valid:
        entry["s3_status"] = "uploaded"
        entry["local_status"] = "uploaded"
    else:
        entry["s3_status"] = "upload_error"
    kv_store[entry["db_key"]] = entry
    return is_valid


def get_entries_to_download():
    url = get_epg_list_url()
    response = epg_retrieve(url)
    for entry in get_epg_entries(response.json()):
        filename = entry["filename"]
        json_filename = f"{filename}.json"
        entry["json_file"] = json_filename
        db_key = get_db_key(entry["id"])
        if db_key in kv_store.keys():
            log.info(f"Skipping download of {filename}")
            continue
        yield entry


def update_from_epg(**kwargs):
    for entry in get_entries_to_download():
        db_key = entry['db_key']
        entry["epg_status"] = "-"
        kv_store[db_key] = entry


def download_all_from_epg(force=True, **kwargs):
    for entry in get_entries_to_download():
        download_from_epg(entry)
        try:
            create_mediainfo(entry=entry)
        except Exception:
            log.error("Failed creating mediainfo", exc_info=True)
        else:
            delete_from_epg(entry=entry, force=force)


def download_from_epg(entry, **kwargs):
    db_key = entry['db_key']
    filename = entry["filename"]
    json_filename = f"{filename}.json"
    log.info(f"Downloading {db_key}: {filename}")
    entry["epg_status"] = "downloading"
    kv_store[db_key] = entry
    try:
        download_file(entry["epg_file_url"], filename)
    except Exception:
        log.error(f"Failed to download {db_key}: {filename}", exc_info=True)
        entry["epg_status"] = "downloading_error"
        kv_store[db_key] = entry
        raise
    if os.path.getsize(filename) != entry["filesize"]:
        entry["epg_status"] = "downloading_error"
        kv_store[db_key] = entry
        log.warn(f"Failed download: {db_key}: {filename}")
        raise ValueError("filesize does not match")
    if not check_crc(filename, entry["id"]):
        entry["epg_status"] = "downloading_error"
        kv_store[db_key] = entry
        log.warn(f"Failed download: {db_key}: {filename}")
        raise ValueError("crc does not match")
    with open(json_filename, "w") as fp:
        json.dump(entry, fp, indent=True, ensure_ascii=False)
    entry["epg_status"] = "downloaded"
    entry["downloaded_on"] = get_datetime()
    kv_store[db_key] = entry
    log.info(f"Success download: {db_key}: {filename}")


def download_one_from_epg(identifier):
    try:
        key = get_db_key(int(identifier))
    except ValueError:
        key = identifier
    entry = kv_store[key]
    return download_from_epg(entry)


def get_entries_to_upload(force=False):
    for entry in get_db_entries():
        filename = entry["filename"]
        if entry["epg_status"] != "downloaded" and entry.get("local_status") != "downloaded":
            log.info('Skip upload of {filename}. Already downloaded')
            continue
        if entry.get("local_status") == "deleted":
            log.info('Skip upload of {filename}. Local file deleted')
            continue
        if not force and entry.get("s3_status") == "uploaded":
            log.info(f"Skipping upload of {filename}. Already uploaded")
            continue
        yield entry


def upload_all_to_s3(force=False, **kwargs):
    for entry in get_entries_to_upload(force):
        upload_to_s3(entry)


def upload_one(entry_id):
    entry = get_entry(entry_id)
    upload_to_s3(entry)


def upload_to_s3(entry, force=False):
    s3 = S3()
    db_key = entry["db_key"]
    filename = entry["filename"]
    log.info(f"Uploading {db_key}: {filename} to S3")
    entry["s3_key"] = s3.get_key(filename)
    entry["s3_status"] = "uploading"
    kv_store[db_key] = entry
    log.info(f"Uploading {filename}")
    json_file = f"{filename}.json"
    log_file = f"{filename}.log"
    mediainfo_file = f'{entry["filename"]}.mediainfo.json'
    try:
        s3.upload(json_file, {"ContentType": "application/json"})
    except Exception:
        pass
    try:
        s3.upload(log_file, {"ContentType": "text/plain; charset=utf-8"})
    except Exception:
        pass
    try:
        s3.upload(mediainfo_file, {"ContentType": "application/json"})
    except Exception:
        pass
    try:
        s3.upload(filename)
    except Exception:
        log.error(f"Failed to upload {db_key}: {filename}", exc_info=True)
        entry["s3_status"] = "upload_error"
        kv_store[db_key] = entry
        raise

    entry["web_origin_url"] = get_s3_origin_url(entry)
    entry["web_cdn_url"] = get_cdn_url(entry)
    if not check_etag(entry['filename'], entry["web_origin_url"]):
        log.error(f"Failed to upload {db_key}: {filename}", exc_info=True)
        entry["s3_status"] = "upload_error"
        kv_store[db_key] = entry
        raise ValueError(f"{db_key}: E-Tag does not match.")
        return

    entry["s3_status"] = "uploaded"
    entry["local_status"] = "uploaded"
    entry["uploaded_on"] = get_datetime()
    kv_store[db_key] = entry


def list_entries(status="all", fields=None, show_status=True, **kwargs):
    if fields is None:
        fields = ["name"]
    for entry in get_db_entries(sort=True):
        if status != "all" and entry.get("epg_status") != status and entry.get("s3_status") != status:
            continue
        shown_entry = {"id": entry["id"]}
        if show_status:
            shown_entry.update(
                {
                    "epg": entry["epg_status"],
                    "s3": entry.get("s3_status", "-"),
                    "local": entry.get("local_status", "-"),
                }
            )
        for field in fields:
            if field == "size":
                size = int(entry["filesize"]) / (1024 * 1024 * 1024)
                shown_entry[field] = f"{size:.2f}"
            else:
                shown_entry[field] = entry.get(field, "-")
        yield shown_entry


def get_info(identifier):
    entry = get_entry(identifier)
    # Fix for version 0.3
    has_changed = False
    if "s3_key" in entry:
        if "web_origin_url" not in entry:
            entry["web_origin_url"] = get_s3_origin_url(entry)
            has_changed = True
        if "web_cdn_url" not in entry:
            entry["web_cdn_url"] = get_cdn_url(entry)
            has_changed = True
    if has_changed:
        kv_store[entry["db_key"]] = entry
    return entry


def migrate_data():
    for entry in get_db_entries():
        entry_id = entry["id"]
        keys = entry.keys()
        if entry["epg_status"] == "uploaded":
            entry["epg_status"] = "downloaded"
            entry["local_status"] = "uploaded"
            entry["s3_status"] = "uploaded"
        if entry.get("s3_status") == "uploaded":
            entry["local_status"] = "uploaded"
            if "web_origin_url" not in keys:
                entry["web_origin_url"] = get_s3_origin_url(entry)
            if "web_cdn_url" not in keys:
                entry["web_cdn_url"] = get_cdn_url(entry)
        if "epg_file_url" not in keys:
            entry["epg_file_url"] = get_epg_file_url(entry_id)
        if "epg_index_url" not in keys:
            entry["epg_index_url"] = get_epg_index_url(entry_id)
        if "db_key" not in keys:
            entry["db_key"] = entry["epg_key"]
        kv_store[entry["epg_key"]] = entry


def delete_local(*, entry=None, entry_id=None):
    if entry is None:
        entry = get_db_entry(entry_id)
    db_key = entry["db_key"]
    if entry.get("local_status") != "deleted":
        try:
            os.unlink(entry["filename"])
        except FileNotFoundError:
            pass
    entry["local_status"] = "deleted"
    kv_store[db_key] = entry


def delete_from_epg(*, entry=None, entry_id=None, force=False):
    if entry is None:
        entry = get_db_entry(entry_id)
    db_key = entry["db_key"]
    if force or entry["epg_status"] != "deleted":
        epg_request(get_epg_info_url(entry["id"]), "DELETE")
    else:
        log.info(f"Skipped {entry['id']}")
    entry["epg_status"] = "deleted"
    kv_store[db_key] = entry


def delete_from_s3(*, entry=None, entry_id=None, force=False):
    if entry is None:
        entry = get_db_entry(entry_id)
    db_key = entry["db_key"]
    s3 = S3()
    if force or entry["s3_status"] != "uploaded":
        s3.delete(entry["s3_key"])
    entry["s3_status"] = "deleted"
    kv_store[db_key] = entry


def get_entry(identifier):
    try:
        key = get_db_key(int(identifier))
    except ValueError:
        key = identifier
    return kv_store[key]


def get_free_space():
    data = get_epg_free()
    free_gb = data["free"] / (1024 * 1024 * 1024)
    total_gb = data["total"] / (1024 * 1024 * 1024)
    percent = 100 * free_gb / total_gb
    return f"Free: {percent:.2f}%  {free_gb:.2f}/{total_gb:.2f} GB"


def gen_html():
    fields = [
        "filename",
        "name",
        "size",
        "web_cdn_url",
        "web_origin_url",
    ]
    content = '<html>\n<meta charset="utf-8">\n<ul>'
    for entry in list_entries(status="uploaded", fields=fields):
        mediainfo_file = "{filename}.mediainfo.json".format(**entry)
        if Path(mediainfo_file).is_file():
            content += """<li>
                <a href="{web_cdn_url}">{name}</a>:&nbsp;
                <a href="{web_cdn_url}.json">details</a>&nbsp;|&nbsp;
                <a href="{web_cdn_url}.log">log/crc</a>&nbsp;|&nbsp;
                <a href="{web_cdn_url}.mediainfo.json">MediaInfo</a>
                Size: {size}GB,
                Filename: {filename}
            </li>
            """.format(
                **entry
            )
        else:
            content += """<li>
                <a href="{web_cdn_url}">{name}</a>:&nbsp;
                <a href="{web_cdn_url}.json">details</a>&nbsp;|&nbsp;
                <a href="{web_cdn_url}.log">log/crc</a>
                Size: {size}GB,
                Filename: {filename}
            </li>
            """.format(
                **entry
            )
    content += "</ul>\n</html>"
    log.debug("Uploading html")
    with open("uploads.html", "w") as fp:
        fp.write(content)
    s3 = S3()
    s3.upload("uploads.html", {"ContentType": "text/html"})
    log.debug("Uploaded html")


def create_mediainfo(entry=None, entry_id=None):
    if entry is None:
        entry = get_entry(entry_id)
    filename = entry["filename"]
    info = MediaInfo.parse(filename)
    mediainfo_file = f"{filename}.mediainfo.json"
    with open(mediainfo_file, "w") as fp:
        fp.write(info.to_json())


def upload_mediainfo(entry=None, entry_id=None):
    s3 = S3()
    if entry is None:
        entry = get_entry(entry_id)
    mediainfo_file = f'{entry["filename"]}.mediainfo.json'
    try:
        s3.upload(mediainfo_file, {"ContentType": "application/json"})
    except Exception:
        pass


def epg_to_s3_all():
    dl_cnt = 0
    for entry in get_entries_to_download():
        try:
            epg_to_s3(entry)
        except Exception:
            log.error("Failed downloading or uploading", exc_info=True)
            continue
        dl_cnt += 1
    if dl_cnt:
        log.info(f"Downloaded {dl_cnt} files")
        # Generate HTML
        gen_html()


def epg_to_s3(entry, force=True):
    download_from_epg(entry)
    try:
        create_mediainfo(entry=entry)
    except Exception:
        log.error("Failed creating mediainfo", exc_info=True)
    upload_to_s3(entry)
    delete_local(entry=entry)
    delete_from_epg(entry=entry, force=force)

import json
import logging
import os

from .app import kv_store
from .clients import S3
from .utils import (
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
    get_epg_list_url,
    get_s3_origin_url,
    download_file,
)


log = logging.getLogger(__name__)


def download_all_from_epg(**kwargs):
    url = get_epg_list_url()
    response = epg_retrieve(url)
    for entry in get_epg_entries(response.json()):
        filename = entry['filename']
        json_filename = f'{filename}.json'
        entry['json_file'] = json_filename
        db_key = get_db_key(entry['id'])
        if db_key in kv_store.keys():
            log.info(f'Skipping download of {filename}')
            continue
        entry['epg_status'] = 'downloading'
        kv_store[db_key] = entry
        try:
            download_file(entry['epg_file_url'], filename)
        except Exception:
            log.error(f'Failed to download {db_key}: {filename}', exc_info=True)
            entry['epg_status'] = 'downloading_error'
            kv_store[db_key] = entry
        with open(json_filename, 'w') as fp:
            json.dump(entry, fp, indent=True, ensure_ascii=False)
        entry['epg_status'] = 'downloaded'
        entry['downloaded_on'] = get_datetime()
        kv_store[db_key] = entry


def upload_all_to_s3(force=False, **kwargs):
    s3 = S3()
    for entry in get_db_entries():
        db_key = get_db_key(entry['id'])
        filename = entry['filename']
        if entry['epg_status'] != 'downloaded' and not force and entry.get('s3_status') != 'uploaded':
            log.info(f'Skipping upload of {filename}')
            continue
        entry['s3_key'] = s3.get_key(filename)
        entry['s3_status'] = 'uploading'
        kv_store[db_key] = entry
        try:
            s3.upload(filename)
        except Exception:
            log.error(f'Failed to upload {db_key}: {filename}', exc_info=True)
            entry['s3_status'] = 'uploading_error'
            kv_store[db_key] = entry
        entry['s3_status'] = 'uploaded'
        entry['uploaded_on'] = get_datetime()
        entry['web_origin_url'] = get_s3_origin_url(entry)
        entry['web_cdn_url'] = get_cdn_url(entry)
        kv_store[db_key] = entry


def list_entries(status='all', fields=None, show_status=True, **kwargs):
    if fields is None:
        fields = ['name']
    for entry in get_db_entries(sort=True):
        if status != 'all' and entry['epg_status'] != status:
            continue
        shown_entry = {
            'id': entry['id'],
        }
        if show_status:
            shown_entry.update({
                'epg': entry['epg_status'],
                's3': entry.get('s3_status', '-'),
                'local': entry.get('local_status', '-'),
            })
        for field in fields:
            shown_entry[field] = entry[field]
        yield shown_entry


def get_info(identifier):
    try:
        key = get_db_key(int(identifier))
    except ValueError:
        key = identifier
    entry = kv_store[key]
    # Fix for version 0.3
    has_changed = False
    if 'web_origin_url' not in entry.keys():
        entry['web_origin_url'] = get_s3_origin_url(entry)
        has_changed = True
    if 'web_cdn_url' not in entry.keys():
        entry['web_cdn_url'] = get_cdn_url(entry)
        has_changed = True
    if has_changed:
        kv_store[key] = entry
    return entry


def migrate_data():
    for entry in get_db_entries():
        entry_id = entry['id']
        keys = entry.keys()
        if entry['epg_status'] == 'uploaded':
            entry['epg_status'] = 'downloaded'
            entry['s3_status'] = 'uploaded'
        if entry.get('s3_status') == 'uploaded':
            if 'web_origin_url' not in keys:
                entry['web_origin_url'] = get_s3_origin_url(entry)
            if 'web_cdn_url' not in keys:
                entry['web_cdn_url'] = get_cdn_url(entry)
        if 'epg_file_url' not in keys:
            entry['epg_file_url'] = get_epg_file_url(entry_id)
        if 'epg_index_url' not in keys:
            entry['epg_index_url'] = get_epg_index_url(entry_id)
        if 'db_key' not in keys:
            entry['db_key'] = entry['epg_key']
        kv_store[entry['epg_key']] = entry


def delete_local(*, entry=None, entry_id=None):
    if entry is None:
        entry = get_db_entry(entry_id)
    db_key = entry['db_key']
    if entry.get('local_status') != 'deleted':
        try:
            os.unlink(entry['filename'])
        except FileNotFoundError:
            pass
    entry['local_status'] = 'deleted'
    kv_store[db_key] = entry


def delete_from_epg(*, entry=None, entry_id=None, force=False):
    if entry is None:
        entry = get_db_entry(entry_id)
    db_key = entry['db_key']
    if force or entry['epg_status'] != 'deleted':
        epg_request(entry['epg_index_url'], 'DELETE')
    entry['epg_status'] = 'deleted'
    kv_store[db_key] = entry


def delete_from_s3(*, entry=None, entry_id=None, force=False):
    if entry is None:
        entry = get_db_entry(entry_id)
    db_key = entry['db_key']
    s3 = S3()
    if force or entry['s3_status'] != 'uploaded':
        s3.delete(entry['s3_key'])
    entry['s3_status'] = 'deleted'
    kv_store[db_key] = entry

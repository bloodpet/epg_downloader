import json
import logging

from .app import kv_store
from .clients import S3
from .utils import (
    get_datetime,
    get_entries,
    get_list_url,
    get_local_key,
    get_s3_origin_url,
    retrieve,
    download_file,
)


log = logging.getLogger(__name__)


def download_from_epg(**kwargs):
    url = get_list_url()
    response = retrieve(url)
    for entry in get_entries(response.json()):
        filename = entry['filename']
        json_filename = f'{filename}.json'
        entry['json_file'] = json_filename
        epg_key = get_local_key(entry['id'])
        if epg_key in kv_store.keys():
            log.info(f'Skipping download of {filename}')
            continue
        entry['epg_status'] = 'downloading'
        kv_store[epg_key] = entry
        try:
            download_file(entry['epg_url'], filename)
        except Exception:
            log.error(f'Failed to download {epg_key}: {filename}', exc_info=True)
            entry['epg_status'] = 'downloading_error'
            kv_store[epg_key] = entry
        with open(json_filename, 'w') as fp:
            json.dump(entry, fp, indent=True, ensure_ascii=False)
        entry['epg_status'] = 'downloaded'
        entry['downloaded_on'] = get_datetime()
        kv_store[epg_key] = entry


def upload_to_s3(**kwargs):
    s3 = S3()
    for entry in kv_store[kv_store.key.startswith('epgd')]:
        epg_key = get_local_key(entry['id'])
        filename = entry['filename']
        if entry['epg_status'] != 'downloaded':
            log.info(f'Skipping upload of {filename}')
            continue
        entry['s3_key'] = s3.get_key(filename)
        entry['epg_status'] = 'uploading'
        kv_store[epg_key] = entry
        try:
            s3.upload(filename)
        except Exception:
            log.error(f'Failed to upload {epg_key}: {filename}', exc_info=True)
            entry['epg_status'] = 'uploading_error'
            kv_store[epg_key] = entry
        entry['epg_status'] = 'uploaded'
        entry['uploaded_on'] = get_datetime()
        kv_store[epg_key] = entry


def list_entries(status='all', **kwargs):
    for entry in kv_store[kv_store.key.startswith('epgd')]:
        if status != 'all' and entry['epg_status'] != status:
            continue
        yield {
            'id': entry['id'],
            'filename': entry['filename'],
            'epg_status': entry['epg_status'],
        }


def get_info(identifier):
    try:
        key = get_local_key(int(identifier))
    except ValueError:
        key = identifier
    entry = kv_store[key]
    entry['s3_url'] = get_s3_origin_url(entry)
    return entry


def list_downloaded(*, entry_id, key, **kwargs):
    pass


def initialize(path):
    pass

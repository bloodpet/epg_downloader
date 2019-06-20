import json
import logging

from .app import kv_store
from .clients import S3
from .utils import (
    get_entries,
    get_list_url,
    get_local_key,
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
        kv_store[epg_key] = entry


def upload_to_s3(**kwargs):
    s3 = S3()
    for entry in kv_store[kv_store.key.startswith('epg')]:
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
        kv_store[epg_key] = entry


def initialize(path):
    pass

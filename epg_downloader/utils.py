from datetime import datetime
from dateutil.tz import tzutc
import logging
from urllib.parse import unquote_plus, quote

import requests

from .app import kv_store, settings

log = logging.getLogger(__name__)


def download_file(url, filename):
    # got from https://stackoverflow.com/a/16696317
    log.info(f'Downloading {filename}')
    with epg_retrieve(url, stream=True) as r:
        r.raise_for_status()
        with open(filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:  # filter out keep-alive new chunks
                    f.write(chunk)
    return filename


def get_datetime():
    return datetime.now(tz=tzutc).isoformat()


def epg_request(url, method='GET', **kwargs):
    kwargs['auth'] = requests.auth.HTTPBasicAuth(settings.EPG_USER, settings.EPG_PASSWORD)
    return requests.request(method, url, **kwargs)


def epg_retrieve(url, **kwargs):
    return epg_request(url, **kwargs)


def get_epg_list_url():
    return '{}://{}/api/recorded/'.format(
        settings.EPG_PROTOCOL,
        settings.EPG_HOST,
    )


def get_epg_file_url(entry_id):
    return '{}://{}/api/recorded/{}/file'.format(
        settings.EPG_PROTOCOL,
        settings.EPG_HOST,
        entry_id,
    )


def get_epg_index_url(entry_id):
    return '{}://{}/api/recorded/{}/file'.format(
        settings.EPG_PROTOCOL,
        settings.EPG_HOST,
        entry_id,
    )


def get_epg_info_url(entry_id):
    return '{}://{}/api/recorded/{}/'.format(
        settings.EPG_PROTOCOL,
        settings.EPG_HOST,
        entry_id,
    )


def get_epg_entries(data):
    for entry in data['recorded']:
        entry_id = entry['id']
        filename = unquote_plus(entry['filename'])
        if not entry['recording']:
            entry['db_key'] = get_db_key(entry_id)
            entry['filename'] = filename
            entry['epg_file_url'] = get_epg_file_url(entry_id)
            entry['epg_index_url'] = get_epg_index_url(entry_id)
            yield entry
        else:
            log.warn(f'Skipping {filename}')


def get_s3_origin_url(entry):
    return f"{settings.AWS_S3_ENDPOINT_URL}/{quote(entry['s3_key'])}"


def get_cdn_url(entry):
    return f"{settings.CDN_ENDPOINT_URL}/{quote(entry['s3_key'])}"


def get_db_key(entry_id):
    return f'{settings.KEY_PREFIX}_{entry_id}'


def get_db_entries(sort=False):
    if not sort:
        return kv_store[kv_store.key.startswith(settings.KEY_PREFIX)]
    for key in sorted(list(kv_store.keys())):
        if key.startswith(settings.KEY_PREFIX):
            yield kv_store[key]


def check_in_local_key(key):
    return key in kv_store.keys()


def get_db_entry(entry_id):
    return kv_store[get_db_key(entry_id)]

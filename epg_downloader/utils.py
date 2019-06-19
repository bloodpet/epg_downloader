import logging
from urllib.parse import unquote_plus

import requests

from .app import settings

log = logging.getLogger(__name__)


def download_file(url, filename):
    # got from https://stackoverflow.com/a/16696317
    log.info(f'Downloading {filename}')
    with retrieve(url, stream=True) as r:
        r.raise_for_status()
        with open(filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:  # filter out keep-alive new chunks
                    f.write(chunk)
    return filename


def retrieve(url, **kwargs):
    kwargs['auth'] = requests.auth.HTTPBasicAuth(settings.EPG_USER, settings.EPG_PASSWORD)
    return requests.get(url, **kwargs)


def get_list_url():
    return '{}://{}/api/recorded/'.format(
        settings.EPG_PROTOCOL,
        settings.EPG_HOST,
    )


def get_epg_url(entry_id):
    return '{}://{}/api/recorded/{}/file'.format(
        settings.EPG_PROTOCOL,
        settings.EPG_HOST,
        entry_id,
    )


def get_entries(data):
    for entry in data['recorded']:
        filename = unquote_plus(entry['filename'])
        if not entry['recording']:
            entry['filename'] = filename
            entry['epg_url'] = get_epg_url(entry['id']),
            yield entry
        else:
            log.warn(f'Skipping {filename}')

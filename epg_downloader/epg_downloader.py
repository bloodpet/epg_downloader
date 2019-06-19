import json
import logging
import os

from .utils import (
    get_entries,
    get_list_url,
    retrieve,
    download_file,
)


log = logging.getLogger(__name__)


def download_from_epg(**kwargs):
    url = get_list_url()
    response = retrieve(url)
    file_list = os.listdir()
    for entry in get_entries(response.json()):
        filename = entry['filename']
        json_filename = f'{filename}.json'
        if json_filename in file_list:
            log.warn(f'Skipping {filename}')
            continue
        download_file(entry['epg_url'], filename)
        with open(json_filename, 'w') as fp:
            json.dump(entry, fp, indent=True, ensure_ascii=False)


def upload_to_s3(**kwargs):
    pass


def initialize(path):
    pass

from datetime import datetime
import hashlib
import logging
from urllib.parse import unquote_plus, quote
import requests
import zlib

from .app import kv_store, settings

log = logging.getLogger(__name__)


def calculate_multipart_etag(source_path, chunk_size=8388608):
    # Chuck size is 8 * 1024 * 1024 by default
    md5s = []
    with open(source_path, 'rb', buffering=8192) as fp:
        while True:
            data = fp.read(chunk_size)
            if not data:
                break
            md5s.append(hashlib.md5(data))
    if len(md5s) > 1:
        digests = b"".join(m.digest() for m in md5s)
        new_md5 = hashlib.md5(digests)
        new_etag = '"%s-%s"' % (new_md5.hexdigest(), len(md5s))
    elif len(md5s) == 1:  # file smaller than chunk size
        new_etag = '"%s"' % md5s[0].hexdigest()
    else:  # empty file
        new_etag = '""'
    return new_etag


def check_crc(filename, entry_id):
    url = get_epg_log_url(entry_id)
    with epg_retrieve(url) as r:
        r.raise_for_status()
        content = r.text
    with open(filename, 'rb') as fp:
        crc32 = zlib.crc32(fp.read())
    return hex(crc32)[2:] in content


def check_etag(filename, url):
    with requests.head(url) as r:
        uploaded = r.headers["ETag"]
    print(f"Uploaded etag {uploaded} for {filename}")
    print(f"python calculate_multipart_etag.py '{filename}' 8 {uploaded}")
    local = calculate_multipart_etag(filename)
    return local in uploaded


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
    return datetime.now().isoformat()


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


def get_epg_free():
    url = '{}://{}/api/storage'.format(
        settings.EPG_PROTOCOL,
        settings.EPG_HOST,
    )
    with epg_retrieve(url) as r:
        r.raise_for_status()
        data = r.json()
    return data


def get_epg_log_url(entry_id):
    return '{}://{}/api/recorded/{}/log'.format(
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
        try:
            filename = unquote_plus(entry['filename'])
        except KeyError:
            log.warning(f"Skipping {entry['id']} has no filename: {entry}")
            continue
        if not entry['recording']:
            entry['db_key'] = get_db_key(entry_id)
            entry['filename'] = filename
            entry['epg_file_url'] = get_epg_file_url(entry_id)
            entry['epg_index_url'] = get_epg_index_url(entry_id)
            yield entry
        else:
            log.warn(f'Skipping {filename}')


def get_s3_origin_url(entry):
    return f"{settings.AWS_S3_ENDPOINT_URL}/{settings.AWS_STORAGE_BUCKET_NAME}/{quote(entry['s3_key'])}"


def get_cdn_url(entry):
    return f"{settings.CDN_ENDPOINT_URL}/{quote(entry['s3_key'])}"


def get_db_key(entry_id):
    return f'{settings.KEY_PREFIX}_{entry_id}'


def get_db_entries(sort=False):
    if not sort:
        for entry in kv_store[kv_store.key.startswith(settings.KEY_PREFIX)]:
            yield entry
    else:
        for key in sorted(list(kv_store.keys())):
            if key.startswith(settings.KEY_PREFIX):
                yield kv_store[key]


def check_in_local_key(key):
    return key in kv_store.keys()


def get_db_entry(entry_id):
    return kv_store[get_db_key(entry_id)]

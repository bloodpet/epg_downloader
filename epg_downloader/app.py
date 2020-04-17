from environs import Env
from playhouse.kv import KeyValue
from playhouse.sqlite_ext import SqliteExtDatabase
import os


env = Env()
env.read_env(os.environ.get('PWD'))


class settings:
    EPG_USER = env('EPG_USER')
    EPG_PASSWORD = env('EPG_PASSWORD')
    EPG_HOST = env('EPG_HOST')
    EPG_PROTOCOL = env('EPG_PROTOCOL', default='http')
    AWS_REGION_NAME = env('AWS_REGION_NAME')
    AWS_ACCESS_KEY_ID = env('AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = env('AWS_SECRET_ACCESS_KEY')
    AWS_STORAGE_BUCKET_NAME = env('AWS_STORAGE_BUCKET_NAME')
    AWS_S3_PREFIX = env('AWS_S3_PREFIX', default='')
    DIRECTORY = env('DIRECTORY', default=env('PWD'))
    DATABASE_PATH = env('DATABASE_PATH', default=f'{DIRECTORY}/epg_downloader.db')
    AWS_S3_ENDPOINT_URL = env(
        'AWS_S3_ENDPOINT_URL',
        'https://{}.digitaloceanspaces.com'.format(AWS_REGION_NAME),
    )
    CDN_ENDPOINT_URL = env(
        'CDN_ENDPOINT_URL',
        'https://{}.{}.cdn.digitaloceanspaces.com'.format(AWS_STORAGE_BUCKET_NAME, AWS_REGION_NAME),
    )
    KEY_PREFIX = 'epgd'


database = SqliteExtDatabase(
    settings.DATABASE_PATH,
    pragmas=(
        ('cache_size', -1024 * 4),  # 4MB page-cache.
        ('journal_mode', 'wal'),  # Use WAL-mode (you should always use this!).
        ('foreign_keys', 1)  # Enforce foreign-key constraints.
    )
)

kv_store = KeyValue(database=database)

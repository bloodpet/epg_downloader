from environs import Env
from peewee import Model, SqliteDatabase
import os

env = Env()
env.read_env(os.environ.get('PWD'))


class settings:
    EPG_USER = env('EPG_USER')
    EPG_PASSWORD = env('EPG_PASSWORD')
    EPG_HOST = env('EPG_HOST')
    EPG_PROTOCOL = env('EPG_PROTOCOL', default='http')
    AWS_REGION_NAME = env('AWS_REGION_NAME')
    AWS_S3_ENDPOINT_URL = env(
        'AWS_S3_ENDPOINT_URL',
        'https://{}.digitaloceanspaces.com'.format(AWS_REGION_NAME),
    )
    DIRECTORY = env('DIRECTORY', default=env('PWD'))
    DATABASE_PATH = env('DATABASE_PATH', default=f'{DIRECTORY}/epg_downloader.db')


database = SqliteDatabase(settings.DATABASE_PATH)


class Recordings(Model):
    class Meta:
        database = database

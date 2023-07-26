import os
import logging

import requests
import tarfile

import dask.dataframe as dd

from fondant.component import DaskTransformComponent
from fondant.executor import DaskTransformExecutor

BLACKLIST_URL = (
    "ftp://ftp.ut-capitole.fr/pub/reseau/cache/squidguard_contrib/blacklists.tar.gz"
)


def download_blacklisted_urls(blacklist_url: str) -> list:
    response = requests.get(blacklist_url)
    response.raise_for_status()

    temp_file_path = "blacklists.tar.gz"
    with open(temp_file_path, "wb") as f:
        f.write(response.content)

    with tarfile.open(temp_file_path, "r:gz") as tar:
        tar.extractall()


class FilterWebpageUrls(DaskTransformComponent):
    def __init__(self, *args):
        pass

    def transform(self, dataframe: dd.DataFrame) -> dd.DataFrame:
        return None

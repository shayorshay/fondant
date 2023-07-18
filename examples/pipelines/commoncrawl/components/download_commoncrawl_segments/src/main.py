import os
import time
import logging
from typing import List, Optional

import io
import boto3
from bs4 import BeautifulSoup

import dask.bag as db
import dask.dataframe as dd
import pandas as pd

import requests
from requests import RequestException, ConnectionError
from warcio.archiveiterator import ArchiveIterator

from fondant.component import DaskTransformComponent

logger = logging.getLogger(__name__)

BASE_URL = "https://data.commoncrawl.org/"


def convert_to_plain_text(content: str) -> str:
    """Converts HTML content to plain text.
    Args:
        content: The HTML content to convert.
    Returns:
        The converted plain text.
    """
    try:
        soup = BeautifulSoup(content, "html.parser")
        return soup.get_text()
    except Exception as e:
        logger.error(f"Error parsing HTML: {e}")
        return None


def get_warc_file_with_retry(warc_file: str, retries: int) -> requests.Response:
    retry = 0
    retry_delay = 5

    while retry < retries:
        try:
            response = requests.get(BASE_URL + warc_file, stream=True)
            response.raise_for_status()
            return response
        except (RequestException, ConnectionError) as e:
            logger.error(f"Error downloading WARC file: {e}")
            logger.error(f"Retrying... {retry}/{retries}")
            time.sleep(retry_delay)
            retry += 1
    raise Exception(f"Failed to download WARC file after multiple retries: {warc_file}")


def get_records_from_warc_file(
    warc_file: str,
    get_plain_text: Optional[bool] = False,
    n_records_to_download: Optional[int] = None,
) -> List[List[str]]:
    """Downloads a WARC file and extracts the webpages.
    Args:
        warc_file: The path to the WARC file.
        get_plain_text: Whether to convert the HTML content to plain text.
        n_records_to_download: The number of webpages to download from the WARC file.
    Returns:
        A list of webpages.
    """
    logger.info(f"Processing WARC file from segment path: {warc_file}...")
    records = []
    counter = 0
    response = get_warc_file_with_retry(warc_file)

    for record in ArchiveIterator(response.raw, arc2warc=True):
        if record.rec_type == "response":
            url = record.rec_headers.get_header("WARC-Target-URI")
            content = (
                record.content_stream()
                .read()
                .decode(errors="replace", encoding="utf-8")
            )
            if get_plain_text:
                content = convert_to_plain_text(content)
            records.append([url, content])
            counter += 1

        if n_records_to_download and counter >= n_records_to_download:
            break

    return records


class DownloadCommoncrawlSegments(DaskTransformComponent):
    def transform(
        self,
        df: dd.DataFrame,
        get_plain_text: Optional[bool] = False,
        n_records_to_download: Optional[int] = None,
        partition_size: Optional[int] = None,
    ) -> dd.DataFrame:
        """Downloads Commoncrawl segments based on a list of WARC paths.
        Args:
            df: A Dask DataFrame containing a column of WARC paths.
            get_plain_text: Whether to convert the HTML content to plain text.
            n_records_to_download: The number of webpages to download from each segment.
        Returns:
            A Dask DataFrame containing the downloaded webpages.
        """
        n_partitions = df.npartitions
        n_workers = os.cpu_count()

        if n_partitions < n_workers:
            df = df.repartition(npartitions=n_workers)

        df = (
            df.apply(
                lambda row: get_records_from_warc_file(
                    row["segment_path"], get_plain_text, n_records_to_download
                ),
                axis=1,
                meta=("object"),
            )
            .explode()
            .apply(pd.Series, meta={0: "object", 1: "object"})
        )

        df.columns = [
            "webpage_url",
            "webpage_html",
        ]

        if partition_size:
            df = df.repartition(partition_size=f"{partition_size}MB")

        df = df.reset_index(drop=True)

        return df


if __name__ == "__main__":
    component = DownloadCommoncrawlSegments.from_args()
    component.run()

import os
import time
import logging
from typing import List

import io
import boto3

import dask.bag as db
import dask.dataframe as dd

import pandas as pd

import requests
from requests import RequestException, ConnectionError
from warcio.archiveiterator import ArchiveIterator

from fondant.component import DaskTransformComponent

logger = logging.getLogger(__name__)

BASE_URL = "https://data.commoncrawl.org/"


def get_warc_file_with_retry(warc_file: str) -> str:
    retries = 0
    max_retries = 3
    retry_delay = 5

    while retries < max_retries:
        try:
            response = requests.get(BASE_URL + warc_file, stream=True)
            response.raise_for_status()
            return response
        except (RequestException, ConnectionError, ConnectionResetError) as e:
            logger.error(f"Error downloading WARC file: {e}")
            logger.error(f"Retrying... {retries}/{max_retries}")
            time.sleep(retry_delay)
            retries += 1
    raise Exception(f"Failed to download WARC file after multiple retries: {warc_file}")


def get_records_from_warc_file(warc_file: str, n_records_to_download: int) -> List:
    """Downloads a WARC file and extracts the webpages.
    Args:
        warc_file: The path to the WARC file.
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
            records.append([url, content])
            counter += 1

        if counter == n_records_to_download:
            break

    return records


class DownloadCommoncrawlSegments(DaskTransformComponent):
    def transform(self, df: dd.DataFrame, n_records_to_download: int) -> dd.DataFrame:
        """Downloads Commoncrawl segments based on a list of WARC paths.
        Args:
            df: A Dask DataFrame containing a column of WARC paths.
            n_webpages_to_download: The number of webpages to download from each segment.
        Returns:
            A Dask DataFrame containing the downloaded webpages.
        """
        logger.info(f"len(df): {len(df)}")
        # df = df.repartition(partition_size="250MB") #if dataframe is too small, we end up with 1 partition which this doesn't solve our OOM problem
        df = df.repartition(
            npartitions=2
        )  # if dataframe is too small, some partitions will be empty
        logger.info(df)

        df = df.apply(
            lambda row: get_records_from_warc_file(
                row["segment_path"], n_records_to_download
            ),
            axis=1,
            meta=("object"),
        )
        logger.info(f"len(df): {len(df)}")
        # df = df.repartition(partition_size="250MB") #looking at the task graph, repartitioning merges the partitions

        df = df.explode()
        df = df.apply(pd.Series, meta={0: "object", 1: "object"})
        df.columns = ["webpage_url", "webpage_html"]

        df.visualize(filename="download_commoncrawl_segments.png")

        return df


if __name__ == "__main__":
    component = DownloadCommoncrawlSegments.from_args()
    component.run()

import os
import logging
import typing as t

import dask.dataframe as dd

from fondant.component import DaskLoadComponent

logger = logging.getLogger(__name__)


class LoadFromDirectory(DaskLoadComponent):
    """Component that loads data from a directory."""

    def __init__(self, *_, directory_name: str, project_id: str):
        """
        Args:
            path: path to the directory.
        """
        self.directory_name = directory_name
        os.environ["GOOGLE_CLOUD_PROJECT"] = project_id

    def load(self) -> dd.DataFrame:
        """
        Returns:
            dataset.
        """
        logger.info("Loading dataset...")
        dataframe = dd.read_parquet(self.directory_name)

        logger.info(f"Dataframe length: {len(dataframe)}")
        logger.info(f"Dataframe head: {dataframe.head()}")

        dataframe = dataframe.rename(
            columns={
                "image_url": "image_image_url",
                "alt_text": "image_alt_text",
                "webpage_url": "image_webpage_url",
                "license_type": "image_license_type",
                "license_location": "image_license_location",
            }
        )

        return dataframe

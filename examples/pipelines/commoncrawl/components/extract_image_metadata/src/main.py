import logging
import typing as t

import pandas as pd
import requests
from PIL import Image
from io import BytesIO


from fondant.component import PandasTransformComponent

logger = logging.getLogger(__name__)


def get_image_metadata(url: str, headers):
    """Extract the width, height and format of an image."""
    try:
        # response = requests.get(url, stream=True, headers=headers)
        response = requests.get(url)

        if response.status_code == 200 or response.status_code == 206:
            image = Image.open(BytesIO(response.content))

            width, height, format = image.width, image.height, image.format
            logger.info(
                f"Image width: {width}, height: {height}, format: {format} for url: {url}"
            )
            return width, height, format
    except:
        logger.warning(f"Could not get image data from {url}")
        return None, None, None


class ExtractImageMetadata(PandasTransformComponent):
    """Component that extracts image dimensions based on image url"""

    def transform(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """
        Args:
            dataframe: Dask dataframe
        Returns:
            dataset.
        """
        logger.info("Extracting image resolution...")
        header_range = {"Range": "bytes=0-100000"}

        logger.info(f"Dataframe length: {len(dataframe)}")
        logger.info(f"Dataframe head: {dataframe.head()}")
        logger.info(f"Dataframe columns: {dataframe.columns}")

        dataframe[
            [
                ("image", "image", "width"),
                ("image", "image", "height"),
                ("image", "image", "format"),
            ]
        ] = dataframe[[("image", "image", "url")]].apply(
            lambda x: get_image_metadata(x.image.image.url, header_range),
            axis=1,
            result_type="expand",
        )

        logger.info(f"Dataframe head: {dataframe.head()}")

        return dataframe

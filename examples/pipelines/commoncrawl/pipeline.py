"""Pipeline used to create the dataset to train the StarCoder model."""

import logging
import sys

sys.path.append("../")

from fondant.pipeline import ComponentOp, Pipeline

logger = logging.getLogger(__name__)

# initialize pipeline
commoncrawl_pipeline = Pipeline(
    pipeline_name="commoncrawl_pipeline",
    base_path="/Users/sharongrundmann/Projects/fondant/fondant_artifacts",
    pipeline_description="A pipeline for downloading Common crawl files.",
)

# define ops
load_from_commoncrawl_op = ComponentOp(
    component_dir="components/load_from_commoncrawl",
    arguments={
        "index_name": "CC-MAIN-2023-14",
        "n_segments_to_load": 1,
        # "offset": 3,
    },
    input_partition_rows="disable",
)

download_commoncrawl_segments_op = ComponentOp(
    component_dir="components/download_commoncrawl_segments",
    arguments={
        "n_records_to_download": 100,
    },
    input_partition_rows="disable",
)

# filter_webpage_urls = ComponentOp(
#     component_dir="components/filter_webpage_urls",
#     arguments={
#         "categories": ["adult", "violence"],
#     },
#     input_partition_rows="disable",
# )

extract_image_licenses = ComponentOp(
    component_dir="components/extract_image_licenses",
    input_partition_rows="disable",
)

# add ops to pipeline
commoncrawl_pipeline.add_op(load_from_commoncrawl_op)
commoncrawl_pipeline.add_op(
    download_commoncrawl_segments_op, dependencies=load_from_commoncrawl_op
)
commoncrawl_pipeline.add_op(
    extract_image_licenses, dependencies=download_commoncrawl_segments_op
)

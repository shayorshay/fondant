"""Pipeline used to create the dataset to train the StarCoder model."""

import logging
import sys

sys.path.append("../")

from fondant.pipeline import ComponentOp, Pipeline

logger = logging.getLogger(__name__)

# initialize pipeline
my_pipeline = Pipeline(
    pipeline_name="my_pipeline",
    base_path="/Users/sharongrundmann/Projects/forum-work/fondant/fondant_artifacts",
    pipeline_description="A pipeline for downloading Common crawl files.",
)

# define ops
load_from_commoncrawl_op = ComponentOp(
    component_spec_path="components/load_from_commoncrawl/fondant_component.yaml",
    arguments={
        "index_name": "CC-MAIN-2023-14",
        "n_segments_to_load": 4,
    },
)

download_commoncrawl_segments_op = ComponentOp(
    component_spec_path="components/download_commoncrawl_segments/fondant_component.yaml",
    arguments={
        "n_records_to_download": 100,
        "partition_size": 250,
    },
)

extract_image_licenses = ComponentOp(
    component_spec_path="components/extract_image_licenses/fondant_component.yaml",
)

# add ops to pipeline
my_pipeline.add_op(load_from_commoncrawl_op)
my_pipeline.add_op(
    download_commoncrawl_segments_op, dependencies=load_from_commoncrawl_op
)
my_pipeline.add_op(
    extract_image_licenses, dependencies=download_commoncrawl_segments_op
)

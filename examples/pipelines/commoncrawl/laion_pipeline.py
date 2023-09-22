import logging
import sys

sys.path.append("../")

from aws_pipeline_configs import PipelineConfigs

from fondant.pipeline import ComponentOp, Pipeline

logger = logging.getLogger(__name__)
# General configs
pipeline_name = "commoncrawl-pipeline"
pipeline_description = "Pipeline that downloads from commoncrawl"


load_from_commoncrawl_op = ComponentOp(
    component_dir="components/load_from_commoncrawl",
    arguments={
        "index_name": "CC-MAIN-2023-23",
        "n_segments_to_load": 1,
    },
    cache=False,
)

download_from_commonwarc_op = ComponentOp(
    component_dir="components/download_commoncrawl_segments",
    arguments={
        "n_records_to_download": 100,
        "use_s3": "False",
    },
    cache=False,
)

extract_images_from_warc_op = ComponentOp(
    component_dir="components/extract_image_licenses",
    arguments={
        "deduplicate": "True",
    },
    cache=False,
)


pipeline = Pipeline(pipeline_name=pipeline_name, base_path=PipelineConfigs.BASE_PATH)


pipeline.add_op(load_from_commoncrawl_op)
pipeline.add_op(download_from_commonwarc_op, dependencies=[load_from_commoncrawl_op])
pipeline.add_op(extract_images_from_warc_op, dependencies=[download_from_commonwarc_op])

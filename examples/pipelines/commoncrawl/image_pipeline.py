import os
import logging
import sys

sys.path.append("../")

from pipeline_configs import PipelineConfigs

from fondant.pipeline import ComponentOp, Pipeline

logger = logging.getLogger(__name__)
# General configs
pipeline_name = "image-pipeline"
pipeline_description = "Pipeline that downloads from commoncrawl"

load_from_directory_op = ComponentOp(
    component_dir="components/load_from_directory",
    arguments={
        "directory_name": PipelineConfigs.DIRECTORY_NAME,
        "project_id": PipelineConfigs.PROJECT_ID,
    },
)

extract_image_metadata_op = ComponentOp(
    component_dir="components/extract_image_metadata",
)


pipeline = Pipeline(pipeline_name=pipeline_name, base_path=PipelineConfigs.BASE_PATH)

pipeline.add_op(load_from_directory_op)
pipeline.add_op(extract_image_metadata_op, dependencies=[load_from_directory_op])

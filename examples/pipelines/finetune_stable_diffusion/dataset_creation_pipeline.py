"""Pipeline used to create a stable diffusion dataset from a set of given images."""
# pylint: disable=import-error
import json

from kfp import components as comp
from kfp import dsl

from kubernetes import client as k8s_client

from config.general_config import KubeflowConfig
from config.components_config import DatasetLoaderConfig, ImageFilterConfig, EmbeddingConfig
from express.kfp_utils import compile_and_upload_pipeline

# Load Components
run_id = '{{workflow.name}}'
artifact_bucket = KubeflowConfig.ARTIFACT_BUCKET

# Component 1
dataset_loader_op = comp.load_component('components/dataset_loader_component/component.yaml')
dataset_loader_extra_args = {"dataset_name": DatasetLoaderConfig.DATASET_NAME}
dataset_loader_metadata_args = {"run_id": run_id, "component_name": dataset_loader_op.__name__, "artifact_bucket": artifact_bucket}
dataset_loader_extra_args = json.dumps(dataset_loader_extra_args)
dataset_loader_metadata_args = json.dumps(dataset_loader_metadata_args)

# Component 2
image_filter_op = comp.load_component('components/image_filter_component/component.yaml')
image_filter_extra_args = {"min_height": ImageFilterConfig.MIN_HEIGHT, "min_width": ImageFilterConfig.MIN_WIDTH}
image_filter_metadata_args = {"run_id": run_id, "component_name": image_filter_op.__name__, "artifact_bucket": artifact_bucket}
image_filter_extra_args = json.dumps(image_filter_extra_args)
image_filter_metadata_args = json.dumps(image_filter_metadata_args)


# Component 3
embedding_op = comp.load_component('components/embedding_component/component.yaml')
embedding_extra_args = {"model_id": EmbeddingConfig.MODEL_ID, "batch_size": EmbeddingConfig.BATCH_SIZE}
embedding_metadata_args = {"run_id": run_id, "component_name": embedding_op.__name__, "artifact_bucket": artifact_bucket}
embedding_extra_args = json.dumps(embedding_extra_args)
embedding_metadata_args = json.dumps(embedding_metadata_args)


# Pipeline
@dsl.pipeline(
    name='image-generator-dataset',
    description='Pipeline that takes example images as input and returns an expanded dataset of '
                'similar images as outputs'
)
# pylint: disable=too-many-arguments, too-many-locals
def sd_dataset_creator_pipeline(dataset_loader_extra_args: str = dataset_loader_extra_args,
                                dataset_loader_metadata_args: str = dataset_loader_metadata_args,
                                image_filter_extra_args: str = image_filter_extra_args,
                                image_filter_metadata_args: str = image_filter_metadata_args,
                                embedding_extra_args: str = embedding_extra_args,
                                embedding_metadata_args: str = embedding_metadata_args,
):
    # Component 1
    dataset_loader_task = dataset_loader_op(extra_args=dataset_loader_extra_args,
                                            metadata_args=dataset_loader_metadata_args,
    ).set_display_name('Load initial images')

    # Component 2
    image_filter_task = image_filter_op(extra_args=image_filter_extra_args,
                                        metadata=image_filter_metadata_args,
                                        input_manifest=dataset_loader_task.outputs["output_manifest"],
    ).set_display_name('Filter images')

    # Component 3
    embedding_task = embedding_op(extra_args=embedding_extra_args,
                                        metadata=embedding_metadata_args,
                                        input_manifest=image_filter_task.outputs["output_manifest"],
    ).set_display_name('Embed images') \
    .set_gpu_limit(1) \
    .add_node_selector_constraint('node_pool', 'model-inference-pool') \
    .add_toleration(
        k8s_client.V1Toleration(effect='NoSchedule', key='reserved-pool', operator='Equal',
                                value='true'))


if __name__ == '__main__':
    compile_and_upload_pipeline(pipeline=sd_dataset_creator_pipeline,
                                host=KubeflowConfig.HOST,
                                env=KubeflowConfig.ENV)

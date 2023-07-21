from dataclasses import dataclass


@dataclass
class PipelineConfigs:
    HOST = "http://localhost:8080"
    BASE_PATH = "s3://fondant-artifacts"

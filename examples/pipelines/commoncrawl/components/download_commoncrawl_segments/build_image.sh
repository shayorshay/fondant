#!/bin/bash -e
# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Set some variables
ARTIFACT_PATH="europe-west4-docker.pkg.dev/soy-audio-379412/soy-audio-379412-default-repository/kubeflow-components/components"
IMAGE_NAME="download_commoncrawl_segments"
IMAGE_TAG="latest"

# Create full name of the image
FULL_IMAGE_NAME=${ARTIFACT_PATH}/${IMAGE_NAME}:${IMAGE_TAG}
echo $FULL_IMAGE_NAME

# Build the image
docker build --push -t $FULL_IMAGE_NAME --build-arg="FONDANT_VERSION=main" --no-cache .

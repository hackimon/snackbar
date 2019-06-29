#!/usr/bin/env python

# Modified by Atsuyoshi Suzuki.
# This script is a modified version of Google's sample script.
# The license is the same as the original.
#
# Copyright 2018 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


"""OCR with PDF/TIFF as source files on GCS
Example:
    python ocr_and_wait.py \
    --gcs-source-uri gs://python-docs-samples-tests/HodgeConj.pdf \
    --gcs-destination-uri gs://BUCKET_NAME/PREFIX/
"""

import argparse
import re

from google.cloud import storage
from google.cloud import vision_v1p2beta1 as vision
from google.protobuf import json_format


from time import sleep
import sys


# [START vision_async_detect_document_ocr]
def async_detect_document(gcs_source_uri, gcs_destination_uri):
    # Supported mime_types are: 'application/pdf' and 'image/tiff'
    mime_type = 'application/pdf'

    # How many pages should be grouped into each json output file.
    # With a file of 5 pages
    #batch_size = 2
    batch_size = 5

    client = vision.ImageAnnotatorClient()

    feature = vision.types.Feature(
        type=vision.enums.Feature.Type.DOCUMENT_TEXT_DETECTION)

    gcs_source = vision.types.GcsSource(uri=gcs_source_uri)
    input_config = vision.types.InputConfig(
        gcs_source=gcs_source, mime_type=mime_type)

    gcs_destination = vision.types.GcsDestination(uri=gcs_destination_uri)
    output_config = vision.types.OutputConfig(
        gcs_destination=gcs_destination, batch_size=batch_size)

    async_request = vision.types.AsyncAnnotateFileRequest(
        features=[feature], input_config=input_config,
        output_config=output_config)

    operation = client.async_batch_annotate_files(
        requests=[async_request])

    print("Operation started: {}".format(operation.operation))

    return operation

# [END vision_async_detect_document_ocr]

# Not used
def is_complete(operation,timeout=90):

    try:
        # 非同期で完了待ちするメソッド

        req.result(timeout=timeout)

        return True

    except:
        print('Operation not finished.')

        return False



if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--gcs-source-uri', required=True)
    parser.add_argument('--gcs-destination-uri', required=True)

    args = parser.parse_args()
    req= async_detect_document(args.gcs_source_uri, args.gcs_destination_uri)

    # print(req)

    wait_time = 15
    sleep(wait_time)

    while (True):
        if req.done():
            break

        print("Operation not completed. Stil waiting...")
        sleep(wait_time)
        wait_time += 10



    # Once the request has completed and the output has been
    # written to GCS, we can list all the output files.

    storage_client = storage.Client()
    blob_list = []

    match = re.match(r'gs://([^/]+)/(.+)', args.gcs_destination_uri)


    if match :

        bucket_name = match.group(1)
        prefix = match.group(2)

        bucket = storage_client.get_bucket(bucket_name=bucket_name)

        # List objects with the given prefix.
        blob_list = list(bucket.list_blobs(prefix=prefix))


    else:
        print("Pattern not matched!")
        sys.exit()


    print('Output files:')


    for blob in blob_list:
        print(blob.name)

    # Process the first output file from GCS.
    # Since we specified batch_size=2, the first response contains
    # the first two pages of the input file.
    output = blob_list[0]

    json_string = output.download_as_string()

    response = json_format.Parse(
        json_string, vision.types.AnnotateFileResponse())

    # The actual response for the first page of the input file.
    first_page_response = response.responses[0]
    annotation = first_page_response.full_text_annotation

    # Here we print the full text from the first page.
    # The response contains more information:
    # annotation/pages/blocks/paragraphs/words/symbols
    # including confidence scores and bounding boxes
    print(u'Full text:\n{}'.format(
        annotation.text))
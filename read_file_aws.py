#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr  1 14:30:31 2024

@author: phoebecai
"""
import pandas as pd
import numpy as np
import os
import boto3
from botocore.exceptions import ClientError

################################################
# Function: for a given bucket, object key, and AWS keys
# reads a CSV and returns it as a df (unless there is an error)
################################################
def read_file(bucket_name, object_key, aws_id, aws_secret):
    try:
        s3 = boto3.client('s3', aws_access_key_id=aws_id, aws_secret_access_key=aws_secret)
        obj = s3.get_object(Bucket=bucket_name, Key=object_key)
        df = pd.read_csv(obj['Body'], index_col = False)
        return df
    except ClientError as ex:
        if ex.response['Error']['Code'] == 'NoSuchKey':
            print("Key doesn't match. Please check the key value entered.")
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

# user function;
def read_file(bucket_name, key_value, aws_id, aws_secret):
    try:
        s3 = boto3.client('s3', aws_access_key_id=aws_id, aws_secret_access_key=aws_secret)
        obj = s3.get_object(Bucket=bucket_name, Key=key_value)
        df = pd.read_csv(obj['Body'], index_col = False)
        return df
    except ClientError as ex:
        if ex.response['Error']['Code'] == 'NoSuchKey':
            print("Key doesn't match. Please check the key value entered.")# Write the data
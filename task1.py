#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr  1 13:51:28 2024

@author: phoebecai
"""
import pandas as pd
import numpy as np
import os
import boto3
import sqlite3
from botocore.exceptions import ClientError
from read_file_aws import read_file
from dateutil.relativedelta import relativedelta


################################################
# STEP 0: Reading and initial cleaning of data
################################################
####################
# Reading the csv from AWS
####################
# I've stored the keys (from the instructions) as environment variables to avoid publicizing them
# Fetching the keys
waymark_task_key_id = os.environ.get('waymark_task_key_id')
waymark_task_secret_key = os.environ.get('waymark_task_secret_key')
bucket_name = 'waymark-assignment'

# I have another file called read_file_aws.py - it has a function read_file,
# which reads files from s3 buckets.
# I've imported that functon, and here I read the patient_id_month_year csv
patient_id_month_year = read_file(bucket_name, 'patient_id_month_year.csv', 
                                  waymark_task_key_id, waymark_task_secret_key)

####################
# Some (light) data cleaning
####################
# Keeping only the columns that matter, converting the date to datetime
patient_id_month_year = patient_id_month_year[['patient_id','month_year']]
patient_id_month_year['month_year']= pd.to_datetime(patient_id_month_year['month_year'], format='%m/%d/%y')

# Dropping NAs
print(patient_id_month_year.isna().sum())
patient_id_month_year = patient_id_month_year.dropna()

# Making sure there are no erroneous dates that are out of range
print(min(patient_id_month_year['month_year']))
print(max(patient_id_month_year['month_year']))


################################################
# STEP 1: Data Transformation
################################################
# It was more straightforward for me to do this in Python, but it 
# could also be done in SQL (e.g. partitioning by patient_id ordered by month,
# and checking if a date is exactly one month after the lagged value above it).

####################
# Making a new column ('cont_period') to label each chunk of continuous dates
####################
# Sort df by ID and date
patient_id_month_year.sort_values(by=['patient_id', 'month_year'])

# The column 'cont start' is Boolean and indicates whether a date is the start of a run
# of continuous months for the patient. This is the case if:
# 1) It's the first observation for this patient, i.e. the lagged (shift(1)) patient_id
#    is different from the current row's patent_id.
# OR 
# 2) It's the first observation after a gap in coverage for a patient, i.e. 
#    the lagged month_year is not last month -- there is a difference of more than 1 month.
patient_id_month_year['cont_start'] = np.logical_or(patient_id_month_year['patient_id'] != patient_id_month_year['patient_id'].shift(1),
                                                    ((patient_id_month_year['month_year'].dt.to_period('M').astype(int) - 
                                                      patient_id_month_year['month_year'].shift(1).dt.to_period('M').astype(int)) != 1))

# Taking the cumulative sum: this splits the periods of coverage into continuous periods of coverage
patient_id_month_year['cont_period'] = patient_id_month_year['cont_start'].cumsum()

####################
# Creating the enrollment spans df
####################
# Group by continuous periods, and get the min (start) and max (first day of final month)
# date in each period, creating a new df called patient_enrollment_span
gb = patient_id_month_year.groupby(['patient_id', 'cont_period'])
patient_enrollment_span = gb.agg(enrollment_start_date=('month_year', 'min'),
                                 enrollment_end_date=('month_year', 'max')).reset_index()

# Change the end date to be the last day of the final month, rather than the first day
patient_enrollment_span['enrollment_end_date'] = np.array([d + relativedelta(months=1, days=-1) for d in patient_enrollment_span['enrollment_end_date']])

# Remove any unnecessary columns and save to csv
patient_enrollment_span = patient_enrollment_span[['patient_id', 'enrollment_start_date', 'enrollment_end_date']]
patient_enrollment_span.to_csv('csv/patient_enrollment_span.csv', index=False) 

# Print the number of rows (Answer 1)
print(len(patient_enrollment_span.index))

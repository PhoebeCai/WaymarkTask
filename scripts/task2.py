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
# Reading the csvs
####################
# Reading the csv I created
patient_enrollment_span = pd.read_csv('../csv/patient_enrollment_span.csv',
    dtype={'patient_id': 'str',
    'enrollment_start_date': 'str',
    'enrollment_end_date': 'str'})

# Fetching AWS keys (I stored as environment variables)
waymark_task_key_id = os.environ.get('waymark_task_key_id')
waymark_task_secret_key = os.environ.get('waymark_task_secret_key')
bucket_name = 'waymark-assignment'

# I imported a function read_file, which reads csvs from s3 buckets.
# Reading outpatient_vsit_files csv
outpatient_visits_file = read_file(bucket_name, 'outpatient_visits_file.csv', 
                                   waymark_task_key_id, waymark_task_secret_key)

####################
# Some (light) data cleaning
####################
# Converting to date
patient_enrollment_span['enrollment_start_date']= pd.to_datetime(patient_enrollment_span['enrollment_start_date'], format='%Y-%m-%d')
patient_enrollment_span['enrollment_end_date']= pd.to_datetime(patient_enrollment_span['enrollment_end_date'], format='%Y-%m-%d')

# Keeping only the columns that matter, converting the date to datetime
outpatient_visits_file = outpatient_visits_file[['patient_id','date','outpatient_visit_count']]
outpatient_visits_file['date']= pd.to_datetime(outpatient_visits_file['date'], format='%m/%d/%y')

# Dropping NAs
print(outpatient_visits_file.isna().sum())
outpatient_visits_file = outpatient_visits_file.dropna()


################################################
# STEP 2: Data Aggregation
################################################
####################
# SQL setup
####################
# This part requires a complex join that is easier with SQL, so I use sqlite
conn = sqlite3.connect(':memory:')

# Before creating the tables I make sure they don't already exist,
# to avoid errors / using outdated data that is previously saved
conn.executescript('drop table if exists patient_enrollment_span;')
conn.executescript('drop table if exists outpatient_visits_file;')

# Writing the patient enrollment and outpatient visit dfs to sql
patient_enrollment_span.to_sql('patient_enrollment_span', conn, index=False)
outpatient_visits_file.to_sql('outpatient_visits_file', conn, index=False)


####################
# SQL Query (with explanation)
# This generates the result df
####################
# Group by: patient id, start date, and end date -- this identifies each enrollment
#           period for a patient

# Left join: we want to keep all the enrollment periods. It is ok to drop a visit
# if for some reason it falls outside an enrollment period, since we only want to
# calculate the values within each enrollment period. So I left join, where
# the "left" dataset is the enrollment periods. I join if the patient ID matches
# and the visit falls between the enrollment start and end dates (inclusive)

# Total outpatient visits: sum of the number of visits (or 0 if null, i.e. there
#                              were no visits recorded and hence there was a
#                              null from the left join)
# Distinct days with visit: count distinct dates within each enrollment period
get_result_qry = '''
    select  
        pes.patient_id
        , enrollment_start_date
        , enrollment_end_date
        , sum(ifnull(outpatient_visit_count,0)) as ct_outpatient_visits
        , count(distinct date) as ct_days_with_outpatient_visit
    from
        patient_enrollment_span pes
    left join outpatient_visits_file ovf on pes.patient_id = ovf.patient_id and
        (ovf.date between pes.enrollment_start_date and pes.enrollment_end_date)
    group by pes.patient_id, enrollment_start_date, enrollment_end_date
    '''
result = pd.read_sql_query(get_result_qry, conn)

# Save to csv
result.to_csv('../csv/result.csv', index=False) 

# Print the number distinct values (Answer 2)
print(result['ct_days_with_outpatient_visit'].nunique())


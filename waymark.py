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

waymark_task_key_id = os.environ.get('waymark_task_key_id')
waymark_task_secret_key = os.environ.get('waymark_task_secret_key')

# Set the values
bucket_name = 'waymark-assignment'


patient_id_month_year = read_file(bucket_name, 'patient_id_month_year.csv', 
                                  waymark_task_key_id, waymark_task_secret_key)
patient_id_month_year = patient_id_month_year[['patient_id','month_year']]
patient_id_month_year['month_year']= pd.to_datetime(patient_id_month_year['month_year'], format='%m/%d/%y')

outpatient_visits_file = read_file(bucket_name, 'outpatient_visits_file.csv', 
                                   waymark_task_key_id, waymark_task_secret_key)
outpatient_visits_file = outpatient_visits_file[['patient_id','date','outpatient_visit_count']]
outpatient_visits_file['date']= pd.to_datetime(outpatient_visits_file['date'])


print(patient_id_month_year.isna().sum())
patient_id_month_year = patient_id_month_year.dropna()

print(outpatient_visits_file.isna().sum())
outpatient_visits_file = outpatient_visits_file.dropna()


patient_id_month_year.sort_values(by=['patient_id', 'month_year'])
patient_id_month_year['cont_cov'] = np.logical_or(((patient_id_month_year['month_year'].dt.to_period('M').astype(int) - 
                                     patient_id_month_year['month_year'].shift(1).dt.to_period('M').astype(int)) != 1),
                                    patient_id_month_year['patient_id'] != patient_id_month_year['patient_id'].shift(1))

patient_id_month_year['cont_cov'] = patient_id_month_year['cont_cov'].cumsum()

gb = patient_id_month_year.groupby(['patient_id', 'cont_cov'])
patient_enrollment_span = gb.agg(enrollment_start_date=('month_year', 'min'),
                                 enrollment_end_date=('month_year', 'max')).reset_index()

patient_enrollment_span['enrollment_end_date'] = np.array([d + relativedelta(months=1, days=-1) for d in patient_enrollment_span['enrollment_end_date']])

patient_enrollment_span = patient_enrollment_span[['patient_id', 'enrollment_start_date', 'enrollment_end_date']]
patient_enrollment_span.to_csv('csv/patient_enrollment_span.csv', index=False) 
print(len(patient_enrollment_span.index))



#Make the db in memory
conn = sqlite3.connect(':memory:')
#write the tables
conn.executescript('drop table if exists patient_enrollment_span;')
conn.executescript('drop table if exists outpatient_visits_file;')


patient_enrollment_span.to_sql('patient_enrollment_span', conn, index=False)
outpatient_visits_file.to_sql('outpatient_visits_file', conn, index=False)

get_result_qry = '''
    with merged as (
        select  
            *
        from
            patient_enrollment_span pes
        left join outpatient_visits_file ovf on pes.patient_id = ovf.patient_id and
            (ovf.date between pes.enrollment_start_date and pes.enrollment_end_date)
    )
    select 
        patient_id
        , enrollment_start_date
        , enrollment_end_date
        , sum(ifnull(outpatient_visit_count,0)) as ct_outpatient_visits
        , count(distinct date) as ct_days_with_outpatient_visit
    from merged
    group by patient_id, enrollment_start_date, enrollment_end_date
    '''
result = pd.read_sql_query(get_result_qry, conn)

result.to_csv('csv/result.csv', index=False) 

print(result['ct_days_with_outpatient_visit'].nunique())


This project is the assessment for the Healthcare and BI Analyst role. 

Files:
1) read_file_aws.py: Contains the function read_file, which files from S3 buckets. This is referenced in the main script waymark.py. Note that I have saved the AWS keys as environment variables -- the data will not be accessible to someone without the keys.

2) task1.py
	- Inputs/dependencies:
		- Uses patient_id_month_year.csv from the Waymark S3 bucket
	- Outputs:
		- Generates csv/patient_enrollment_span.csv locally

3) task2.py
	- Inputs/dependencies:
		- Uses csv/patent_enrollment_span.csv (need to ensure this file is up-to-date, or re-run task1.py before running task2.py)
		- Uses outpatient_vists_file.csv from the Waymark S3 bucket
	- Outputs:
		- Generates csv/result.csv locally



Design choices:
- I choose not to save patient_id_month_year and outpatient_visits_file locally as csvs and instead work directly with them in pandas
	-...but under certain circumstances (e.g. it is very slow to fetch something and it is updated infrequently), it could make sense to save a copy locally and work with that.
- If there were more complex dependencies or redundancies, I would think more about how to break the tasks into separate scripts and write functions to automate any repetitive parts.
	- Right now, there are two scripts, and the second one uses the csv that I created in the first one.
	- This is slightly slower than just writing one long script where I continue to work with the df from step 1, but I break the tasks up for readability.
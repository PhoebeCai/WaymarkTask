This project is the assessment for the Healthcare and BI Analyst role. 

<b>Scripts (in scripts folder):</b>
1) <b>read_file_aws.py</b>: Contains the function read_file, which reads csv files from S3 buckets
	- This is referenced in the <b>task1.py</b> and <b>task2.py</b> scripts.
	- Note that I have saved the AWS keys as environment variables -- the data will not be accessible to someone without the keys.

2) <b>task1.py</b>
	- <i>Inputs/dependencies:</i>
		- Uses <b>patient_id_month_year.csv</b> from the Waymark S3 bucket
		- Uses read_file function from <b>read_file_aws.py</b>
	-  <i>Outputs:</i>
		- Generates <b>csv/patient_enrollment_span.csv</b> locally

3) <b>task2.py</b>
	- <i>Inputs/dependencies:</i>
		- Uses local file <b>csv/patent_enrollment_span.csv</b> (need to ensure this file is up-to-date, or re-run <b>task1.py</b> before running <b>task2.py</b>)
		- Uses <b>outpatient_vists_file.csv</b> from the Waymark S3 bucket
		- Uses read_file function from <b>read_file_aws.py</b>
	- <i>Outputs:</i>
		- Generates <b>result.csv</b> locally

<b>CSV files (in csv folder):</b>

These are the two files we are asked to produce in the assignment. (I won't describe them here, since they are already documented in the assignment sheet)
1) <b>patient_enrollment_span.csv</b>
2) <b>result.csv</b>

<b>Design choices:</b>
- I choose not to save <b>patient_id_month_year.csv</b> and <b>outpatient_visits_file.csv</b> locally and instead work directly with them in pandas
	- ...but under certain circumstances (e.g. it is very slow to fetch something and it is updated infrequently), it could make sense to save a copy locally and work with that.
- If there were more complex dependencies or redundancies, I would think more about how to break the tasks into separate scripts and write functions to automate any repetitive parts.
	- Right now, there are two scripts, and the second one uses the csv that I created in the first one.
	- This is slightly slower than just writing one long script where I continue to work with the df from step 1, but I break the tasks up for readability.
# Apache Beam example

This data pipeline reads immigration data and performs transformations to join with other datasets using Apache Beam

## For Local run

1. On your working directory (Windows) run on terminal:
`python3 pipeline.py --input_dir <PATH>\<TO>\<DATA>\ --output_dir <PATH>\<TO>\<OUTPUT>\`

## For Dataflow run 

> This asumes to have installed and configured `gsutil` on local computer. Also that file paths are written with syntax as Linux (`/`) instead of syntax as Windows (`\`)

1. Create Google storage bucket

2. Upload data from directory `data/` to Google storage bucket

`gsutil cp -r data gs://<YOUR GCP BUCKET>/`

3. On Google cloud shell terminal install packages

`sudo apt-get install python3-pip`

`sudo install -U pip`
`sudo pip3 install apache-beam[gcp] oauth2client==3.0.0 pandas`

4. On Google cloud shell editor upload run file: `pipeline.py`

5. Run job on cloud shell terminal

`python3 pipeline.py --input_dir gs://<YOUR GCP BUCKET>/<PATH>/<TO>/<DATA>/ --output_dir gs://<YOUR GCP BUCKET>/<PATH>/<TO>/<DATA>/ --project <YOUR GCP PRJECT ID>
--job_name <SET JOB NAME> --temp_location gs://<YOUR GCP BUCKET>/staging/ --staging_location gs://<YOUR GCP BUCKET>/staging/
--region us-central1 --runner DataflowRunner`
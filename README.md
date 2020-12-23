# etl-tool

Python application for database management and patient data ETL.  
Requires Python 3.8+.

### How to run :

1. Create new venv for this project and activate it, make sure you have Python 3.8 or newer  
   `python -m venv .venv`  
   `source .venv/bin/activate`  

2. Install either requirements  
   Basic requirements:  
   `pip install -r requirements.txt`  

   For development purpose (linting, testing):  
   `pip install -r requirements-dev.txt`  

3. Install app as python module  
   `pip install -e .`

4. App requires PostgreSQL database, you can run it as docker container  
   `docker-compose up -d`  

5. Apply database schema  
   `invoke db.schema`

6. Run app  
   `etl-tool [-c] [-v] [-e STRING]`  
   `-c` clears database before running app  
   `-e` runs app for single data typ, possible values: {patients, encounters, procedures, observations}  
   `-v` runs app in verbose mode  


### Extras:
Run app only for "patients" data with verbose mode:  
`etl-tool -v -e patients`

Recreate database schema (all stored data will be lost):  
`invoke db.drop db.schema`  

If development requirements are installed you can run tests or mypy and flake8 linting  

Linting (flake8 + mypy):  
`invoke lint`  

Run all tests:  
`invoke test`

Sample logs:
```
(.venv)  ~/work/patient-data-management(master) etl-tool -v -e patients -c
[2020-12-23 15:39:14,200 - app - INFO] Resolving Patients
[2020-12-23 15:39:14,220 - app - DEBUG] Worker queue-0 START
[2020-12-23 15:39:14,220 - app - DEBUG] Worker queue-1 START
[2020-12-23 15:39:14,797 - app - DEBUG] EOF reached
[2020-12-23 15:39:15,213 - app.tables.basic_batcher - DEBUG] 1628 records in this batch, total: 1628
[2020-12-23 15:39:15,214 - app - INFO] Patients resolving time: 1.0129 s
- Final Report -
Batchers statistics:
	Patients item processed:           1628
	Patients records inserted:         1628
	Encounters item processed:            0
	Encounters records inserted:          0
	Procedures item processed:            0
	Procedures records inserted:          0
	Observations item processed:          0
	Observations records inserted:        0
Additional statistics:
	Patients by gender:
	              female      828
	                male      800
	10 most popular procedures:
	Most popular start encounter days of week:
	Most popular end encounter days of week:
[2020-12-23 15:39:15,264 - app - INFO] TOTAL TIME: 1.0816 s
```


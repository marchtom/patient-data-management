# etl-tool-management

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


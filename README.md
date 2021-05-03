# Ballot Curing Project - Backend
Ballot Curing team project for EN.601.310 Software for Resilient Communities 

See our website [here](http://www.cnds.jhu.edu/courses/cs310/ballot-curing)

## Repository Architecture
Flask API endpoints are in `api/`.

State ingest programs, daily querying programs, and ballot statistics programs are in `db/`.

## Configuration
The `BALLOT_CURING_PATH` and `PYTHONPATH` environment variables must be set.

```
cd ballot-curing-backend/
export BALLOT_CURING_PATH=$PWD
export PYTHONPATH=$PYTHONPATH:$PWD:$PWD/db/shared
export FLASK_APP=$PWD/api/src/app.py
```

Set up variables for state configurations in `config.ini`. A sample configuration is provided.

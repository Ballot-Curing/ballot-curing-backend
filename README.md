# Ballot Curing Project - Backend
Ballot Curing team project for EN.601.310 Software for Resilient Communities 

See our website [here](http://www.cnds.jhu.edu/courses/cs310/ballot-curing)

## Repository Architecture
Flask API endpoints are in `api/`.
Please see API README for API configuration.

State ingest programs, daily querying programs, and ballot statistics programs are in `db/`.

## Configuration
Quick-start: `source exports.sh`

If you would like to manually set things up:
The `BALLOT_CURING_PATH` and `PYTHONPATH` environment variables must be set.

```
cd Ballot-Curing-Project/
export BALLOT_CURING_PATH=$PWD
export PYTHONPATH=$PYTHONPATH:$PWD:$PWD/db/shared
```

Set up variables for state configurations in `config.ini`. A sample configuration is provided.

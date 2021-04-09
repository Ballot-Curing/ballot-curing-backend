# Ballot Curing Project - Backend
Vote-by-Mail team project for EN.601.310 Software for Resilient Communities 

See our website [here](http://www.cnds.jhu.edu/courses/cs310/vote-by-mail/)

## Repository Architecture
Flask API endpoints are in `api/`.

State ingest programs, daily querying programs, and ballot statistics programs are in `db/`.

## Configuration
Make sure top-level directory and `db/shared` are in your PYTHONPATH e.g.

```
export PYTHONPATH=$PYTHONPATH:/path/to/Ballot-Curing-Project:/path/to/Ballot-Curing-Project/db/shared
```

Set up variables for state configurations in `config.ini`. A sample configuration is provided.

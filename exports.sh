#!/bin/bash

export BALLOT_CURING_PATH=$PWD
export PYTHONPATH=$PYTHONPATH:$PWD:$PWD/db/shared
export FLASK_APP=$PWD/api/src/app.py

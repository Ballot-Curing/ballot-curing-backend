### API Docs

#### 1. Setup
A Conda environment was used to manage packages for our project. The requirements are stored in `requirements.txt`. An example of creating an environment is:

```
conda create --name proj3 --file requirements.txt
```

then 

```
conda activate proj3
```

#### 2. Configuration
A `config.ini` should be set up following the structure in `config-sample.ini`.
`FLASK_APP` should be set as well.

#### 3. Running
Running development server:
```
cd src
flask run --host=0.0.0.0 --port=5500
```

Running server for production:
```
TODO
```

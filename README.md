# DLDataGen

This project provides scripts two scripts to generate data for experiments need, especially in a context of data lakes.

The first script, "DocumentDataGen.py" allows to download (long) textual documents from the HAL repository. 

The second script, "TabularDataGen.py" allows to extract tabular files from a base SQLite database.

## 1- "DocumentDataGen.py"

### Dependencies
The script needs the following python packages to run effectively: tqdm, pandas, argopt, joblib, unidecode and requests

### Usage
To generate documents, type 'python DocumentDataGen.py -l <limit> -j <n_jobs>', with limit the number of thousands of documents to generate, and n_jobs the number
of processor cores to use. 
  
For example, to generate 10000 documents using 5 cores, you have to enter 'python -l 10 -j 5'. 

The script automatically creates a 'data' folder where all generated data are stored. It also provides a CSV catalogue of basic metadata (title, language, path, institution)  on all generated files. 

## 2- "TabularDataGen.py"

### Dependencies
The script needs the following python packages to run effectively: tqdm, pandas, argopt and sqlalchemy

In addition to these python packages, you have to put in the same folder the SQLite database from which tabular files are extrected. Such database must be downloaded from https://storage.googleapis.com/table-union-benchmark/large/benchmark.sqlite

### Usage
To generate tabular files, type 'python TabularDataGen.py -l <limit>', with limit the number of thousands of tabular files to generate.
  
For example, to generate 2000 tabular files, you have to enter 'python -l 2'. 

The script automatically creates a 'data' folder where all generated data are stored.  
